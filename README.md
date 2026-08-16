# EventFS v2

![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)

EventFS v2 是基于 Rust、[openraft](https://github.com/datafuselabs/openraft) 和
[surrealkv](https://github.com/surrealdb/surrealkv) 的分布式事件存储。每个 Shard
拥有独立的 Raft group 和 LSM tree；Stream 归属由控制 Shard 强一致提交，并支持
运行期增加节点与 Shard、在线迁移、临时订阅、持久化拉取订阅，以及面向聚合类型
事件集的 Linux FUSE 文件接口。

## 能力

| 能力 | 当前语义 |
|---|---|
| Stream 写入 | 乐观并发、按 `event_id` 幂等、单事务提交 |
| Stream 读取 | 单流版本顺序；跨 Shard `ReadAll` 按 HLC 近似归并，不提供严格全序 |
| 订阅 | 多 Stream 或 `$all` 临时订阅；命名持久化消费者组支持 Ack/Retry/Park/Skip |
| 分片 | `[placement]` 显式放置；默认 `replication_factor = 1`；每 Shard 独立 Raft 与存储 |
| 归属与迁移 | 控制 Shard 保存权威归属，generation fencing 阻止旧路由继续写源 |
| AggregateStore | 固定 256 个虚拟事件分区、实例级 OCC、状态 revision CAS、显式结算消费者组 |
| Linux FUSE | 将 AggregateStore 映射为事件 JSONL、状态 JSON 和消费者组文件 |
| 安全传输 | 公共与内部 gRPC 支持 TLS；配置 CA 时严格校验，否则 HTTPS 默认跳过证书校验 |

架构不变量、协议和数据布局见 [设计文档](docs/design.md)，领域词汇见
[CONTEXT.md](CONTEXT.md)。

## 组件

| 二进制 / crate | 用途 |
|---|---|
| `eventstored` / `es-server` | 事件存储、Raft 管理、迁移和 AggregateStore gRPC 服务 |
| `es-client` | Rust 客户端，封装 Stream、持久化订阅和 AggregateStore API |
| `esctl` | 数据读写、订阅、集群、迁移、快照和 AggregateStore 管理 CLI |
| `eventfs-fuse` | Linux FUSE3 适配器 |
| `es-core` / `es-storage` / `es-raft` / `es-proto` | 领域模型、持久化、共识与协议实现 |

公共 `listen_addr` 注册六个 gRPC service：`EventStore`、
`PersistentSubscriptions`、`AggregateStore`、`RaftRpc`、`RaftAdmin` 和
`Migration`。多节点内部调用使用独立的 `internal_listen_addr`；该端口必须只允许
集群节点访问。完整拓扑见 [设计文档](docs/design.md#3-整体架构)。

## 快速开始

### 前置条件

- Rust 1.88 或更高版本
- Protocol Buffers 编译器 `protoc`

### 单节点

构建 debug 二进制：

```bash
cargo build --workspace --bins --locked
```

创建最小 `config.toml`：

```toml
[node]
id = 1
listen_addr = "127.0.0.1:50051"

[storage]
data_dir = "./data/node1"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]
replica = []
```

启动服务：

```bash
./target/debug/eventstored --config ./config.toml
```

在另一个终端初始化唯一的 Raft group，并完成写读闭环：

```bash
./target/debug/esctl init --shard 0 \
  --member 1@127.0.0.1:50051 --yes

./target/debug/esctl append orders/1 \
  --event-type OrderPlaced \
  --data '{"quantity":1}' \
  --expected-version nostream

./target/debug/esctl read orders/1 --from-version 0
```

`node.peers` 为空时需要手动执行 `esctl init`；多节点配置了完整 peers 后会自动
组建每个 Shard。配置字段、自动/手动组建和数据目录约束见
[部署与运维](docs/deployment.md)。

### Docker 三节点集群

Compose 使用当前提交对应的 GitHub Actions Release artifact 构建三个服务节点和一个
FUSE 客户端。需要已认证的 GitHub CLI、Docker Compose，以及当前提交成功的
`Release` workflow 手动运行：

```bash
./scripts/download-release-artifact.sh
docker compose up --build -d
docker compose ps
```

检查集群和容器内挂载：

```bash
docker compose exec client esctl \
  --endpoints http://eventfs-node1:50051,http://eventfs-node2:50051,http://eventfs-node3:50051 \
  status
docker compose exec client mountpoint /mnt/eventfs
```

宿主公共端点为 `127.0.0.1:50051`、`:50052`、`:50053`。Compose 数据和挂载点
均为临时状态；`docker compose down` 后不会保留。产物选择、代理和容器权限边界见
[部署与运维](docs/deployment.md#docker-三节点集群)。

## 使用入口

### esctl

全局参数必须放在子命令之前：

```bash
esctl --endpoints http://127.0.0.1:50051 status
esctl append orders/1 --event-type OrderPlaced --data '{"quantity":1}'
esctl watch --stream orders/1
esctl persistent list
esctl aggregate list
```

完整参数、输出格式、退出码和 leader 发现策略见
[esctl 手册](docs/esctl.md)。

### Rust 客户端

`es-client` 提供 Stream、持久化订阅和 AggregateStore 客户端。可运行示例：

```bash
cargo run -p es-server --example client_example
```

调用方若直接使用 `es-proto` 生成的 gRPC 客户端，需要自行处理非 leader 返回的
`Unavailable` 和 `leader_addr`；`es-client` 已封装重定向重试。

### eventfs-fuse

`eventfs-fuse` 仅支持 Linux FUSE3，并以前台进程运行：

```bash
cargo build -p eventfs-fuse --locked
mkdir -p /data/eventfs
./target/debug/eventfs-fuse mount \
  --config eventfs-fuse.example.toml \
  /data/eventfs
```

默认仅挂载用户可访问；共享给其他本机用户必须显式传入 `--allow-other`。文件路径、
JSON 契约、CAS、消费确认和 errno 见 [EventFS FUSE 设计](docs/eventfs-fuse.md)。

## Release

`.github/workflows/release.yml` 仅支持手动触发。它在 Linux/macOS 的 x86_64、ARM64
原生 runner 上执行 workspace 默认测试并构建：

- 四个平台均发布 `eventstored`、`esctl`、README 和服务配置示例；
- Linux 产物额外包含 `eventfs-fuse` 与 FUSE 配置示例；
- 汇总 artifact 保留 30 天，包含四个平台压缩包和 `SHA256SUMS`；
- 版本名为 `sha-<7 位提交号>`，workflow 不创建 GitHub Release。

下载和校验方式见 [部署与运维](docs/deployment.md#release-产物)。

## 验证

默认测试：

```bash
cargo test --workspace --locked
```

真实多进程测试默认标记为 `#[ignore]`，需要串行执行：

```bash
cargo test -p es-server --test multi_node_test -- --ignored --test-threads=1
cargo test -p es-ctl --test multi_node_test -- --ignored --test-threads=1
```

Linux 真挂载测试还要求 `/dev/fuse` 与 `fusermount3`：

```bash
cargo test -p eventfs-fuse --test mount_e2e_test -- --ignored --test-threads=1
```

测试拓扑与故障场景见 [多节点集成测试](docs/multi_node_testing.md)，历史性能结果见
[性能基准](docs/benchmarks.md)。

## 已知限制

- 写入必须到达目标 Shard leader；SDK/CLI 会按 leader hint 重试，直接 gRPC 调用方需自行处理。
- 被网络隔离的旧 leader 不会立即退位，写入可能等待至客户端超时。
- `ReadAll` 的 HLC 归并不是跨 Shard 严格全序。
- 所有节点的 peers 和 placement 必须一致；配置分歧可能形成无法自动修复的双集群。
- AggregateStore 已有事件分区放置与 generation 领域模型，但当前没有事件分区数据迁移或自动再平衡 API。
- `eventfs-fuse` 不支持 macOS、后台化、删除/重命名、跨文件事务或本地离线写入。
- 快照为全量快照；安装时 surrealkv 单事务内存开销接近未压缩快照体积。

更多故障边界分别记录在
[设计文档](docs/design.md#10-本期不实现)、
[在线迁移](docs/migrate.md#已知限制)、
[快照](docs/snapshot.md#已知限制) 和
[FUSE 设计](docs/eventfs-fuse.md#1-目标与边界)。

## 文档

维护中的设计、运维和专题文档统一从 [文档索引](docs/README.md) 进入。

## License

Apache-2.0
