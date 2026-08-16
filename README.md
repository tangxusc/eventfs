# EventFS v2

EventFS v2 是基于 Rust、openraft 和 surrealkv 的分布式 AggregateStore。单个聚合实例由
`(business_space, aggregate_type, aggregate_id)` 标识；系统保存不可变事件、可覆盖状态
文档和消费者组进度。

本版本是 Aggregate-only 架构，不提供通用 EventStore Stream、实例级事件历史读取或旧协议
兼容。升级必须使用全新数据目录，旧客户端和旧 Raft 日志不能复用。

## 能力

| 能力 | 语义 |
|---|---|
| 聚合类型 | `RegisterAggregateType` 注册；每个类型固定 256 个虚拟分区 |
| 事件写入 | 单事件追加、实例级 OCC、`event_id` 幂等 |
| 类型级读取 | `FollowAggregateTypeEvents`；实例内按 `aggregate_version` 有序，实例间无全序 |
| 状态文档 | 按聚合实例读写，revision CAS，携带服务端修改时间 |
| 消费者组 | Fetch、Ack/Retry/Park/Skip、租约续期和连续进度 |
| 分布式存储 | 每个 Shard 独立 Raft group、LSM tree、成员管理和快照 |
| FUSE | Linux 上映射事件 JSONL、状态 JSON 和消费者组文件 |
| 传输安全 | 公共和内部 gRPC 均支持 TLS |

领域词汇见 [CONTEXT.md](CONTEXT.md)，架构不变量和数据流见
[docs/design.md](docs/design.md)，决策背景见
[ADR-0006](docs/adr/0006-aggregate-only.md)。

## 组件

| 二进制 / crate | 用途 |
|---|---|
| `eventstored` / `es-server` | AggregateStore、Raft 管理和节点间 gRPC 服务 |
| `es-client` | AggregateStore Rust SDK，含 leader 重定向和流式重连 |
| `esctl` / `es-ctl` | 聚合数据、消费者组、成员和快照 CLI |
| `eventfs-fuse` | Linux FUSE3 适配器 |
| `es-core` | 聚合领域模型、HLC 和错误类型 |
| `es-storage` | key 编码、Raft 日志和状态机存储 |
| `es-raft` | 多 Shard Raft 运行时 |
| `es-proto` | AggregateStore、RaftRpc 和 RaftAdmin protobuf 契约 |

公共 listener 注册 `AggregateStore`、`RaftRpc` 和 `RaftAdmin`；内部 listener 只注册
`AggregateStoreInternal`。生产环境必须通过网络策略限制 Raft 和内部端口访问。
配置热更新新增 Shard 时，存储创建、远端节点定位和新 AggregateType 的放置集合会作为
同一个运行期拓扑生效；任一步失败都会保留旧拓扑。

## 快速开始

需要 Rust 1.88+ 和 `protoc`。Debug 构建：

```bash
cargo build --workspace --bins --locked
```

最小单节点配置：

```toml
[node]
id = 1
listen_addr = "127.0.0.1:50051"
internal_listen_addr = "127.0.0.1:51051"

[storage]
data_dir = "./data/node1"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]
replica = []
```

启动并初始化：

```bash
./target/debug/eventstored --config ./config.toml
./target/debug/esctl init --shard 0 --member 1@127.0.0.1:50051 --yes
```

注册聚合类型并追加事件：

```bash
./target/debug/esctl aggregate type register orders order
./target/debug/esctl aggregate append orders order order-1 \
  --event-type OrderPlaced \
  --data '{"quantity":1}' \
  --expected-version no-aggregate
```

跟随类型级 feed：

```bash
./target/debug/esctl aggregate follow orders order --once
```

该 feed 会输出 `aggregate_id` 与实例内 `aggregate_version`；它不是按 `aggregate_id`
回放完整历史的接口。

## FUSE

`eventfs-fuse` 仅支持 Linux FUSE3：

```bash
cargo build -p eventfs-fuse --locked
mkdir -p /data/eventfs
./target/debug/eventfs-fuse mount --config eventfs-fuse.example.toml /data/eventfs
```

固定路径：

```text
/{business_space}/{aggregate_type}/events.jsonl
/{business_space}/{aggregate_type}/states
/{business_space}/{aggregate_type}/groups
```

详见 [docs/eventfs-fuse.md](docs/eventfs-fuse.md)。

## 文档

- [设计](docs/design.md)
- [部署与运维](docs/deployment.md)
- [esctl](docs/esctl.md)
- [快照](docs/snapshot.md)
- [多节点测试](docs/multi_node_testing.md)
- [基准测试](docs/benchmarks.md)

## 验证

```bash
cargo fmt --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

真实 FUSE mount e2e 需要 Linux、`/dev/fuse` 和相应权限；Darwin 只能运行后端与公共契约
测试。覆盖率门槛为行覆盖和分支覆盖均不低于 80%。

## License

Apache-2.0，见 [LICENSE](LICENSE)。
