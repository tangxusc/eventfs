# EventFS v2

![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)

基于 Rust + [openraft](https://github.com/datafuselabs/openraft) + [surrealkv](https://github.com/surrealdb/surrealkv) 的分布式事件存储中间件。按 `stream_id` 哈希分片，每个分片是一个独立的 Raft group，提供事件溯源语义：乐观并发、幂等写入、流订阅（catch-up → live）。

## 特性

- **多分片水平扩展**：`xxh3(stream_id) % num_shards` 路由，每分片独立 Raft group 选主与复制；单 LSM（surrealkv `Arc<Tree>`）按 key 前缀隔离全部分片
- **乐观并发控制**：`Any` / `NoStream` / `StreamExists` / `Exact(version)` 四种期望版本，校验在状态机 `apply` 内完成（单 Raft group 串行执行点，保证原子性）
- **幂等写入**：按 `event_id` 建索引，客户端重试（网络超时但实际已提交）不会产生重复事件
- **单事务原子提交**：事件、流元数据、position 指针、幂等索引、已应用状态在同一 surrealkv 事务内提交，崩溃不留下版本回退
- **混合逻辑时钟（HLC）**：leader 提交前分配并随日志下发，各副本 apply 出相同时间戳，为日后的近似全序预留基础
- **流订阅**：先补齐历史（catch-up）再转为实时推送（broadcast），追平边界不丢事件，落后时退回扫描补齐
- **跨分片 ReadAll**：按 HLC 做 k 路归并（保分片内 position 序），逐分片游标翻页，支持反向
- **离线 reshard**：变更分片数时按新路由重写数据，保留 `stream_id` / `version` / `event_id` / HLC，重新分配 position

## 架构

单个进程在**一个端口**上同时提供三个 gRPC 服务：

| 服务 | 用途 | 方法 |
|---|---|---|
| `EventStore` | 客户端 API | `Append` / `ReadStream` / `ReadAll` / `Subscribe` / `GetStreamMeta` |
| `RaftRpc` | 节点间复制与选举 | `AppendEntries` / `Vote` / `InstallSnapshot` |
| `RaftAdmin` | 集群管理 | `Initialize` / `AddLearner` / `ChangeMembership` / `GetRaftState` |

crate 依赖自上而下单向：

```
es-client ──┐
            ├─→ es-proto（gRPC 协议 + 端点归一化）
es-server ──┤
   │        └─→ es-core（Event / HLC / 路由）
   ↓
es-ctl（命令行管理工具，参照 etcdctl）
   │
es-raft（ShardManager、GrpcNetwork、Admin/RPC 服务）
   ↓
es-storage（RaftLogStorage + RaftStateMachine + reshard）
   ↓
surrealkv（单个 Tree，多分片按 key 前缀隔离）
```

集群组建：`node.peers` 非空时启动后自动组建（etcd 静态引导语义：探测无现存集群的节点用完整成员 `initialize` 一步到位），**每个分片独立组建**；`peers` 为空保留手动 `RaftAdmin` 路径（见下文）。

## 快速开始

### 构建

```bash
cargo build --bin eventstored
```

### 启动节点

每个节点需要**独立的数据目录**。用配置文件（TOML 或 JSON，按扩展名判断）：

```toml
# node1.toml
[node]
id = 1
listen_addr = "127.0.0.1:50051"

[storage]
data_dir = "./data/node1"

[shards]
num_shards = 8
```

```bash
./target/debug/eventstored --config node1.toml
./target/debug/eventstored --config node2.toml
./target/debug/eventstored --config node3.toml
```

`--node-id` 与 `--listen` 可覆盖配置文件对应项。

### 组建集群

**方式 A：自动组建（推荐）**——配置 `[[node.peers]]` 后启动即可，无需任何额外操作：

```toml
# node1.toml（node2/node3 除 id、listen_addr、data_dir 外相同）
[node]
id = 1
listen_addr = "127.0.0.1:50051"

# 必须包含本节点，且所有节点配置完全一致（不一致可能形成双集群）
[[node.peers]]
id = 1
addr = "127.0.0.1:50051"
[[node.peers]]
id = 2
addr = "127.0.0.1:50052"
[[node.peers]]
id = 3
addr = "127.0.0.1:50053"
```

每个节点启动后，日志为空的节点探测 peer 是否已有集群，无则用完整 peers 调用
`initialize` 一步到位（与 etcd `--initial-cluster` 语义一致）；已有日志的节点
（重启）自动跳过，从本地日志恢复。每个分片各组建一次（分片间不共享 membership 与 leader）。

**方式 B：手动组建（peers 为空时）**——通过 `RaftAdmin` 显式组建，每个分片各做一次：

```
对每个 shard_id in 0..num_shards：
  1. node1.Initialize(shard_id, [{1, "http://127.0.0.1:50051"}])   单成员自举，立刻成为 leader
  2. node1.AddLearner(shard_id, {2, addr2}, blocking=true)          学习者只收日志、不投票
  3. node1.AddLearner(shard_id, {3, addr3}, blocking=true)
  4. node1.ChangeMembership(shard_id, [1,2,3])                      一次性提升为 3 投票成员
```

直接 `Initialize([1,2,3])` 也收敛（随机化选举超时，已实测），单成员自举可保证
全程有 leader，便于排障。参考实现见 `es-server/tests/multi_node_test.rs` 的 `form_shard`。

### TLS（https，可选）

配置 `[tls]`（`cert_file` + `key_file`，PEM）即启用 TLS 监听；**TLS 部署时所有节点
`peers.addr` 必须显式写 `https://` 前缀**（裸地址会被补成 `http://`，节点间会以明文
直连 TLS 端口而失败）。证书可用 openssl 生成自签——**必须显式 `CA:FALSE`**（默认
`CA:TRUE` 的证书在严格校验模式下会被 rustls 拒绝，报 `CaUsedAsEndEntity`）：

```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout server.key -out server.crt \
  -days 365 -subj "/CN=127.0.0.1" -addext "subjectAltName=IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE"
```

信任策略（节点间 RPC、RaftAdmin 探测、客户端 API 统一）：
- **默认跳过证书校验**（自签友好，等价 curl -k）——仅建议内网/开发环境
- 配置 `ca_file` 后**严格校验**对端证书链（生产建议）——多自签节点需把全部节点证书
  拼接进同一 ca 文件，或使用真实 CA 签发

示例见 `config.example.toml`。证书轮换需重启节点生效。

### 客户端

```rust
use es_client::{EventStoreClient, EventBuilder, ExpectedVersionBuilder};

let mut client = EventStoreClient::connect(vec![
    "http://127.0.0.1:50051".to_string(),
]).await?;

let event = EventBuilder::new("OrderPlaced")
    .data_json(&serde_json::json!({ "order_id": "order-123", "amount": 99.99 }))?
    .build();

// 要求流不存在，即首次创建
let resp = client.append(
    "order-order-123".to_string(),
    ExpectedVersionBuilder::no_stream(),
    vec![event],
).await?;

println!("shard={} position={}", resp.shard_id, resp.first_position);
```

完整示例：`cargo run -p es-server --example client_example`（需先启动并组建好集群）。

连接 https 节点：`connect` 对 https 地址默认跳过证书校验；需要严格校验时用
`connect_with_tls`（CA 为 PEM 字节，可含多张证书）：

```rust
use es_client::{EventStoreClient, TlsClientConfig};

let mut client = EventStoreClient::connect_with_tls(
    vec!["https://127.0.0.1:50051".to_string()],
    Some(TlsClientConfig::Ca(std::fs::read("ca.crt")?)),
).await?;
```

### esctl 命令行工具

`esctl` 是参照 etcdctl 的管理工具（独立二进制，构建：`cargo build --bin esctl`），
覆盖数据面读写、订阅、集群组建与管理、端点健康、离线 reshard：

```bash
# 全局参数：--endpoints（逗号分隔多地址）、--dial-timeout、--timeout、
#            --cacert / --insecure-skip-tls-verify（https）、-w simple|table|json

# 数据面：写事件、读流、跨分片读 $all、查流元数据
esctl append orders/1 --event-type OrderPlaced --data '{"qty":1}' --expected-version nostream
esctl read orders/1 --from-version 0 --max-count 100
esctl readall --max-count 100          # 取满时输出下一页续读游标提示
esctl meta orders/1

# 订阅：先追平历史再实时推送；--once 追平即退出（脚本/测试用）
esctl watch orders/1 --from-start
esctl watch --all --from-start

# 管理面：初始化分片（每个分片独立 Raft group）、加/删成员、查看状态
esctl init --all-shards --member 1@127.0.0.1:50051 --member 2@127.0.0.1:50052
esctl member add --all-shards --member 3@127.0.0.1:50053
esctl member remove --all-shards --node-id 3
esctl member list
esctl status              # 各端点健康与分片归属

# 离线 reshard：变更分片数（需集群停机、先备份数据）
esctl reshard --src-dir ./data --src-shards 2 --dst-dir ./data-new --dst-shards 4 --yes
```

退出码：0 成功 / 1 运行时失败（连接失败、无 leader、乐观并发冲突等）/ 2 参数错误。
完整命令手册见 [docs/esctl.md](docs/esctl.md)。

## 测试

```bash
# 默认套件，90 项
cargo test --workspace

# 多节点测试：11 项（6 手动组建 + 5 自动组建），测试框架自动构建并直接运行二进制
cargo test -p es-server --test multi_node_test -- --ignored --test-threads=1
```

| 套件 | 项数 | 内容 |
|---|---|---|
| `es-core` | 9 | HLC 单调性、分片路由 |
| `es-proto` | 10 | gRPC 代码生成验证、TLS 信任策略、端点归一化 |
| `es-storage` | 48 | Key 编码排序性质、日志语义、apply、快照、reshard |
| `es-raft/partition_test` | 6 | 网络分区、快照追赶、慢节点（进程内可控网络层） |
| `es-server/e2e_test` | 19 | 端到端读写、乐观并发、订阅、跨分片 ReadAll、反向读取 |
| `es-server/server_test` | 1 | 服务器启动与分片初始化 |
| `es-server/multi_node_test` | 11 | 3 节点真实进程集群（6 项手动组建 + 5 项自动组建，`--ignored` 启用） |
| `es-ctl` 单测 | 47 | 参数解析、leader 提示解析、分片探测、输出渲染 |
| `es-ctl/e2e_test` | 15 | 进程内全链路：读写/订阅/管理面/TLS/双节点成员管理 |
| `es-ctl/reshard_test` | 5 | 离线 reshard 端到端（数据完整、负例、LOCK 约束） |
| `es-ctl/multi_node_test` | 2 | esctl 组建三节点真实进程集群（`--ignored` 启用） |

多节点测试标为 `#[ignore]`：每项要拉起 3 个进程。串行运行以免争抢端口：

```bash
cargo test -p es-server --test multi_node_test -- --ignored --test-threads=1
cargo test -p es-ctl --test multi_node_test -- --ignored --test-threads=1
```

```bash
# 存储层基准
cargo bench -p es-storage
```

## 文档

| 文档 | 内容 |
|---|---|
| [docs/](docs/README.md) | 文档索引（设计 + 专题） |
| [docs/design.md](docs/design.md) | Key 编码与排序性质证明、写入路径、HLC、全序边界 |
| [docs/esctl.md](docs/esctl.md) | esctl 完整命令手册（参数、输出格式、leader 发现策略） |
| [docs/multi_node_testing.md](docs/multi_node_testing.md) | 多节点与分区测试、集群组建流程、踩坑记录 |
| [docs/snapshot.md](docs/snapshot.md) | 快照四方法实现要点、参数权衡 |
| [docs/reshard.md](docs/reshard.md) | 分片变更三种方案对比与离线方案设计 |
| [docs/benchmarks.md](docs/benchmarks.md) | 基准结果与未覆盖场景 |

## 路线图

以下为未实现的功能与计划（不承诺时间）：

**客户端 SDK**
- [ ] `es-client` 封装剩余 API：`ReadAll` / `Subscribe` / `GetStreamMeta`（当前需直接用 `es-proto` 生成的 gRPC 客户端）
- [ ] `es-client` 内置 leader 重定向重试（当前需调用方处理 `Unavailable` 响应中的 `leader_addr`）
- [ ] 多语言客户端（Python / Go / Java）

**存储与快照**
- [ ] 快照压缩（zstd / lz4），减小体积与传输量
- [ ] 快照存独立文件，与业务数据分离；分块传输大快照
- [ ] 保留多个历史快照，支持时间点恢复

**集群运维**
- [x] reshard 命令行工具（`esctl reshard`，基于 `es_storage::reshard::reshard()`）
- [x] reshard 含数据完整测试（es-ctl 的 reshard_test：流/事件数一致、version/event_id 不变）
- [ ] reshard 并行处理与增量重分布（中断后可续跑）
- [ ] 在线分片变更（方案 B 分裂/合并 或方案 C 虚拟节点，需架构级改动）
- [ ] 磁盘故障注入测试
- [ ] 跨分片 `$all` 订阅（当前只读 shard 0）

**可观测性与基准**
- [ ] Prometheus 指标（当前只有 tracing 日志）
- [ ] 端到端写入吞吐基准（单节点集群 gRPC 压测 Append）
- [ ] 大流读取延迟（1k / 10k / 100k 事件的流）
- [ ] Raft 复制延迟（leader 写入到 follower apply 的时间分布）
- [ ] 真实数据量的 reshard 基准（含事件而非只有 StreamMeta）

**功能**
- [ ] Projection 机制
- [ ] 持久化订阅

## 已知限制

- **写入必须打到 leader**：服务端不做透明转发。非 leader 返回 `Unavailable`，
  message 中带 `leader_addr=...`，客户端据此重定向。`es-client` 尚未内置该重试逻辑。
- **向被隔离的旧 leader 写入会挂起而非快速失败**：openraft 0.9 无租约退位机制，
  被隔离的 leader 在自己视角里仍是 leader。客户端必须设写超时。
- **跨分片非严格全序**：`ReadAll` 按 HLC 归并，只保证分片内严格按 position。
  HLC 由各 leader 的墙上时钟推进，跨分片顺序是近似的。
- **自动组建要求所有节点 `node.peers` 配置完全一致**：不一致可能形成双集群
  （与 etcd 相同，运行时无法自动修复；探测到已初始化 peer 的 voter_ids 与配置
  不符时节点会告警并放弃自举）。已有节点重启不参与组建，从本地日志恢复。
- **极端窗口双故障**：节点在"收到选票但日志为空"的子秒级窗口内宕机，且其余
  节点也宕机，可能卡在无日志有选票状态——清空该节点数据目录重建即可恢复。
- **reshard 需停机**：`esctl reshard` 直接操作数据目录（集群未停时 LOCK 占用会拒绝执行），
  运行前须备份。reshard 后客户端的 position 游标失效，需从头读或用别的方式续读。
- **`esctl member remove` 无法移除 learner**：RaftAdmin 无 remove_learner RPC；
  `member list` 不含地址列与 learner 行（GetRaftState 不暴露成员地址）。
- **`esctl watch --all` 仅支持分片 0**：服务端 `$all` 订阅的已知限制。
- **快照为全量、未压缩**：每次 `build_snapshot` 用 serde_json 序列化整个分片状态机，
  大状态机体积偏大。

## 技术栈

| 组件 | 版本 | 备注 |
|---|---|---|
| Rust | 1.88+ / edition 2024 | |
| openraft | 0.9.25 | features: `storage-v2`, `serde` |
| surrealkv | 0.21.3 | 单 Tree 多分片，key 前缀隔离 |
| tonic | 0.14.6 | features: `tls-ring`（https 节点间通信与客户端 API） |
| rustls | 0.23 | ring 后端；TLS 信任策略封装在 es-proto |
| tokio | 1.48 | |
| xxhash-rust | 0.8.18 | xxh3 算法固定，可安全用于持久化路由 |

## License

Apache-2.0
