# EventFS v2

![License](https://img.shields.io/badge/License-Apache--2.0-blue.svg)

基于 Rust + [openraft](https://github.com/datafuselabs/openraft) + [surrealkv](https://github.com/surrealdb/surrealkv) 的分布式事件存储中间件。分片由**显式放置表**（`[placement]`）定义，每个分片是一个独立的 Raft group + 独立的 surrealkv LSM tree；stream → shard 归属由服务端**流路由表**（`routes.json`）显式分配，支持运行期动态扩容与在线迁移。提供事件溯源语义：乐观并发、幂等写入、流订阅（catch-up → live）。

## 特性

- **显式放置表分片**：`[placement]` 配置（`replication_factor` 默认 2、每节点 primary/replica shard 列表）取代旧 `[shards] num_shards`；每个 shard 的 raft 成员 = 放置表中承载它的节点子集，各分片独立选主与复制
- **per-shard 独立 LSM tree**：每个 shard 一个独立 surrealkv tree（`{data_dir}/shard-{id}/`，memtable 4MiB），分片独立落盘、独立 LOCK、崩溃域隔离，取代「共享单 tree + key 前缀隔离」
- **流路由表（显式分配）**：`{data_dir}/routes.json` 记录 stream → shard（+ per-shard 计数 + 版本号）；`esctl create-stream` 与隐式建流由服务端分配（大致最少流）；watcher 热更新 + 整表广播 + 版本仲裁跨节点收敛
- **乐观并发控制**：`Any` / `NoStream` / `StreamExists` / `Exact(version)` 四种期望版本，校验在状态机 `apply` 内完成（单 Raft group 串行执行点，保证原子性）
- **幂等写入**：按 `event_id` 建索引，客户端重试（网络超时但实际已提交）不会产生重复事件
- **单事务原子提交**：事件、流元数据、position 指针、幂等索引、已应用状态在同一 surrealkv 事务内提交，崩溃不留下版本回退
- **混合逻辑时钟（HLC）**：leader 提交前分配并随日志下发，各副本 apply 出相同时间戳，为日后的近似全序预留基础
- **流订阅**：先补齐历史（catch-up）再转为实时推送（broadcast），追平边界不丢事件，落后时退回扫描补齐
- **跨分片 ReadAll**：按 HLC 做 k 路归并（保分片内 position 序），服务端按归并消费水位
  驱动逐分片续读游标（`next_positions`，客户端原样透传翻页），支持反向；
  反向读到分片最早事件后游标带 `ended` 读尽标记，**空页即终止**（正反两向一致）
- **运行期动态扩容**：加节点 = 更新所有节点配置 → 各节点 watch 热更新 → 动态创建
  新 shards 并自举，全程无需重启
- **在线迁移**：`esctl migrate` 取代离线 reshard——`Preparing → FullCopying → Tailing →
  Switching → Draining → Verifying → Finalizing`，流数据处理不暂停；排水冲突 Any
  兜底、重跑自愈、孤儿流自动定位
- **优雅退出**：Ctrl-C / SIGTERM → 逐 shard 停 Raft + 关闭存储（flush WAL 并释放 LOCK）

## 架构

单个进程在**一个端口**上同时提供四个 gRPC 服务：

| 服务 | 用途 | 方法 |
|---|---|---|
| `EventStore` | 客户端 API | `Append` / `ReadStream` / `ReadAll` / `Subscribe` / `GetStreamMeta` / `CreateStream` |
| `RaftRpc` | 节点间复制与选举 | `AppendEntries` / `Vote` / `InstallSnapshot` |
| `RaftAdmin` | 集群管理 | `Initialize` / `AddLearner` / `ChangeMembership` / `GetRaftState` / `ListShards` |
| `Migration` | 路由表同步 + 在线迁移原语（节点间） | `GetRouteTable` / `PushRouteTable` / `SetStreamShard` / `RecountStreams` / `AppendMigrated` / `DeleteStreamFromShard` / `ReadStreamFromShard` / `GetStreamMetaFromShard` / `ListStreams` |

crate 依赖自上而下单向：

```
es-client ──┐
            ├─→ es-proto（gRPC 协议 + 端点归一化 + TLS 信任策略）
es-server ──┤
   │        └─→ es-core（Event / HLC / 流路由表 RouteTable / 错误类型）
   ↓
es-ctl（命令行管理工具，参照 etcdctl）
   │
es-raft（TypeConfig、ShardManager、GrpcNetwork、Admin/RPC 服务）
   ↓
es-storage（RaftLogStorage + RaftStateMachine，每 shard 一个 EsStorage）
   ↓
surrealkv（每 shard 一个独立 LSM tree：{data_dir}/shard-{id}/）
```

**存储布局**：每 shard 一个独立 surrealkv LSM tree（`{data_dir}/shard-{id}/`，独立 LOCK）。
memtable arena 默认 4MiB（`[storage] memtable_arena_bytes`）——surrealkv 默认 100MB
且打开即预分配，per-shard 布局下 N 个 shard 就是 N 个实例，必须调小。

**流路由表**（`RouteTableManager`）与 ShardManager 并列：写路径先查/分配
stream → shard 归属（`{data_dir}/routes.json`），再按归属寻址分片。未知名流写入 =
隐式建流（锁内双检查，同节点并发不重复分配）；读未建流 → NotFound（显式分配语义）。
跨节点收敛靠**整表广播 + 版本仲裁**（接收方只采纳更高版本，重复广播幂等）；
watcher 监听本地文件热更新（运维手工改文件、版本更高同样生效），损坏文件保留内存
旧表并告警。

**集群组建**：`node.peers` 非空时启动后自动组建（etcd 静态引导语义：探测无现存集群
的节点用完整成员 `initialize` 一步到位），**每个分片独立组建**，成员 = 放置表中承载
该 shard 的节点子集（未承载的节点不注册它）；`peers` 为空保留手动 `RaftAdmin` 路径
（见下文）。

**优雅退出**：Ctrl-C / SIGTERM → 停 watcher → 逐 shard 先 `raft.shutdown()` 再
`storage.close()`（flush WAL + 释放 surrealkv LOCK，顺序不可颠倒——先停 Raft 避免
关闭期间后台任务还在写；不显式关闭则重启同目录会报 "already locked"）。

## 快速开始

### 构建

```bash
cargo build --bin eventstored
```

### 启动节点

每个节点需要**独立的数据目录**。用配置文件（TOML 或 JSON，按扩展名判断），
分片由 `[placement]` 显式定义。**2 节点 rf=1 示例**（每 shard 一个投票成员，无副本）：

```toml
# node1.toml（node2.toml 除 id、listen_addr、data_dir 外相同）
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

[storage]
data_dir = "./data/node1"
# memtable_arena_bytes 默认 4MiB；每 shard 一个 LSM tree，N 个 shard 即 N 个实例
memtable_arena_bytes = 4194304

# 2 节点 rf=1：shard 0/1 由 node1 承载，shard 2/3 由 node2 承载
[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0, 1]
replica = []

[[placement.nodes]]
id = 2
primary = [2, 3]
replica = []
```

```bash
./target/debug/eventstored --config node1.toml
./target/debug/eventstored --config node2.toml
```

`--node-id` 与 `--listen` 可覆盖配置文件对应项。**3 节点 rf=2 示例**见
`config.example.toml`（6 个 shard 环形分布，每 shard 有 primary + replica 共 2 个
投票成员）。

### 组建集群

**方式 A：自动组建（推荐）**——配置 `[[node.peers]]` 后启动即可，无需任何额外操作。

每个节点启动后，日志为空的节点探测 peer 是否已有集群，无则用完整 peers 调用
`initialize` 一步到位（与 etcd `--initial-cluster` 语义一致）；已有日志的节点
（重启）自动跳过，从本地日志恢复。**每个分片各组建一次**（分片间不共享 membership
与 leader），成员由放置表推导。

**方式 B：手动组建（peers 为空时）**——通过 `RaftAdmin` 显式组建，每个分片各做一次：

```
对放置表中的每个 shard_id：
  1. node1.Initialize(shard_id, [{1, "http://127.0.0.1:50051"}])   单成员自举，立刻成为 leader
  2. node1.AddLearner(shard_id, {2, addr2}, blocking=true)          学习者只收日志、不投票
  3. node1.ChangeMembership(shard_id, [1,2])                        一次性提升为投票成员
```

直接 `Initialize([1,2])` 也收敛（随机化选举超时，已实测），单成员自举可保证
全程有 leader，便于排障。参考实现见 `es-server/tests/multi_node_test.rs` 的 `form_shard`。

### 运行期动态扩容（不重启）

加节点 = 更新**所有节点**配置（`[[node.peers]]` 新增节点 + `[[placement.nodes]]`
新增节点与 shards 行）→ 各节点 watcher 热加载（解析/校验失败保留旧配置、服务
不受影响）→ diff 出新增的本地 shards → 动态创建并单分片自举（幂等）。配置中移除的
shard 仅告警，数据目录保留，重新加入时幂等打开恢复。新 shard 加入后可用
`esctl migrate` 把热点流迁入以利用新增承载。

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

// 显式创建流：服务端分配 shard（大致最少流）并记录路由表；幂等，已存在返回现有归属
let resp = client.create_stream("order-order-123".to_string()).await?;
println!("stream on shard={} exists={}", resp.shard_id, resp.exists);

let event = EventBuilder::new("OrderPlaced")
    .data_json(&serde_json::json!({ "order_id": "order-123", "amount": 99.99 }))?
    .build();

// 追加事件（append 未知名流也会隐式建流并分配归属）
let resp = client.append(
    "order-order-123".to_string(),
    ExpectedVersionBuilder::any(),
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
覆盖数据面读写、订阅、集群组建与管理、端点健康、流路由表与在线迁移：

```bash
# 全局参数：--endpoints（逗号分隔多地址）、--dial-timeout、--timeout、
#            --cacert / --insecure-skip-tls-verify（https）、-w simple|table|json

# 数据面：显式建流、写事件、读流、跨分片读 $all、查流元数据
esctl create-stream orders/1              # 服务端分配 shard 并记录路由表（幂等）
esctl append orders/1 --event-type OrderPlaced --data '{"qty":1}' --expected-version nostream
esctl read orders/1 --from-version 0 --max-count 100
esctl readall --max-count 100          # 取满时输出下一页续读游标（服务端 next_positions 驱动）
esctl meta orders/1

# 订阅：先追平历史再实时推送；--once 追平即退出（脚本/测试用）
esctl watch orders/1 --from-start
esctl watch --all --shard 1 --from-start   # $all 订阅分片 1（一次一个分片）

# 流路由表：查看 / 校准计数 / 孤儿流检测
esctl route                    # 展示 stream -> shard 归属与表版本
esctl route --recount          # 校准 per-shard 流计数（迁移后建议执行）
esctl route --check            # 孤儿流检测（对比各分片实际存储与路由表）

# 在线迁移：把流从当前 shard 迁到 shard 3（流数据处理不暂停，取代离线 reshard）
esctl migrate --stream orders/1 --to 3 --dry-run   # 先看迁移计划与版本差
esctl migrate --stream orders/1 --to 3

# 管理面：初始化分片（每个分片独立 Raft group）、加/删成员、查看状态、快照
esctl init --all-shards --member 1@127.0.0.1:50051 --member 2@127.0.0.1:50052
esctl member add --all-shards --member 3@127.0.0.1:50053
esctl member remove --all-shards --node-id 3
esctl member list
esctl status              # 各端点健康与分片归属
esctl snapshot list ./data/node1
esctl snapshot restore ./data/node1 ./data/node1/snapshots/snap-0-1-100.esnap --yes
```

退出码：0 成功 / 1 运行时失败（连接失败、无 leader、乐观并发冲突等）/ 2 参数错误。
完整命令手册见 [docs/esctl.md](docs/esctl.md)。

## 测试

```bash
# 默认套件，422 项全绿
cargo test --workspace

# 多节点测试：12 项（7 手动组建 + 5 自动组建），测试框架自动构建并直接运行二进制
cargo test -p es-server --test multi_node_test -- --ignored --test-threads=1
```

| 套件 | 项数 | 内容 |
|---|---|---|
| `es-core` | 42 | HLC 单调性、流路由表分配与版本仲裁（最少流/双检查/recount/跨节点仲裁）、leader 重定向策略 |
| `es-proto` | 10 | gRPC 代码生成验证、TLS 信任策略、端点归一化 |
| `es-storage` | 89 | Key 编码排序性质、日志语义、apply、快照往返、模糊测试（随机 append/delete 不变量） |
| `es-raft` | 34 | ShardManager 注册与寻址、RaftAdmin 参数校验、网络分区、慢节点、消息大小限制（进程内可控网络层） |
| `es-server` | 92 | 服务器启动、bootstrap 自动组建、路由表 watcher 热更新、端到端读写/乐观并发/订阅/跨分片 ReadAll/反向读取与翻页终止 |
| `es-server/multi_node_test` | 12 | 3 节点真实进程集群（7 项手动组建 + 5 项自动组建，`--ignored` 启用） |
| `es-client` | 39 | SDK 单测 + stub 集成 + 进程内 e2e（重定向重试、正反向翻页、订阅含 catch-up 窗口、元数据） |
| `es-ctl` 单测 | 79 | 参数解析、leader 提示解析、分片探测、输出渲染、重定向重试 |
| `es-ctl/e2e_test` | 29 | 进程内全链路：读写/订阅/管理面/TLS/成员管理/翻页/CAS + 在线迁移场景矩阵（单流/批量/干跑/重跑幂等/孤儿自动定位/切换中断自愈） |
| `es-ctl/client_failover_test` | 4 | 端点故障转移（写重试、leader 未知退避、轮询起点分散负载） |
| `es-ctl/snapshot_test` | 4 | 快照 list/restore 端到端（离线 LOCK 约束） |
| `es-ctl/multi_node_test` | 2 | esctl 组建三节点真实进程集群（`--ignored` 启用） |

多节点测试标为 `#[ignore]`：每项要拉起 3 个进程。串行运行以免争抢端口：

```bash
cargo test -p es-server --test multi_node_test -- --ignored --test-threads=1
cargo test -p es-ctl --test multi_node_test -- --ignored --test-threads=1
```

覆盖率（`cargo llvm-cov --workspace`）：行 87.87%、分支 80.24%。

```bash
# 存储层基准
cargo bench -p es-storage
```

## 文档

| 文档 | 内容 |
|---|---|
| [docs/](docs/README.md) | 文档索引（设计 + 专题） |
| [docs/design.md](docs/design.md) | 架构设计总览：Key 编码与排序性质证明、写入路径、HLC、放置表、流路由表、gRPC 接口、测试策略 |
| [docs/esctl.md](docs/esctl.md) | esctl 完整命令手册（参数、输出格式、leader 发现策略） |
| [docs/migrate.md](docs/migrate.md) | 在线迁移设计（状态机、幂等原语、切换窗口、断点续传）、esctl migrate / route 用法 |
| [docs/multi_node_testing.md](docs/multi_node_testing.md) | 多节点与分区测试、集群组建流程、踩坑记录 |
| [docs/snapshot.md](docs/snapshot.md) | 快照四方法实现要点、参数权衡 |
| [docs/benchmarks.md](docs/benchmarks.md) | 基准结果与未覆盖场景 |

## 路线图

以下为未实现的功能与计划（不承诺时间）：

**客户端 SDK**
- [x] `es-client` 封装剩余 API：`ReadAll` / `Subscribe` / `GetStreamMeta` / `CreateStream`（此前需直接用 `es-proto` 生成的 gRPC 客户端）
- [x] `es-client` 内置 leader 重定向重试（此前需调用方处理 `Unavailable` 响应中的 `leader_addr`）
- [ ] 多语言客户端（Python / Go / Java）

**存储与快照**
- [x] 快照压缩（zstd / lz4，配置可选），减小体积与传输量
- [x] 快照存独立文件（`{data_dir}/snapshots/`），与业务数据分离；分块传输从文件流式读块
- [x] 保留多个历史快照（`[snapshot] keep`），`esctl snapshot list/restore` 支持时间点恢复

**集群运维**
- [x] 显式放置表（`[placement]`）与流路由表（`routes.json` 热更新 + 版本仲裁）
- [x] 运行期动态扩容（配置热更新 + 动态创建 shards，不重启）
- [x] 在线迁移（`esctl migrate`：单流/批量/断点续传/孤儿流修复，取代离线 reshard）
- [ ] 磁盘故障注入测试
- [ ] 跨分片 `$all` 聚合订阅（当前 `--all` 按 `--shard` 单分片订阅，多分片需各自发起）
- [ ] 自动再平衡（按流量而非仅按流数触发迁移）

**可观测性与基准**
- [ ] Prometheus 指标（当前只有 tracing 日志）
- [ ] 端到端写入吞吐基准（单节点集群 gRPC 压测 Append）
- [ ] 大流读取延迟（1k / 10k / 100k 事件的流）
- [ ] Raft 复制延迟（leader 写入到 follower apply 的时间分布）
- [ ] 真实数据量的迁移基准（含事件而非只有 StreamMeta）

**功能**
- [ ] Projection 机制
- [ ] 持久化订阅

## 已知限制

- **写入必须打到 leader**：服务端不做透明转发。非 leader 返回 `Unavailable`，
  message 中带 `leader_addr=...`。`es-client`（append）与 `es-ctl`（with_leader）
  均已内置重定向重试（策略共用 `es-core::redirect`）；直接使用 gRPC 客户端的
  调用方需自行处理。
- **向被隔离的旧 leader 写入会挂起而非快速失败**：openraft 0.9 无租约退位机制，
  被隔离的 leader 在自己视角里仍是 leader。客户端必须设写超时。
- **跨分片非严格全序**：`ReadAll` 按 HLC 归并，只保证分片内严格按 position。
  HLC 由各 leader 的墙上时钟推进，跨分片顺序是近似的。
- **自动组建要求所有节点 `node.peers` 配置完全一致**：不一致可能形成双集群
  （与 etcd 相同，运行时无法自动修复；探测到已初始化 peer 的 voter_ids 与配置
  不符时节点会告警并放弃自举）。已有节点重启不参与组建，从本地日志恢复。
- **极端窗口双故障**：节点在"收到选票但日志为空"的子秒级窗口内宕机，且其余
  节点也宕机，可能卡在无日志有选票状态——清空该节点数据目录重建即可恢复。
- **迁移切换→收敛窗口读者可见性 <1s**：切换点（`SetStreamShard`）后路由表整表
  广播到全部节点前，打到未收敛节点的读者仍按旧归属读源分片（读到的是迁移前
  数据，无损坏），窗口通常 <1s；广播失败由下次变更全表重发自愈。
- **跨节点并发首建同一流有孤儿流残留窗口**：多个节点同时首次写入同一个未知名流时，
  可能各自分配归属、未收敛一侧产生孤儿流（存储有数据但路由表无记录）；
  `esctl route check` 检测、`esctl migrate` 合并修复。
- **`esctl member remove` 无法移除 learner**：RaftAdmin 无 remove_learner RPC；
  `member list` 不含地址列与 learner 行（GetRaftState 不暴露成员地址）。
- **`esctl watch --all` 单分片订阅**：`--shard <N>` 指定分片（默认 0），多分片 $all
  需各自发起订阅；跨分片聚合订阅尚未实现。
- **快照为全量**：每次 `build_snapshot` 序列化整个分片状态机，大状态机耗时明显
  （支持 zstd/lz4 压缩与多快照保留，见 docs/snapshot.md）。
- **install 单事务内存 ≈ 快照未压缩体积**：surrealkv 事务写入全内存缓冲，超大快照
  不适用（失败时事务原子，旧数据无损）。
- **快照分块与 append 批量共用 8MB 消息上限**：快照分块（3MiB 默认，上限 6MiB
  启动校验）有余量；append 超限由网络层映射为 openraft PayloadTooLarge 拆小
  重试（可自愈），单事件（1MiB）与批次（7MiB，`[limits]` 可配）超限在服务端
  权威拒绝、客户端本地前置校验。

## 技术栈

| 组件 | 版本 | 备注 |
|---|---|---|
| Rust | 1.88+ / edition 2024 | |
| openraft | 0.9.25 | features: `storage-v2`, `serde` |
| surrealkv | 0.21.3 | 每 shard 一个独立 Tree（`{data_dir}/shard-{id}/`，memtable 4MiB） |
| tonic | 0.14.6 | features: `tls-ring`（https 节点间通信与客户端 API） |
| rustls | 0.23 | ring 后端；TLS 信任策略封装在 es-proto |
| tokio | 1.48 | |
| xxhash-rust | 0.8.18 | xxh3 算法固定；仅作 esctl 客户端预选提示（服务端落盘路由表为权威） |

## License

Apache-2.0
