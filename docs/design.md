# EventStore 设计文档

分布式事件存储中间件。独立进程启动，多节点集群，基于 openraft 共识与 surrealkv 嵌入式存储，
客户端与节点间通信统一使用 gRPC。

- 文档版本：1.3（2026-08-12 显式放置表 + 流路由表 + 在线迁移）
- 建立日期：2026-08-10
- 状态：已实现；标注「已实现」的章节**以代码为准**

## 1. 已确认的设计决策

| 决策项 | 选定方案 | 关键理由 |
|---|---|---|
| 事件语义 | EventStore 风格事件溯源 | Stream 为聚合根单位，append-only，版本号递增，乐观并发 |
| openraft 版本 | 0.9.25 稳定版 | 已实测编译通过；0.10 仍为 alpha，破坏性变更会反复打断开发与测试 |
| 分片存储布局 | 每 shard 一个独立 surrealkv LSM tree（`{data_dir}/shard-{id}/`），key 内保留 shard 前缀 | 分片独立落盘、独立 LOCK、崩溃域隔离；memtable arena 调小（默认 4MiB）避免 N×100MB 内存；shard 前缀保留供迁移工具按显式 shard 寻址 |
| 全局顺序语义 | 仅分片内有序 + 记录 HLC | 无写入瓶颈，可水平扩展；HLC 已落盘，日后可平滑加近似全序 |
| crate 结构 | 多 crate 拆分 | 边界清晰、可并行编译、可独立测试，客户端 SDK 可单独发布 |
| 节点发现 | 静态配置 | 开发期 3 节点，节点数可配置 |
| 数据分片 | multi-raft + Raft-backed Stream 归属（stream → shard） | 单 stream 完整落在一个 Raft group；控制 Shard 强一致提交归属，`routes.json` 仅为兼容投影 |
| 管理 API | 已实现（`RaftAdmin`：Initialize / AddLearner / ChangeMembership / GetRaftState / ListShards；`Migration`：路由表同步 + 迁移原语） | 组建双路径（自动按 peers 配置 / 手动 RaftAdmin），见 7.3；迁移见 7.5 |

## 2. 依赖验证证据

以下结论均由临时 spike 项目**真实编译**得出，而非文档推测。

| 验证项 | 实测结论 |
|---|---|
| 版本共存 | openraft 0.9.25 + surrealkv 0.21.3 + tonic 0.14.6 + prost 0.14.4 编译通过（28.89s） |
| `Sealed` bound | 不阻止外部实现，`RaftLogStorage`/`RaftStateMachine` 可在本项目内实现 |
| surrealkv 入口 | 0.21.3 已无 `Store`，改为 `TreeBuilder::new().with_path(PathBuf).build()` → `Tree` |
| `Tree` 线程安全 | `Send + Sync + 'static` 成立，每分片一个独立 `Arc<Tree>`（不再共享） |
| `range()` 形态 | 游标式：`seek_first()`/`next()` 返回 `Result`，`valid() -> bool`，`value()`；区间 `[start, end)` |
| 工具链 | rustc 1.94.1 满足 surrealkv（1.86+）与 tonic 0.14.6（1.88+，edition 2024） |

### 2.1 由验证结论导出的硬性约束

1. **固定宽度大端编码**：Raft 日志 index 与事件 version 在 key 中必须编码为 8 字节 big-endian。
   surrealkv 的 `range()` 按字节序扫描，若用变长或小端编码，字节序不等于数值序，扫描结果会错乱。
2. **删除语义区分**：`delete()` 硬删除一个 key 的所有历史版本，`soft_delete()` 才是打墓碑。
   Raft 的 `purge`/`truncate` 语义是永久移除日志，必须用 `delete()`。
3. **`range()` 两端类型一致**：`range<K: IntoBytes>(start: K, end: K)` 共用同一泛型参数，
   两个边界必须是相同类型，统一使用 `Vec<u8>`。

### 2.2 依赖 feature 与配套 crate（易错点）

**openraft 0.9 默认不启用任何 feature**，必须显式开启：

| feature | 是否启用 | 理由 |
|---|---|---|
| `storage-v2` | 启用 | 将 `RaftLogStorage`/`RaftStateMachine` 启用为 v2 存储，log IO 与状态机 IO 可并行；同时禁用 v1 兼容层 `Adapter`（我们直接实现 v2，不需要） |
| `serde` | 启用 | 为 `Vote`、`Entry`、`AppendEntriesRequest` 等跨存储与网络边界的类型派生序列化，surrealkv 持久化与 gRPC payload 都依赖它 |
| `generic-snapshot-data` | 不启用 | 保持默认，`SnapshotData` 为自定义 `SnapshotFile`（包装 `tokio::fs::File`，满足 `AsyncRead + AsyncWrite + AsyncSeek + Unpin`），复用 openraft 提供的分块 `install_snapshot` 默认实现（Chunked 直接从文件流式读块，见 docs/snapshot.md） |
| `single-term-leader` | 不启用 | 保持标准 `LogId = (term, node_id, index)` |
| `loosen-follower-log-revert` | 不启用 | 官方明确警告勿用 |

注：spike 中未开 `storage-v2` 时，`L: RaftLogStorage<TypeConfig>` 的 where 约束断言仍然通过，
说明这两个 trait 是无条件公开的；该 feature 影响的是 `Raft::new()` 的接入与 `Adapter` 的存在。
因此「`Raft::new()` 能接受我们的存储实现」这一点在 es-raft 阶段仍需实测确认，不能凭 spike 结论推定。

**tonic 0.14 已将 prost codec 拆出**，需要三个 crate 协同：

| crate | 位置 | 作用 |
|---|---|---|
| `tonic` | 依赖 | gRPC 运行时与传输 |
| `tonic-prost` | 依赖 | prost codec 实现，生成代码在运行期编解码消息靠它 |
| `tonic-prost-build` | build-dependencies | 构建期 codegen，取代旧版直接用 `tonic-build` 的写法 |

仅加 `tonic` + `prost` 可以编译通过（spike 即如此），但一旦真正生成并使用 gRPC 代码就会失败。

**TLS（https）**：tonic 需显式开 `tls-ring` feature（rustls ring 后端，tokio-rustls 0.26 →
rustls 0.23）；rustls 直接依赖仅 es-proto 一处（`NoCertVerify` 内部实现，类型不外泄）。
客户端跳过校验的唯一官方路径是 `Endpoint::tls_config_with_verifier` + 自定义
`ServerCertVerifier`——注意 tonic 生成代码的 `connect` 对 https 自动套空 roots
（自签必然失败），所有 https 装配必须统一走 `es_proto::tls::apply_endpoint_tls`。

## 2.3 TLS 信任策略与地址语义

- **监听协议**由 `[tls]`（cert_file+key_file）存在性决定；`listen_addr` 保持裸 bind 地址。
- **对端协议**由 `peers.addr` 的 scheme 决定（`normalize_endpoint` 保留 http/https 前缀，
  裸地址补 http）——**TLS 部署必须显式写 `https://`**，混合 http/https 集群按 peer
  scheme 逐端应用，天然支持。
- **信任策略**：`[tls].ca_file` 存在 → 严格校验对端证书链（PEM 可含多张，多自签节点
  需拼接全部证书）；缺省 → 跳过校验（自签友好，等价 curl -k，仅建议内网/开发）。
  读取失败绝不静默降级（bootstrap 跳过组建并告警）。
- **es-client**：`connect` 对 https 默认跳过校验；`connect_with_tls(nodes, Ca(pem))`
  严格校验。
- **安全含义**：跳过校验的部署可被中间人攻击；生产建议 ca_file 严格校验。
  证书轮换需重启节点生效（serve 每次从磁盘重读）。

## 3. 整体架构

```
                    ┌────────────────────────────────────────┐
                    │                                        │
                       ┌──────────────────────────┐           
                       │EventStore / RaftRpc      │           
                       │RaftAdmin / Migration     │           
                       │四服务共用单端口            │           
                       └────────────┬─────────────┘           
                                                     ┌──────┐ 
                                                     │Route │ 
                                                     │Table │ 
                                                     │Mgr   │ 
                                                     └──┬───┘ 
                    │               ▼                   ▼    │
                    │               │                   │    │
                       ┌──────────────────────────┐           
                       │es-raft ShardManager      │           
                       │  Raft[0] Raft[1] ...     │           
                       └────────────┬─────────────┘           
                    │               ▼                   │    │
                    │               │                   │    │
                       ┌──────────────────────────┐           
                       │es-storage                │           
                       │LogStorage + StateMachine │           
                       └────────────┬─────────────┘           
                    └───────────────┼───────────────────┼────┘
                                    ▼                   ▼
                      {data_dir}/shard-{id}/      {data_dir}/routes.json
                      每 shard 一个独立           stream → shard 归属
                      surrealkv LSM tree            （watcher 热更新 + 广播同步）
```

客户端 API、Raft 节点间 RPC、RaftAdmin 与 Migration 共用公共 `listen_addr` 端口。
跨节点聚合订阅的 `InternalSubscription` 只监听 `internal_listen_addr` 专用端口，
该端口必须由网络策略限制为仅集群节点可访问，不能与客户端 API 共用。
节点间通过 gRPC 交换 Raft 消息（Vote / AppendEntries / InstallSnapshot），
每条消息携带 `shard_id` 用于路由到对应的 Raft 实例。

**Stream 归属**由 `StreamOwnership` module 管理：未知 Stream 经控制 Shard Raft 提交，
已知 Stream 读取 `{data_dir}/routes.json` 兼容投影并携带 generation 写入；Migration
服务承载投影同步、归属提交与在线迁移原语（见 §7.5）。

**存储布局**：每 shard 一个独立 surrealkv LSM tree（`{data_dir}/shard-{id}/`，
独立 LOCK），取代旧「全分片共享单 tree + key 前缀隔离」。memtable arena 调小
（`[storage] memtable_arena_bytes`，默认 4MiB，surrealkv 默认 100MB 打开即预分配）。

**优雅退出**：Ctrl-C / SIGTERM → 停 watcher → 逐 shard 先 `raft.shutdown()`
再 `storage.close()`（flush WAL + 释放 surrealkv LOCK，顺序不可颠倒——先停
Raft 避免关闭期间后台任务还在写；仅靠 drop 不保证释放锁，不显式关闭则重启
同目录会报 "already locked"）。

### 3.1 crate 划分

| crate | 职责 | 依赖 |
|---|---|---|
| `es-proto` | protobuf 定义与 tonic-build 生成代码 | tonic, prost |
| `es-core` | 领域模型、HLC、流路由表（RouteTable）、错误类型 | serde |
| `es-storage` | surrealkv 封装、key 编码、两个 openraft 存储 trait 实现（每 shard 一个 `EsStorage`） | es-core, openraft, surrealkv |
| `es-raft` | TypeConfig、ShardManager、gRPC RaftNetwork | es-core, es-storage, es-proto |
| `es-server` | gRPC 服务实现、配置（含放置表校验）、路由表管理 + watcher 热更新、进程入口 | 全部 |
| `es-client` | 客户端 SDK、leader 重定向重试 | es-core, es-proto |

分层依赖单向向下，`es-client` 不依赖 `es-raft`/`es-storage`，因此可独立发布而不拖入共识与存储依赖。

## 4. Key 编码方案

所有整数一律 8 字节 big-endian（记作 `BE8`），保证字节序等于数值序。
首字节为命名空间 tag，避免不同类别 key 前缀互相包含。

```
Raft 日志区（每分片独立）
  [0x01][shard:BE8][0x01][index:BE8]        -> 序列化的 Entry
  [0x01][shard:BE8][0x02]                   -> vote
  [0x01][shard:BE8][0x03]                   -> last_purged_log_id
  [0x01][shard:BE8][0x04]                   -> committed log id

状态机区（每分片独立）
  [0x02][shard:BE8][0x01][slen:BE8][stream][version:BE8]  -> 事件载荷
  [0x02][shard:BE8][0x02][slen:BE8][stream]               -> StreamMeta（当前版本）
  [0x02][shard:BE8][0x03][position:BE8]                   -> (stream, version) 指针
  [0x02][shard:BE8][0x04]                                 -> 已应用状态（last_applied + membership）
  [0x02][shard:BE8][0x05][event_id:16B]                   -> 幂等索引，值为 (stream, version)
  [0x02][shard:BE8][0x06]                                 -> next_position 计数器

快照区（已废弃：快照改为独立文件，见 docs/snapshot.md）
  [0x03][shard:BE8][0x01]                   -> 旧版快照 key，启动时检测即删（仅升级清理用）
```

**key 编码在 per-shard tree 布局下不变**：存储布局已改为每 shard 一个独立 LSM
tree（§3），tree 内部其实只剩一个 shard 的数据，但 key 中的 `shard:BE8` 前缀
**仍然保留**——迁移工具（esctl migrate）按显式 shard 读写（`AppendMigrated` /
`ReadStreamFromShard` / `DeleteStreamFromShard` 等原语把 shard 写进 key），
带前缀保证同一套编码同时适用于任意布局（迁移期间源/目标分片并存），
也保留未来合并回共享 tree 的兼容性。

### 4.1 为什么 stream_id 前面要加长度前缀

若直接拼接 `[stream][version:BE8]`，两个 stream 名存在前缀包含关系时（例如 `"a"` 与 `"a\x00..."`），
范围扫描会串流。加上 `slen:BE8` 长度前缀后，不同长度的 stream 落在不同前缀段，
扫描 `[0x02][shard][0x01][slen][stream]` 得到的必定只属于该 stream。

扫描某 stream 的 `[from, to)` 版本区间：

```
start = [0x02][shard][0x01][slen][stream][from:BE8]
end   = [0x02][shard][0x01][slen][stream][to:BE8]
```

### 4.2 全流扫描的上界：必须用前缀后继，不能用 u64::MAX

`range()` 是左闭右开。要扫某 stream 的全部版本，若取
`end = [前缀][u64::MAX:BE8]`，会恰好**漏掉 version 等于 `u64::MAX` 的那条事件**。
虽然实践中难以触及，但这属于静默错误，必须在编码层封死。

正确做法是对前缀取字节序后继（prefix successor）：

```
P = [0x02][shard:BE8][0x01][slen:BE8][stream]     // 该 stream 的公共前缀
start = P || [0u8; 8]                              // 版本 0
end   = successor(P)                               // P 的字节序后继
```

`successor` 从末字节向前找第一个不等于 `0xFF` 的字节，加一并截断其后所有字节；
若全为 `0xFF` 则返回 `None`，表示上界无穷（扫到末尾）。
因 `slen` 是定宽且位于 `stream` 之前，不同 stream 的前缀段天然隔离，
后继不会越界到其它 stream。

同一算法用于 Raft 日志按 index 区间扫描与分片 `$all` 位置扫描。

## 5. 数据模型与写入路径

### 5.1 事件结构

```rust
pub struct Event {
    pub stream_id: String,      // 聚合根标识
    pub version: u64,           // 流内版本，从 0 起单调递增
    pub event_id: Uuid,         // 全局唯一，用于幂等去重
    pub event_type: String,     // 事件类型名
    pub data: Vec<u8>,          // 业务载荷，存储层不解释
    pub metadata: Vec<u8>,      // 元数据，存储层不解释
    pub hlc: Hlc,               // leader 分配的混合逻辑时钟
    pub position: u64,          // 分片内提交位置，分片内单调
}
```

### 5.2 乐观并发控制

`ExpectedVersion` 四种取值：

| 取值 | 语义 |
|---|---|
| `Any` | 不校验，直接追加 |
| `NoStream` | 要求流不存在 |
| `StreamExists` | 要求流已存在 |
| `Exact(v)` | 要求当前版本恰为 `v` |

**校验必须在状态机 `apply` 内完成，不能客户端先读后写。** 这是正确性要求，不是优化：
`apply` 是单个 Raft group 内的串行执行点，只有在这里做「读当前版本 → 比对 → 写入」
才是原子的。若放在客户端或 leader 提交前，两个并发请求可能都通过校验，
随后都被提交，破坏乐观并发保证。

### 5.3 写入流程

```
客户端 Append(stream_id, expected_version, events)
  │
  ├─ es-client：连接任一节点（客户端不做分片路由）；服务端按路由表解析归属
  │             （未知名流 = 隐式建流并分配，见 §6），非 leader/非承载节点
  │             返回 Unavailable + leader_addr 由客户端重定向
  │
  ├─ leader 节点：分配 HLC，构造 EsRequest::Append
  │
  ├─ Raft 复制到多数派
  │
  └─ 各节点 apply（串行）：
       ├─ 读 StreamMeta 取当前版本
       ├─ 校验 expected_version，不符则返回冲突（不写入）
       ├─ 检查 event_id 幂等索引，已存在则跳过并返回原结果
       ├─ 逐条写事件、递增 version
       ├─ 读 next_position 计数器，逐条分配 position 并写指针
       ├─ 更新 StreamMeta、next_position 与 last_applied
       └─ commit 事务（以上全部在同一个 surrealkv 事务内）
```

**整个 apply 必须在单个 surrealkv 事务内提交。** 事件、StreamMeta、position 指针、
next_position、幂等索引、last_applied 六者若分多次提交，进程在中途崩溃会留下
不一致状态（例如事件已写但 StreamMeta 未更新，重启后版本号回退并覆盖已有事件）。
单事务提交使「已应用」与「状态变更」原子化，重启后 `applied_state` 读到的
last_applied 必然与实际数据一致，openraft 会从该点之后重放，不会重复也不会遗漏。

这同时决定了 openraft 两种持久化策略中我们选前者：**在 `apply` 内持久化状态**，
因此快照不要求落盘即可保证正确性。

幂等去重的必要性：客户端重试（网络超时但实际已提交）会导致重复追加。
以 `event_id` 建索引，重放时直接返回原结果，使 Append 在重试下语义幂等。

### 5.4 HLC 混合逻辑时钟

```rust
pub struct Hlc {
    pub wall: u64,     // 物理时间，毫秒
    pub logical: u32,  // 同毫秒内的逻辑计数
}
```

规则：取 `max(本地物理时钟, 上次 wall)`；若与上次 wall 相同则 `logical += 1`，否则 `logical = 0`。
保证单节点内严格单调。HLC 由 leader 在写入前分配并随事件落盘，
使日后要加跨分片近似全序时无需数据迁移。

### 5.5 全局顺序的明确边界

按已确认方案，`$all` **不提供跨分片严格全序**。服务端提供：

- 分片内严格有序：按 `position` 递增，与 Raft 提交顺序一致
- 跨分片：`ReadAll` 按 **HLC 归并**（每路内部保持 position 序；shard_id/position 仅作并列定序），HLC 随事件暴露供消费者按需排序

消费者（含 `eventfs-fuse`）需接受跨分片存在有界乱序。
per-stream 顺序始终严格，因为单 stream 恒在单一分片内。

## 6. Stream 归属（显式分配）

流 → 分片的归属**不再由 `hash(stream_id) % shard_count` 推导**，而是服务端显式
分配并记录在**归属权威**中。首次部署选择编号最小的现有 Shard，并把控制 Shard ID
持久化到 `{data_dir}/ownership-control.json`；其 Raft 状态机
线性化提交首次归属和迁移切换；路由表是面向快读与旧工具的兼容投影：

```rust
pub struct RouteTable {
    pub version: u64,                            // 集群级单调版本号，每次变更 +1
    pub streams: BTreeMap<String, u64>,          // stream → shard 兼容投影
    pub shard_stream_counts: BTreeMap<u64, u64>, // per-shard 流计数（分配用，允许漂移）
    pub stream_generations: BTreeMap<String, u64>, // 迁移 fencing 代次
    pub stream_revisions: BTreeMap<String, u64>,   // 单 Stream 最后变更 revision
}
```

持久化形态为 `{data_dir}/routes.json`（es-server 与 es-ctl 共用 `es_core::route::RouteTable`）。

### 6.1 分配策略

- 创建流（`EventStore.CreateStream`）与隐式建流（写未知名流，见 6.3）都由
  `StreamOwnership::for_append` 提交控制 Shard：从放置表全部分片中选
  **计数最少的 shard**，并列取最小 shard_id（「大致最少流」即可，需求确认流持续
  生产、不要求精确均衡）。
- 已存在的流返回现有归属（写锁内双检查），不重复分配、不 bump 版本。
- 计数允许漂移（删除/迁移不精确扣减），由 `Migration.RecountStreams` 校准
  （`esctl route recount`）；计数是派生数据，校准不推进归属 revision。
- 分配成功后权威 revision +1，再发布 `routes.json` 投影并整表广播到 peers；无法确认
  控制 Shard quorum 时拒绝首次写入，不回退到本地分配。

### 6.2 跨节点收敛

- **Raft 是裁决点**：同一 Stream 的并发首次归属只由控制 Shard 状态机创建一次。
- **整表广播是刷新通知**：`PushRouteTable` 的 payload 不会直接安装，接收节点必须
  回控制 Shard 取得权威投影；写入仍携带 generation。广播/拉取带 2s 超时。
- **启动加载**：本地文件与 peers 取**版本最高**的表（本地缺失时向 peers 拉取；本地存在时也比对——节点离线期间集群路由表可能已前进）
  （`GetRouteTable`，首个成功者）。
- **本地文件调和**：watcher 接受内容相同的 no-op；新增、删除或改写归属会被拒绝，
  并用权威内存投影恢复文件。文件事件丢失由周期指纹检查补偿。

### 6.3 读写路径语义

- **写**（Append）：投影命中走本地快路径；未命中 = **隐式建流**，经控制 Shard
  Raft 提交。每次写携带归属 generation；遇到过期 fencing 时刷新权威归属并重试一次。
  写请求打到未承载该 shard 的节点时，
  返回与本地 ForwardToLeader 同格式的 leader 重定向提示。
- **读**（ReadStream / GetStreamMeta）：**只查不分配**（读无副作用）。未知流
  （从未创建或路由表缺失）→ ReadStream 返回 **NotFound**；GetStreamMeta 返回
  `exists=false`（不报错，退出码 0）——显式分配语义：流必须先创建
  （CreateStream 或首次写入）才能读。
- `event_id` 必须是合法 **16 字节 UUID**，否则 Append/CreateStream 相关写入返回 `InvalidArgument`（静默替换会破坏幂等去重）。
- 控制 Shard Raft 状态机是归属的**唯一权威**。es-core 仍保留 `hash(stream_id) % count` 的
  `route()` 函数，但仅作 esctl 的**客户端预选提示**（选起始探测端点、
  预显示分片，以服务端落盘为准），不参与数据寻址。

### 6.4 孤儿流检测（esctl route check）

对比各分片实际存储的流（`Migration.ListStreams`，按显式 shard 枚举）与路由表：

- **孤儿**：存储中有但路由表无记录——隐式建流跨节点竞态等残留，
  可用 `esctl migrate --stream <s> --to <shard>` 合并修复。
- **虚挂**：路由表指向的分片与存储实际所在不一致——迁移切换后未收敛或
  路由表手工编辑出错，指向的写入会 NotFound。

### 6.5 扩容（运行期动态加 shards）

加节点 = 更新**所有节点**配置（新增节点 + 新增 shards 行）→ 各节点 watcher
热加载配置（fail-soft：解析/校验失败保留旧配置、服务不受影响）→ diff 出新增的
本地 shards → 逐个 `factory::create_shard`（与启动共用单一代码路径）→ 注册 →
单分片自举（`bootstrap_new_shard`，幂等）。配置中移除的 shard 仅告警（提示
`route check` 检测残留流），数据目录保留，重新加入时幂等打开恢复。**全程无需重启**。

实现细节（评审修复）：
- 热更新按**实际节点 id** 计算 local_shards（`--node-id` 覆盖启动时不用文件里的
  node.id，避免把别的节点的分片创建/自举到本节点形成幽灵 raft group）
- **分配池延迟到全部新 shards 创建成功后才更新**——避免扩容窗口内新流被
  分配到尚未就绪的 shard（append 重定向失败且流永久钉在新建分片上）
- watcher 事件**前置过滤**（FSEvents 目录 watch 是递归的，surrealkv 写活动
  产生事件风暴会挤掉配置变更事件）
- watcher 同时每 500ms 比较目标文件内容指纹，补偿文件系统通知丢失；运行时不得
  从放置表移除持久化控制 Shard；加入更小编号 Shard不会在重启后改变控制 Shard

## 7. gRPC 接口

### 7.1 客户端 API

```protobuf
service EventStore {
  rpc Append(AppendRequest) returns (AppendResponse);
  rpc ReadStream(ReadStreamRequest) returns (stream ReadEventsResponse);
  rpc ReadAll(ReadAllRequest) returns (stream ReadEventsResponse);
  rpc Subscribe(SubscribeRequest) returns (stream SubscribeResponse);
  rpc GetStreamMeta(GetStreamMetaRequest) returns (GetStreamMetaResponse);
  rpc CreateStream(CreateStreamRequest) returns (CreateStreamResponse);  // 显式创建流
}
```

`CreateStream`：服务端分配 shard（大致最少流，§6.1）并记录路由表；
幂等——流已存在时返回现有归属（`exists=true`，不重复分配）；
响应携带目标 shard 与 leader 地址（尽力探测，未知返回空串，调用方经常规
重定向路径定位亦可）。

写请求打到非 leader 时，返回 `Unavailable`（gRPC 可重试语义），message 中携带
`leader_addr=...`，由客户端重定向重试。服务端不做透明转发，避免持有到其它节点的
业务连接而扩大故障域。`es-client`（append）与 `es-ctl`（with_leader）均已内置
重定向重试，解析与预算制重试队列下沉为 `es-core::redirect` 共用；直接使用
gRPC 客户端的调用方需自行解析重定向。

### 7.2 节点间 Raft API

```protobuf
service RaftRpc {
  rpc AppendEntries(RaftAppendEntriesRequest) returns (RaftAppendEntriesResponse);
  rpc Vote(RaftVoteRequest) returns (RaftVoteResponse);
  rpc InstallSnapshot(RaftInstallSnapshotRequest) returns (RaftInstallSnapshotResponse);
}

message RaftAppendEntriesRequest {
  uint64 shard_id = 1;   // 路由到本节点对应的 Raft 实例
  bytes payload = 2;     // bincode 序列化的 openraft 消息
}
```

**设计取舍**：`payload` 用序列化字节而非逐字段映射 protobuf。理由是 openraft 的
`VoteRequest`/`AppendEntriesRequest` 等类型含大量泛型与内部结构，手工映射到 protobuf
会在 openraft 升级时全面失效，且极易出现字段遗漏这类静默错误。openraft 官方 gRPC 示例
同样采用这一做法。代价是线上抓包不能直接读出字段内容，需借工具解码。
这不影响功能完整性，仅影响可观测性。

> 注：早期 `proto/raft.proto` 中的 `RaftInternal`（同样载荷方案）已被上述
> `RaftRpc` 取代且未注册到服务端，属废弃定义，待清理。

### 7.3 集群组建与集群管理 API

```protobuf
service RaftAdmin {
  rpc Initialize(InitializeRequest) returns (InitializeResponse);          // 初始化集群
  rpc AddLearner(AddLearnerRequest) returns (AddLearnerResponse);          // 学习者追平数据
  rpc ChangeMembership(ChangeMembershipRequest) returns (ChangeMembershipResponse);
  rpc GetRaftState(GetRaftStateRequest) returns (GetRaftStateResponse);
  rpc ListShards(ListShardsRequest) returns (ListShardsResponse);          // 本节点承载的分片
}
```

`ListShards`：返回本节点已注册（承载）的分片 ID 集合。esctl 的分片探测用它：
逐端点 `ListShards` 取**并集**即集群全部分片集（节点只承载放置表分配的部分
分片，旧「按 0,1,2,… 连续扫描 GetRaftState、首个 NotFound 即边界」的方案在
部分承载布局下会错误地探测到 0 个分片）。

组建集群有两条路径，**每个分片各组建一次**（分片间不共享 membership 与 leader）。
**每个 shard 的 membership = 放置表中承载它的节点子集，而非全量 peers**
（`Config::shard_members` 计算；未承载该 shard 的节点不注册它，向它复制日志
会得到 NotFound）——所有节点对同一 shard 的计算结果一致，是 bootstrap
membership 与双集群防护的基础：

**自动组建（推荐，etcd 静态引导语义）**：`node.peers` 非空即触发（见 `es-server/src/bootstrap.rs`）。
日志为空的节点按分片：随机延迟 0~2s（formation delay）→ 探测所有 peer 的
`GetRaftState` 判定是否已有集群 → 无则用完整 peers 调用 `initialize` 一步到位，
有则放弃自举等日志复制加入。并发 `initialize` 因同配置而日志内容一致，openraft
日志仲裁收敛（官方明确"同配置并发 initialize 安全"）；多节点同时竞选靠
600~900ms 随机化选举超时收敛（已在多进程测试实测）。已有日志的节点（重启）
跳过组建，从本地日志恢复。组建不成功不阻塞服务，可经 RaftAdmin 手动接管。

**手动组建（peers 为空时）**：先 `Initialize(shard_id, [self])` 单成员自举（立刻
成为 leader），再 `AddLearner` 加入其余节点，追平后 `ChangeMembership` 一次性
提升为投票成员。直接 `Initialize([1,2,3])` 同样收敛（随机化选举超时），单成员
自举保证全程有 leader，便于排障。

**双集群防护**：同一配置下并发 initialize 的 membership 日志内容一致，raft 仲裁
收敛，不会形成双集群；不同配置的 split brain 运行时不可解——探测到已初始化
peer 时对比其 voter_ids 与本节点配置，不一致即告警，且要求全节点配置完全一致
（与 etcd 相同约束）。

### 7.4 订阅实现

公共订阅面向 stream 集合而非 shard：请求指定一个或多个 stream，或 `$all`。服务端按
路由表将目标 stream 分组到内部 shard。本机仅在自己是 leader 时直接读取；本机 follower
和远程 shard 均通过仅节点间可用的 `InternalSubscription` RPC 向 leader 转发，公开事件
不含 shard ID 或分片 position。该 RPC 仅监听 `internal_listen_addr` 专用端口，必须由网络
策略限制为集群节点可访问。

每个内部来源均按 catch-up 与 live 两阶段执行：

1. 从请求起始位置扫描存储，推送历史事件
2. 追平后切到 `tokio::sync::broadcast`（每分片一个通道）接收实时事件

切换点需处理边界：先订阅 broadcast 再做最后一段扫描，避免两阶段之间漏事件。
聚合器不承诺跨 stream 顺序，但保留每个 stream 内的 version 顺序。全部内部来源追平后才
发送一次 `caught_up`。来源广播落后、连接失败或关闭时，聚合器发送不含内部细节的
`degraded` 状态并继续健康来源；`--once` 场景以退出码 1 报错。

### 7.5 路由表同步与在线迁移 API（Migration 服务，节点间）

承载路由表同步与在线迁移原语，不暴露给客户端：

| RPC | 作用 |
|---|---|
| `GetRouteTable` / `PushRouteTable` | 读取兼容投影/通知接收方回控制 Shard刷新；payload 不参与归属裁决 |
| `SetStreamShard` | 携带调用方观察到的 Shard + generation + operation ID，条件切换归属 |
| `OwnershipInternal.CommitOwnership` / `InstallOwnershipFence` | 仅内部 listener：控制 Shard 提交与数据 Shard fencing |
| `RecountStreams` | 校准本地 per-shard 派生计数，不改变归属 revision |
| `AppendMigrated` | 迁移复制写入：单事件一条 raft 日志、**保留源 HLC**、按显式 shard 寻址（不走路由表、不隐式建流） |
| `DeleteStreamFromShard` | 迁移清尾：删除源 shard 上的流（幂等 no-op） |
| `ReadStreamFromShard` | 显式 shard 读流（迁移排水/校验用；路由表已切换时仍可读源） |
| `GetStreamMetaFromShard` | 显式 shard 读流元数据（排水收敛判据） |
| `ListStreams` | 列出 shard 上的全部流（迁移枚举 / route check 孤儿检测） |

**迁移原语按显式 shard 寻址，不走路由表**——迁移切换后路由表已指向目标，
排水阶段必须仍能读源。写入原语在目标 shard leader 上执行（非 leader 返回
标准重定向提示），读取原语走本地存储（无需 leader）。在线迁移状态机与
用法见 [docs/migrate.md](migrate.md)。

## 8. 配置

```toml
[node]
id = 1
listen_addr = "127.0.0.1:50051"   # 四个 gRPC 服务共用一个端口
internal_listen_addr = "127.0.0.1:51051" # 节点间订阅与归属控制；多节点必填

# 集群节点列表（3 节点示例；非空即触发启动时自动组建，见 7.3）
# 必须包含本节点，且所有节点配置完全一致；addr 可省略 http:// 前缀
[[node.peers]]
id = 1
addr = "127.0.0.1:50051"
internal_addr = "127.0.0.1:51051"
[[node.peers]]
id = 2
addr = "127.0.0.1:50052"
internal_addr = "127.0.0.1:51052"

[storage]
data_dir = "./data/node1"         # 每节点独立
memtable_arena_bytes = 4194304    # 每 shard 的 LSM memtable arena（默认 4MiB，
                                  # 范围 [1MiB, 16MiB]；surrealkv 默认 100MB 打开即预分配）

# 分片放置表（替代旧 [shards] num_shards；完整示例见 config.example.toml）
# - 每个 shard 一个 raft group，成员 = 放置表中承载它的全部节点
# - replication_factor：每 shard 投票成员数（primary + replica 合计），默认 1
# - primary 是管理偏好标签（leader 仍由 raft 选举产生）
# - 不变式（启动校验强制）：primary 分区互斥、每 shard 承载数 == rf
# - 配置变更（加节点/加 shards）由 watcher 热加载后运行期生效，无需重启（见 6.5）
[placement]
replication_factor = 2

[[placement.nodes]]
id = 1
primary = [0, 1]
replica = [2, 3]
```

客户端、节点间、管理、迁移四类 gRPC 服务共用 `listen_addr` 单端口（`server.rs`
同一 `Server` 注册四个 service）。`node.peers` 在启动时由 `bootstrap` 消费：
地址 normalize（补 `http://`）后随 `initialize` 写入 membership 的 `BasicNode.addr`，
与网络层回连规则同源（`es_raft::normalize_endpoint`）。流路由表持久化为
`{data_dir}/routes.json`（§6），由 watcher 热更新；配置文件本身同样被 watcher
监听（§6.5 动态扩容）。

## 9. 测试策略

覆盖率目标：行覆盖 80%，分支覆盖 80%，用 `cargo-llvm-cov` 度量。

| 层次 | 内容 |
|---|---|
| 单元测试 | key 编码与排序性质、归属命令的幂等/冲突/非法输入、归属 interface 错误映射、HLC 单调性、`ExpectedVersion` 校验矩阵 |
| 存储层测试 | RaftLogStorage 语义、归属 catalog 持久化、缺失/过期 generation fencing、apply 幂等性、快照往返 |
| 端到端测试 | 3 节点真实集群：并发首次归属、控制 Shard 无 quorum 拒写与恢复、读写一致性、leader 故障、订阅、在线迁移 |
| 模糊测试 | 归属 Ensure 序列唯一性与投影计数；随机 Append/DeleteStream 序列的版本连续、per-stream 严格有序不变量 |

key 编码的排序性质测试是重点：第 2.1 节的大端约束若被破坏，
错误表现为范围扫描静默返回错误数据，而非崩溃，只有排序性质断言能捕获。

## 10. 本期不实现

| 项 | 原因 |
|---|---|
| 在线分裂/合并 | 单流在线迁移已实现（`esctl migrate`，见 `docs/migrate.md`）；离线 reshard 已下线，region 式分片拆分/合并未实现，需架构级改动 |
| 跨分片严格全序 | 已确认取分片内有序 + HLC |
| 多数据中心 | 未列入需求 |
