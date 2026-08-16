# EventFS FUSE 设计

状态：AggregateStore 核心数据面、`esctl aggregate` 和 Linux FUSE 适配已实现。本文描述
当前文件与 gRPC 契约；尚未实现的能力会明确标注，不作为可用接口。

当前实现包括 catalog、固定 256 个虚拟分区、实例级 OCC、状态 CAS、跨 Shard follow、
显式结算消费者组和真挂载 e2e。AggregateStore 分区数据迁移与自动再平衡尚未实现；
`eventfs-fuse` 只支持 Linux 前台挂载。

## 1. 目标与边界

`eventfs-fuse` 在 Linux FUSE3 上把聚合事件、业务状态文档和持久化消费者组映射为文件。它是新增 `AggregateStore` Interface 的 gRPC Adapter，不直接复用或改变现有 `EventStore` Stream 语义。

首期目标：

- 每种聚合根类型只暴露一个聚合类型事件集，聚合实例 ID 位于 JSON 内容或状态文件名中。
- 同一聚合实例严格按聚合版本做乐观并发，不同实例互不冲突且不承诺全序。
- 聚合类型事件集可横跨 Shard，避免单 Stream 单 leader 热点。
- 事件追加、状态 CAS、持久消费和显式结算都由服务端持久化语义直接支撑。
- FUSE daemon 无本地持久化权威状态，重挂载不需要恢复本地数据库。

首期不实现：

- macOS/macFUSE。
- 旧 `EventStore` Stream 的自动读取、双写或迁移。
- 事件 TTL、裁剪、归档和业务删除。
- 事件、状态和消费者结算之间的跨文件事务。
- 在线改变事件集的 256 个虚拟分区数量。
- AggregateStore 事件分区的数据迁移与自动再平衡。
- 跨聚合实例严格全序。

相关决策见 [ADR-0001](adr/0001-eventfs-fuse-may-extend-server-contract.md)、[ADR-0003](adr/0003-partition-aggregate-event-sets.md)、[ADR-0004](adr/0004-isolate-aggregate-store-from-event-store.md) 与 [ADR-0005](adr/0005-keep-eventfs-fuse-stateless.md)。

## 2. 用户可见路径

```text
/data/eventfs/{business_space}/{aggregate_type}/
├── events.jsonl
├── states/
│   └── {aggregate_id}.json
└── groups/
    └── {group_name}/
        └── {consumer_id}.jsonl
```

示例：

```text
/data/eventfs/orders/order/events.jsonl
/data/eventfs/orders/order/states/order-1.json
/data/eventfs/orders/order/groups/group-1/consumer-a.jsonl
```

`business_space`、`aggregate_type`、`aggregate_id`、`group_name` 与 `consumer_id` 首期均须匹配：

```text
[A-Za-z0-9][A-Za-z0-9._-]{0,127}
```

`events.jsonl`、`states`、`groups` 等保留名称不能用作业务标识符。路径不接受百分号转义、Unicode 归一化或嵌入斜杠。

## 3. 事件文件

### 3.1 写入

`events.jsonl` 的 `O_WRONLY` 打开表示追加一个事件；`O_TRUNC` 不删除历史，`>` 与 `>>` 都是追加命令。`O_RDWR` 返回 `EINVAL`。

```bash
printf '{
  "spec_version":"1.0",
  "aggregate_id":"order-1",
  "event_type":"create",
  "data":{"amount":100},
  "expected_version":{"kind":"no_aggregate"}
}' > /data/eventfs/orders/order/events.jsonl

printf '{
  "spec_version":"1.0",
  "aggregate_id":"order-1",
  "event_type":"pay",
  "data":{"amount":50},
  "expected_version":{"kind":"exact","version":0}
}' >> /data/eventfs/orders/order/events.jsonl
```

输入 envelope：

```json
{
  "spec_version": "1.0",
  "aggregate_id": "order-1",
  "event_type": "pay",
  "data": {"amount": 50},
  "event_id": "可选 UUID",
  "expected_version": {"kind": "exact", "version": 0},
  "metadata": {}
}
```

契约：

- `spec_version`、`aggregate_id`、`event_type` 与 `data` 必填。
- `expected_version.kind` 支持 `any`、`no_aggregate`、`exists` 与 `exact`；缺省整体字段时为 `any`。
- `event_id` 可选；FUSE 省略时生成 UUID，并在当前文件句柄的 RPC 重试中复用。
- `metadata` 可选，缺省 `{}`，存在时必须是 JSON 对象。
- 未知顶层字段、重复 key、非法 UUID、空 `event_type` 或多余 JSON 值返回 `EINVAL`。
- 一个可写文件句柄只接受一个完整 JSON 值，允许首尾空白和格式化换行。
- 首次成功 `fsync` 提交；未调用 `fsync` 时由 `flush/release` 尝试提交。重复调用不重复追加，提交后继续写返回 `EBUSY`。
- 单个 envelope 默认最多 1 MiB，配置可调，硬上限 6 MiB；超限返回 `EFBIG`。
- 跨新句柄重试若未复用调用方提供的 `event_id`，可能产生重复事件。

### 3.2 读取

`events.jsonl` 的每个新 `O_RDONLY` 句柄从 Beginning 追平，然后持续跟随；普通读取不持久化 checkpoint。流式句柄使用 `direct_io`、不可 seek、不可 mmap，并支持 `poll`。

输出统一为紧凑 JSONL frame：

```json
{"kind":"event","aggregate_id":"order-1","aggregate_version":0,"event_id":"...","event_type":"create","data":{"amount":100},"metadata":{}}
{"kind":"caught_up"}
{"kind":"degraded","unavailable_source_count":1,"retrying":true}
{"kind":"recovered"}
```

不同聚合实例之间不承诺输出顺序；同一 `aggregate_id` 严格按 `aggregate_version` 输出。部分事件分区不可用时可以继续输出其他分区，但必须发送 `degraded` frame；恢复后从不透明 cursor 补读，不能漏事件。每个流式读句柄最多缓冲 8 MiB，达到上限后暂停后端生产任务，读取消费后恢复；单个 frame 超过上限返回 `EFBIG`。

## 4. 业务状态文档

每个聚合实例至多一个业务状态文档：

```bash
printf '{"balance":50}' > /data/eventfs/orders/order/states/order-1.json
fsync /data/eventfs/orders/order/states/order-1.json
cat /data/eventfs/orders/order/states/order-1.json
```

契约：

- 聚合实例必须已有至少一个事件，否则创建状态返回 `ENOENT`。
- 打开时线性一致读取当前内容和 revision；句柄内内容不可变，新句柄读取最新 revision。
- 写句柄捕获打开时 revision；`fsync` 使用 `Absent` 或 `Exact(revision)` 做 CAS，冲突返回 `EAGAIN`。
- 状态内容是原始业务 JSON，不注入 revision 或存储字段。
- 状态默认最多 1 MiB，配置可调，硬上限 6 MiB。
- 首期不支持 `unlink`、`rename` 或一个实例多个命名状态文档。
- `states` 使用服务端分页、稳定排序的 `readdir`，每次最多向服务端取 256 项；目录句柄在 `releasedir` 时释放本次遍历保留的 inode，内核 lookup 引用则由 `forget` 独立释放。并发变更时不提供目录快照，但同一次遍历不能返回重复名称。

状态提交与事件追加互不原子。状态更新后、消费 Ack 前崩溃会导致事件重投，业务处理必须幂等。

## 5. 消费者组

创建默认消费者组：

```bash
mkdir /data/eventfs/orders/order/groups/group-1
```

`mkdir` 从 Beginning 创建目标为整个 `orders/order` 聚合类型事件集的组。Now 起点、配额、超时、重试与删除等高级管理使用 `esctl aggregate group`，不增加配置文件。

读取成员投递：

```bash
cat /data/eventfs/orders/order/groups/group-1/consumer-a.jsonl
```

同一路径 `O_WRONLY` 提交结算：

```bash
printf '{
  "settlements":[
    {"delivery_id":"opaque-1","action":"ack"},
    {"delivery_id":"opaque-2","action":"retry","reason":"timeout"}
  ]
}' > /data/eventfs/orders/order/groups/group-1/consumer-a.jsonl
```

契约：

- `O_RDONLY` 注册消费成员并输出 `kind:"delivery"` frame；`O_WRONLY` 提交 Ack、Retry、Park 或 Skip。
- 同一 `(事件集, group, consumer_id)` 只允许一个活跃读句柄，第二个返回 `EBUSY`；结算写句柄不受影响。
- 读句柄存活期间 FUSE 按服务端返回的最早 `deadline_ms` 自动 Renew 未确认租约；Renew 计时可抢占正在进行的 Fetch 长轮询。关闭不等于 Ack，关闭后租约到期可重投。
- 每组每个 `aggregate_id` 最多一条未结算投递；Retry 只阻塞该实例，不阻塞其他实例。
- 投递是 at-least-once；同一实例保持版本顺序，不同实例可以由不同成员并行处理。
- 结算请求格式错误时整批不提交；合法请求跨分区逐条提交并逐条返回结果，不承诺整批原子性。
- `delivery_id` 是可自路由的不透明 token，调用方不填写分区或 group epoch。

## 6. AggregateStore Interface

现有 `EventStore` Interface 和数据保持不变。新增 `AggregateStore` Interface：

```text
GetAggregateStoreCapabilities
CreateEventSet / ListEventSets / GetEventSet
AppendAggregateEvent / ReadAggregateEvents
ListAggregateStates / GetAggregateState / PutAggregateState
GetAggregateStoreStatus / ListAggregatePartitions
CreateAggregateGroup / UpdateAggregateGroup / DeleteAggregateGroup
GetAggregateGroup / ListAggregateGroups
FetchAggregateGroup / SettleAggregateGroup / RenewAggregateGroup
```

调用形态：

- capabilities、事件集、状态、管理和结算调用使用 unary RPC。
- `ReadAggregateEvents` 使用 server streaming，服务端隐藏跨分区 fan-out。
- `ListAggregateStates` 使用分页 unary。
- `FetchAggregateGroup` 使用 unary long polling，保持调用方背压。
  `PutAggregateState` 与 `FetchAggregateGroup` 在普通瞬时错误后不自动重放，只有
  响应携带明确 leader hint 时才重定向，避免模糊成功后的重复副作用。

FUSE 到自有 gRPC 是 remote-but-owned seam：生产使用 gRPC Adapter，契约测试使用内存 Adapter。路由与版本计算留在 Module Implementation；Raft 与 surrealkv 是 local-substitutable 依赖，通过进程内测试和真实存储测试验证，不向公共 Interface 暴露内部端口。

## 7. 事件分区与存储

每个聚合类型事件集创建时固定：

```text
partition_count = 256
partition = xxh3(event_set_seed || aggregate_id) % 256
```

`hash_algorithm_id`、随机 seed 与分区数由控制 Shard 持久化，客户端不能推导或覆盖。控制面保存 `(event_set, partition) → (shard, generation)`，不保存逐实例归属。

状态机 key 空间：

```text
aggregate_meta[event_set, partition, aggregate_id] -> current_aggregate_version
event[event_set, partition, aggregate_id, aggregate_version] -> event
partition_index[event_set, partition, partition_position] -> event locator
next_partition_position[event_set, partition] -> u64
state[event_set, partition, aggregate_id] -> revision + JSON
idempotency[event_set, partition, event_id] -> request digest + original result
```

不变量：

- 同一聚合实例始终路由到一个事件分区，在一次 Raft apply 中原子校验并递增聚合版本。
- 不同实例不共享乐观并发版本。
- 生产者不提交 `partition_position`；服务端提交时分配，只用于 cursor、group checkpoint 与迁移。
- 类型级 cursor 是版本化不透明 token，内部保存 256 个分区的 next position；调用方只能原样回传。
- 单个极热聚合实例仍受一个 Raft leader 的串行上限约束，基础设施不能在保留线性 OCC 的同时透明拆分它。

## 8. 生命周期与迁移边界

事件集创建采用 `Creating → Active` 生命周期。`mkdir` 通过稳定 operation ID 幂等创建定义和 256 个分区放置；全部目标 Shard 安装 fencing 后才对目录可见。失败重试复用 seed 和创建计划。

领域模型保存每个分区的 `shard_id`、`generation` 和可选 `pending_move`，并定义
`PrepareMove` / `CompleteMove` 的 generation CAS。当前服务端没有分区事件、状态和
消费者进度的数据复制编排，也没有对应的公共迁移或自动再平衡 RPC；`esctl aggregate`
只能查询 `partitions`。因此不得仅修改 catalog 放置来搬迁已有分区。

未来实现分区迁移时仍需满足：源目标不双写、旧 generation 被 fencing、cursor 不失效，
并同时迁移 group checkpoint、lease 和 progress。这些是后续实现约束，不是当前可调用
能力。

混合版本集群中，旧 `EventStore` 继续工作；只有全部承载节点声明兼容 `AggregateStore` v1 时才允许创建事件集或挂载 FUSE。

## 9. FUSE 行为

当前使用 `fuser 0.18.0` 的稳定 `Filesystem` Interface，不启用 `experimental`
或 `libfuse*` feature。同步回调把拥有型 Reply 和工作投递到 Tokio runtime，RPC 完成后
异步回复；真挂载测试覆盖 deferred reply、poll wakeup 与取消路径。

支持的用户可见操作：

```text
lookup getattr opendir readdir releasedir forget open read write flush fsync release mkdir poll create
```

不支持的操作：

```text
rename unlink link symlink mmap fallocate chmod chown xattr flock
```

不支持的操作返回 `EPERM` 或 `ENOTSUP`，不能假装成功。初始化阶段要求内核协商 `FUSE_ATOMIC_O_TRUNC`，保证 shell 的 `>` 不会先以独立 truncate 清空逻辑文件。流式文件 `size=0`、`direct_io`、nonseekable；状态文件返回真实 JSON 字节数。状态正文与服务端 HLC 修改时间在同一次 Raft apply 和存储事务中提交，`lookup/getattr` 每次从服务端刷新大小与 `mtime`；旧状态缺少独立时间 key 时回退 Unix epoch。inode 只保证单次挂载生命周期内稳定。

稳定 errno 映射：

| AggregateStore 错误 | errno |
|---|---:|
| InvalidArgument | `EINVAL` |
| NotFound | `ENOENT` |
| AlreadyExists | `EEXIST` |
| OptimisticConflict | `EAGAIN` |
| PayloadTooLarge | `EFBIG` |
| StaleLease / StaleCursor | `ESTALE` |
| PermissionDenied | `EACCES` |
| DeadlineExceeded | `ETIMEDOUT` |
| Unavailable | `EHOSTUNREACH` |
| ResourceBusy | `EBUSY` |
| Internal / Corruption | `EIO` |

只自动重试具有稳定幂等身份的写操作和携带不透明 cursor 的读操作；达到 deadline 后必须返回错误，不能无限阻塞。

## 10. 运行与安全

启动方式：

```bash
eventfs-fuse mount --config /etc/eventfs/fuse.toml /data/eventfs
```

当前只支持前台运行，建议由 systemd 管理；设计中的显式 `--daemonize` 尚未实现。挂载前必须完成 capabilities 协商；全部端点不可达或缺少必需能力时挂载失败。挂载后断网不自动卸载，读写按稳定 errno 返回操作错误。

配置样例见仓库根目录的 [`eventfs-fuse.example.toml`](../eventfs-fuse.example.toml)。配置使用严格 TOML，未知字段、TLS 冲突、空端点或超过 6 MiB 的本地上限都会在挂载前失败。

默认只有挂载用户可访问；`--allow-other` 必须显式开启，并把 fuser session ACL 切换为 `All`。整个挂载使用一套服务端 TLS 凭据，Unix UID/GID 只做本机访问控制，不映射成服务端主体。

写入在 `fsync`、`flush` 或 `release` 时提交。强制终止进程或卸载可能丢弃尚未触发
这些回调的缓冲，因此需要持久提交确认时应显式调用 `fsync`。

## 11. 管理与可观测性

`esctl` 当前提供：

```text
aggregate capabilities
aggregate create/list/get
aggregate append/follow
aggregate state list/get/put
aggregate group create/update/delete/list
aggregate group fetch/settle
aggregate status
aggregate partitions
```

服务端使用结构化 tracing。`GetAggregateStoreStatus` 当前返回 catalog revision，以及
事件集总数、Creating 数和 Active 数；`ListAggregatePartitions` 返回物理放置、
generation 和 pending move 信息。当前没有 Prometheus 指标，也不暴露 Append 延迟、
group lag、FUSE 句柄数或 RPC 重连次数。

## 12. 验证

测试分层：

- `es-core` 属性与模糊测试覆盖稳定 hash、实例级 OCC、消费者组结算顺序和 catalog 状态转换。
- `es-storage` 测试覆盖 key 排序、分区 fencing、状态 CAS、消费者进度、重开与快照。
- `es-server` e2e 覆盖 catalog、跨 Shard follow、状态、消费者组和内部 RPC。
- `eventfs-fuse` 公共契约测试覆盖路径、严格 JSON codec、写句柄状态机和错误分类。
- Linux FUSE3 真挂载 e2e 覆盖创建事件集、事件 `fsync`、`poll/read`、状态 CAS 和正常卸载。

Release workflow 在四个原生 runner 上运行默认 workspace 测试和 release 构建。Linux
真挂载需要 `/dev/fuse` 与 `fusermount3`，不属于普通 macOS 本地测试。

当前可重复验证命令：

```bash
# 任意平台：路径、codec、配置、句柄状态机与公共契约
cargo test -p eventfs-fuse --locked

# Linux 上默认命令还覆盖 fuser Adapter 与 GrpcBackend 操作；
# 以下 ignored 用例进一步执行真实挂载、事件 poll/read 与状态 fsync
cargo test -p eventfs-fuse --test mount_e2e_test -- --ignored --test-threads=1
```
