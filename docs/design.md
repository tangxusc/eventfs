# EventFS Aggregate-only 设计

## 1. 范围

EventFS 是分布式 AggregateStore。公共数据面只包含聚合类型 catalog、事件追加、类型级
事件 feed、状态文档和聚合消费者组。它不提供通用命名事件序列、实例级历史回放、全局
事件顺序或旧协议兼容。

聚合实例的唯一身份是：

```text
(business_space, aggregate_type, aggregate_id)
```

`business_space`、`aggregate_type`、`aggregate_id` 和组名遵循受限 ASCII 标识符规则，
避免路径歧义和跨语言规范化差异。

## 2. 不变量

1. `AggregateType` 由 `(business_space, aggregate_type)` 标识，注册后固定为 256 个虚拟分区。
2. 同一 `aggregate_id` 稳定映射到同一虚拟分区；分区和 Shard 放置对普通调用方隐藏。
3. 同一聚合实例的 `aggregate_version` 从 0 开始严格递增。
4. `AppendAggregateEvent` 每次只追加一条事件；期望版本在同一 Raft apply 中校验。
5. `event_id` 是幂等键：相同请求返回原结果，不同内容复用同一 ID 必须冲突。
6. 类型级 feed 保持每个实例内的版本顺序，不承诺不同实例或不同 Shard 的全序。
7. feed cursor、状态 page token 和 delivery ID 都是不透明凭据，调用方只能原样续传。
8. 状态文档与事件并存；状态 revision 独立于 aggregate version，并以 CAS 更新。
9. 消费者组只有连续已结算进度可以提交；同一实例同一时刻最多租给一个消费成员。
10. 每个 Shard 是独立 Raft group 和存储树；状态机命令只包含 Aggregate 领域命令。

## 3. 组件边界

```text
esctl / es-client / eventfs-fuse
              |
              v
       AggregateStore (public)
              |
       Aggregate RPC service
          /           \
 control catalog    data shards
          \           /
          Raft manager
              |
        es-storage state machine
```

| 组件 | 职责 |
|---|---|
| `es-core` | 聚合类型、事件、状态、消费者组、HLC 与错误模型 |
| `es-storage` | 稳定 key 编码、Raft 日志、状态机 apply、快照内容 |
| `es-raft` | 每 Shard 的复制、选举、成员变更与 snapshot transport |
| `es-server` | gRPC 验证、路由到本地/远端 Shard、错误映射和事件通知 |
| `es-client` | 节点候选、leader hint、幂等重试和类型级流重连 |
| `esctl` | AggregateStore 与集群管理命令 |
| `eventfs-fuse` | 文件语义到 AggregateStore 的无状态适配 |

公共 listener 注册 `AggregateStore`、`RaftRpc`、`RaftAdmin`。内部 listener 仅注册
`AggregateStoreInternal`，用于 catalog 提交、partition fence、分区读取和消费者组分区
操作。`rpc_support` 统一承担远程 Shard 定位、Raft 错误映射和 `RuntimeTopology`，避免把
这些职责绑定到某个领域服务。运行期拓扑把可放置 Shard 集合与远端节点定位器作为一个
原子快照发布；一次 AggregateStore 操作只观察一个快照。

## 4. 聚合类型注册

`RegisterAggregateType` 向控制 Shard 提交带 `operation_id` 的 catalog 命令。控制状态机
持久化随机 hash seed、256 个分区放置和 catalog revision。服务端为每个分区安装初始
generation fence；全部完成后类型从 `REGISTERING` 变为 `ACTIVE`。

注册具有操作级幂等性。客户端遇到超时必须复用原 16 字节 UUID；相同 operation ID 不能
表示不同注册请求。只有 `ACTIVE` 类型可接受事件、状态和消费者组操作。

## 5. 事件写入

```text
AppendAggregateEvent
  -> 校验 AggregateType / aggregate_id / UUID / payload
  -> hash(seed, aggregate_id) % 256
  -> 获取 partition placement 与 generation
  -> 定位 Shard leader
  -> Raft client_write(AggregateAppend)
  -> 状态机原子校验 OCC 与 event_id
  -> 写事件、实例元数据、分区位置和幂等记录
  -> 返回 aggregate_version
```

期望版本：

| 条件 | 含义 |
|---|---|
| `Any` | 不检查当前版本 |
| `NoAggregate` | 实例必须没有事件 |
| `AggregateExists` | 实例必须已有事件 |
| `Exact(n)` | 当前版本必须等于 `n` |

事件内部记录 `partition_id`、`partition_position` 和 HLC，但公共事件只暴露业务所需的
`aggregate_id`、`aggregate_version`、事件内容和 HLC。物理位置不构成生产者接口。

## 6. 类型级事件 Feed

`FollowAggregateTypeEvents` 支持 `Beginning`、`Now` 和 opaque cursor。服务端按当前
placement 将 256 个虚拟分区分组到 Shard，通过 `AggregateStoreInternal` 获取各组事件，
再合并为一个持续 gRPC response stream。

合并器只维护分区游标，不构造跨分区全序。对任意单个聚合实例，hash 稳定性确保其事件只
来自一个分区，因此 `aggregate_version` 顺序不变。全部来源追平后发送 `caught_up`；部分
来源暂不可用时发送 `degraded` 并重试，恢复时发送 `recovered`。每个 frame 的 cursor 表示
消费该 frame 之后的续读位置。

该接口不是实例历史读取 API。调用方不能用 `aggregate_id` 请求服务端回放完整事件历史；
需要实例投影时应持续消费类型级 feed 或读取状态文档。

## 7. 状态文档

状态 key 由聚合类型、虚拟分区和 `aggregate_id` 隔离。`PutAggregateState` 必须携带
`Absent` 或 `Exact(revision)`；状态机在一次事务中校验 CAS、覆盖正文、递增 revision，
并保存同一次提交的 HLC。`ListAggregateStates` 对 256 个分区执行稳定归并，按
`aggregate_id` 字节序分页；page token 绑定聚合类型和每个分区的排他起点。

任一数据源不可用时列表请求整体失败，防止调用方把不完整列表误认为完整事实。

## 8. 聚合消费者组

消费者组 catalog 位于控制 Shard，定义包含聚合类型、组名、revision、epoch、起点和设置。
创建、更新和删除均带 operation ID；更新和删除还使用 expected revision CAS。reset 起点
会递增 epoch，使旧 delivery 立即失效。

Fetch 按 group/consumer 的额度从各分区领取候选事件。状态机记录 delivery 租约、实例租约、
重试、park 和连续进度。Settle 支持：

| 动作 | 结果 |
|---|---|
| Ack | 标记成功并尝试推进连续进度 |
| Retry | 延迟后重新投递，受最大重试和退避约束 |
| Park | 放入 parked 集合并允许进度越过 |
| Skip | 明确跳过并允许进度越过 |

Renew 只延长仍由相同 consumer 持有且 epoch 未变化的 delivery。Settle/Renew 逐项返回
Applied、AlreadySettled、StaleLease 或 WrongConsumer，调用方不能根据 RPC 成功推断每一项
都已应用。

## 9. 存储与 Raft

key 编码使用长度前缀和大端整数，确保业务空间、聚合类型、分区、实例和位置之间无前缀
碰撞，并维持范围扫描排序。状态机仅接受以下领域族：

- Aggregate append、partition fence；
- AggregateType catalog；
- Aggregate state CAS；
- AggregateGroup catalog 与分区消费进度。

除此之外保留 Raft membership、日志、快照和 HLC 支撑。旧通用事件命令、订阅、归属、
路由和迁移命令不在编码或 apply 分支中。由于 bincode Raft 命令布局已变化，本版本不读取
旧数据目录或旧 Raft 日志。

## 10. 服务发现与故障

写请求若命中非 leader，服务端返回 `Unavailable` 和 leader 地址。官方客户端对幂等操作
跟随 hint 并轮询候选节点；无法安全重放的操作不会盲目重试。Aggregate append 因稳定
`event_id` 可安全重试，catalog 操作因稳定 `operation_id` 可安全重试。

主要失败模式：

| 场景 | 行为 |
|---|---|
| OCC/CAS 不匹配 | `FailedPrecondition`，不写入 |
| 相同幂等 ID、不同内容 | 冲突，原结果不变 |
| partition generation 过期 | 拒绝并刷新 catalog |
| 部分 feed 来源不可用 | degraded frame，后台重试 |
| 状态枚举来源不可用 | 整体 `Unavailable` |
| delivery 租约过期或 epoch 变化 | 逐项返回 stale lease |
| 热更新新增本地 Shard 失败 | 不发布新拓扑，继续使用原 Shard/peer 快照 |
| 热更新移除 Shard 或变更控制 Shard | 拒绝更新，运行中不得缩容或切换 catalog |
| 旧数据目录 | 启动/反序列化不受支持；必须换新目录 |

## 11. FUSE 映射

FUSE 不建立第二套领域模型，只把 AggregateStore 映射为：

```text
/{business_space}/{aggregate_type}/events.jsonl
/{business_space}/{aggregate_type}/states
/{business_space}/{aggregate_type}/groups
```

`events.jsonl` 是非 seek 的持续读取和单条 envelope 写入；`states` 使用打开时 revision 做
CAS；`groups` 将 delivery 和 settlement 映射为 JSONL。读句柄名 `StreamingRead` 仅描述
Rust/tonic 流式传输，不表示被删除的领域概念。

## 12. 安全与兼容边界

- 公共和内部 listener 可启用 TLS；内部端口还必须通过防火墙或安全组限制来源。
- cursor、page token、delivery ID 和 bincode payload 都要验证版本、绑定身份和尾随字节。
- 旧客户端、旧 protobuf service、旧数据目录和旧 Raft 日志均不兼容。
- 不提供迁移 RPC、迁移 CLI 或离线数据转换工具。
