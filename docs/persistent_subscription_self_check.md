# 持久化拉取订阅设计自检

## 已确认目标

- 命名竞争消费者组，服务端持久化组定义、消费进度、租约、重试和 parked 状态。
- 支持显式 Streams 与 `$all`；恢复真值使用 `(stream_id, next_version)`，不把
  会随在线迁移变化的 shard position 暴露给客户端。
- 消费者通过 unary long-poll `Fetch` 主动申请额度，通过批量 `Settle` 对每条
  delivery 执行 Ack、Retry、Park 或 Skip，提供 at-least-once 语义。
- 背压同时受请求 `max_events`、`max_bytes`、每消费者未确认上限和每组未确认上限约束。
- 同一 stream 使用 lease 批量交给一个 consumer；其它 consumer 在 lease 释放前
  不得取得该 stream，连续 Ack 后才推进 checkpoint。

## 方案比较

| 方案 | Interface | Depth / Locality | 结论 |
|---|---|---|---|
| Kafka/RocketMQ 式逻辑分区分配 | Join/Rebalance/Fetch/Commit，客户端理解 lane | 数据读取局部性高，但迁移和 rebalance 复杂度泄漏给调用方 | 拒绝：现有公开契约刻意隐藏 shard/position |
| 服务端租约分派拉取 | CRUD + Fetch + Settle，delivery ID 不透明 | 背压、竞争、恢复和迁移隐藏在 module 内 | 采用 |
| 客户端续订 token | Subscribe + token | interface 小，但服务端不管理消费组、租约和失败 | 拒绝：不满足持久化订阅目标 |

## Module 与 seam

`PersistentSubscriptions` 是 deep module。External interface 只有组管理、Fetch、
Settle、parked 查询/重放；Raft 命令、分片扫描提示、归属 generation、租约时钟和
跨节点读取均隐藏在 implementation 内。

状态提交依赖控制 Shard，是 remote-but-owned 依赖。纯领域状态
`es_core::PersistentGroup` 不执行 I/O，既由 control Shard Raft adapter 调用，也由
proptest 直接驱动随机状态序列。事件来源由服务端按路由选择本地存储或内部 gRPC；
公共 service 不感知具体 shard 读取位置。

删除测试：若删除该 module，checkpoint 连续性、lease 唯一性、重试退避、parked、
跨迁移补读、背压额度和错误映射会散落到 RPC、SDK、CLI 与各 shard 读取调用方，
复杂度不会消失，module 具备足够 depth。

## 持久化状态与不变量

控制 Shard 状态机当前以“每组一个 key”原子保存 meta、逐 stream progress、delivery、
lease、retry 和 parked 引用；事件 payload 始终留在数据 Shard。该布局简化原子恢复，
代价是大组 Settle 的写放大，后续可在保持 Raft 命令契约不变的前提下拆 key。

1. 同一组、同一 stream 在任意时刻至多属于一个有效 consumer lease。
2. 非 reset 操作不得降低 `next_version`；乱序 Ack 只形成 gap，连续区间闭合才推进。
3. Claim 必须先经 Raft 提交再返回，响应丢失最多造成超时重投，不会漏投。
4. 每消费者与每组有效 delivery 数不得超过配置；Fetch 字节数遵守 gRPC 8 MiB 上限。
5. delivery 只能由创建它的 consumer、group epoch 和 lease epoch 结算。
6. Retry/Park/Skip/Ack 与租约过期均为幂等状态转换；旧 delivery 重放 Settle 不得二次推进。
7. parked 视为主队列已解决；全量 replay 可晚于新版本，必须标记 `replayed`。
8. `$all` 的 shard position 仅是扫描性能提示，逐 stream checkpoint/generation 才是恢复真值。
9. ownership generation 只能单调增加；变化时受影响 Stream 从 version 0 重扫，允许重复但
   禁止因迁移重排 version 漏事件。该 Stream 的 lease、retry 与 parked 引用一并清理，事件
   回到主消费路径；其它 Stream 的 checkpoint 不受影响。
10. 控制 Shard 无 leader/quorum 时拒绝 Fetch/Settle，不允许本地降级提交。

## 背压与默认值

- Fetch 默认/上限：100/1000 条、4/7 MiB、15/30 秒长轮询。
- 每消费者最多 128 条、每组最多 4096 条未确认。
- Ack timeout 10 秒；失败最多重试 5 次；指数退避 100 ms 至 5 秒。
- `max_bytes` 是软批次上限：首条合法事件即使超过请求值也单独返回，避免饥饿；
  响应仍不得超过系统 8 MiB 上限。
- 长轮询不得持有存储事务、Raft guard 或全局互斥锁；当前每 50ms 重新检查事件、额度、
  重试与 lease，后续可替换为事件通知唤醒而不改变公开协议。

## 起点与更新

- StartSpec = `default(Start|Now)` + `stream_id -> inclusive next_version` 覆盖。
- Now 是逐 stream 观察 head，不声明跨 shard 全局瞬时点；创建后的新 stream 从 0 纳入 `$all`。
- Update 必须携带 expected revision。目标/reset 变化提升 group epoch；被移除或 reset Stream
  的旧 delivery 失效，仍保留且未 reset Stream 的活动 delivery 转为立即可投递的 retry，
  保留 event ID、attempt 与乱序 Ack gap。纯调优参数更新保留进度和有效 lease。
- reset 清单逐 stream 指定新起点，并清理该 stream 的 checkpoint gap、lease、retry 与 parked；
  从目标移除的 stream 删除全部组内状态。

## 失败与恢复

| 失败点 | 可见结果 | 恢复 |
|---|---|---|
| Claim 提交前 | 无 delivery | Fetch 重试 |
| Claim 已提交、响应丢失 | 暂时占用额度 | lease 到期后重投 |
| Settle 提交、响应丢失 | 客户端结果未知 | 相同 delivery 重试返回 AlreadySettled |
| consumer 崩溃 | 未确认 delivery 保留到 deadline | 到期后增加 attempt 并退避重投 |
| control leader 切换 | 短暂 Unavailable | SDK 根据 leader hint/节点轮换重试 |
| 数据 shard 不可用 | Fetch 返回可重试降级结果，不推进 hint | 来源恢复后从原 hint/checkpoint 继续 |
| stream 在线迁移 | 目标 version 可能重排 | generation 对账后仅将受影响 Stream 从 version 0 重扫；允许重复、禁止漏投 |
| Update reset 并发 Settle | 旧 epoch delivery 失效 | Settle 返回 StaleLease，不改变新 checkpoint |

## 兼容与验证门槛

- 保留现有 `EventStore.Subscribe` 行为和 protobuf 字段；持久化订阅使用独立 service。
- 新 `EsRequest`/`EsResponse` 变体只追加，旧 Raft 日志必须继续反序列化。
- 新状态 key 自动进入现有状态机快照区间，必须验证重启与快照往返。
- e2e 覆盖单/多 stream、`$all`、竞争消费者、背压、长轮询、Ack gap、重试、parked、
  leader 故障、无 quorum、跨节点来源和在线迁移。
- proptest 生成 Fetch/Settle/Expire/Update/Replay 序列验证上述不变量。
- `cargo test --workspace` 全部通过；行覆盖和分支覆盖均不低于 80%。
