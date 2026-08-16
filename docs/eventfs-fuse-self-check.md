# EventFS FUSE 设计自检存档

日期：2026-08-15
状态：用户已确认共享理解，准许进入实施

## 需求覆盖

| 需求 | 设计落点 | 结果 |
|---|---|---|
| Shell 追加 JSON 事件 | `events.jsonl` 写句柄、严格 v1 envelope | 已覆盖 |
| 同实例 OCC、实例间独立 | `aggregate_version` + 稳定事件分区 | 已覆盖 |
| 避免海量 Stream 与单 Shard 热点 | 每类型一个事件集、内部 256 分区 | 已覆盖 |
| 持续读取 | server-streaming + opaque cursor + poll | 已覆盖 |
| 状态覆盖与 fsync CAS | `states/{aggregate_id}.json` | 已覆盖 |
| 消费者组续读 | 分区 checkpoint + 实例 lease | 已覆盖 |
| 显式 Ack | 同一成员路径的 O_WRONLY 批量 settlement | 已覆盖 |
| 路径精简 | 每类型两个固定入口，加按需成员与状态文件 | 已覆盖 |
| Linux FUSE3 | fuser 0.18 + 真挂载 e2e | 已覆盖 |

## 核心不变量检查

- 同一聚合实例的版本校验与事件落盘位于同一 Raft apply 和存储事务。
- 生产者不提供分区位置；服务端位置只服务于 cursor、checkpoint 与迁移。
- 分区迁移绝不双写，generation fencing 阻止旧归属继续写入。
- 普通读取没有持久 checkpoint；消费者组才提供 at-least-once 续读。
- 关闭消费文件不等于 Ack；租约到期后允许重投。
- 状态、事件和结算分别原子，不暗示跨文件事务。
- FUSE 不保存本地权威状态，服务端始终是唯一恢复来源。
- 旧 EventStore 与新 AggregateStore 互不解释、互不双写。

## 方案复核

| 方案 | 结论 | 原因 |
|---|---|---|
| 每聚合实例一个 Stream | 拒绝 | 归属、路由与组元数据随实例数无界增长 |
| 每类型一个物理 Stream | 被 ADR-0003 取代 | 所有实例共享一个 leader，形成热点且共享 OCC 边界 |
| 类型事件集 + 固定虚拟分区 | 采用 | 路由元数据有界、实例级 OCC、可跨 Shard 迁移 |
| 纯 FUSE 模拟状态和消费 | 拒绝 | 本地状态无法成为集群权威，故障语义失真 |
| FUSE 本地 WAL | 首期拒绝 | 引入双重恢复来源；跨句柄幂等由调用方 UUID 明确承担 |
| fuse3 crate | 拒绝 | 0.9 MSRV 1.91 不兼容；0.8 poll 未稳定 |
| fuser 0.18 稳定 Interface | 采用 | Rust 1.88 兼容，所需 FUSE 能力有稳定接口与真挂载 CI 证据 |

## 风险与验证门槛

1. `fuser` 是同步回调模型，必须先用 Linux smoke spike 证明 deferred Reply、Tokio RPC、poll wakeup 和取消不会阻塞 dispatcher。
2. 256 分区的 catalog、cursor 与 group progress 成本必须有规模测试；不能只验证少量分区。
3. 部分分区降级后继续输出会产生跨实例晚到事件，必须通过 `kind:"degraded"` 明示并验证恢复不漏。
4. 单个极热聚合实例仍是串行瓶颈，必须暴露诊断信息，不能宣传为可透明横向拆分。
5. 可选 `event_id` 留下跨新句柄模糊重试重复窗口，文档与 e2e 必须覆盖。
6. 事件永久保留会持续占用存储；首期必须在文档和容量规划中显式说明。
7. 历史真实多进程失败必须以事件可见性而非不同日志基线上的 applied 水位复验。
8. 真挂载证据必须来自提供 `/dev/fuse` 的 Linux 环境，不能以无挂载单测替代。

## 代码证据

- `eventstore/es-proto/proto/eventstore.proto`：现有 EventStore 与 PersistentSubscriptions Interface，以及输入事件不携带 version/position 的事实。
- `eventstore/es-storage/src/state_machine.rs`：现有 Stream 级 OCC、Shard position 与单事务 apply。
- `eventstore/es-core/src/persistent.rs`：现有 Stream 级 checkpoint、lease、Ack gap、Retry 与 Park。
- `eventstore/es-core/src/route.rs` 与 `ownership.rs`：现有 Stream 唯一归属一个 Shard、generation fencing 与迁移控制事实。
- `eventstore/es-server/tests/multi_node_test.rs`：真实子进程、随机端口、SIGTERM 回收与覆盖率 profile 模式。
- `docs/multi_node_testing.md`：已知多进程故障证据。
- `docs/benchmarks.md`：现有 EventStore 性能基线。

## 自检结论

当前设计没有静默依赖“实例间全序”“关闭即 Ack”“本地状态可恢复”或“Raft Snapshot 会删除业务事件”等错误假设。Interface、错误、故障恢复、兼容、部署与测试分支均已访问；用户已确认双方达到共享理解，实施必须逐阶段提供测试证据。

## 实施验证记录

2026-08-16 全功能验收使用匹配的 nightly LLVM 23 工具重新统计当前 workspace：行覆盖
`90.98%`、分支覆盖 `81.40%`、函数覆盖 `82.29%`、区域覆盖 `89.37%`。行和分支均
通过 80% 门禁。环境型用例独立执行，包括 14 项真实 `es-server` 多进程测试、2 项
真实 `esctl` 多进程测试和 1 项真 FUSE 挂载测试；三类均已通过本轮复验。

该次验收已包含 8 MiB 流背压、256 项状态分页与 inode 回收、租约 deadline 驱动
Renew、模糊 PutState/Fetch 不重放、`FUSE_ATOMIC_O_TRUNC` 和 `allow_other` ACL
契约断言。最近一次行和分支覆盖率均通过 80% 门禁；真多进程与真 FUSE 挂载按上述
风险门槛完成独立验收。
