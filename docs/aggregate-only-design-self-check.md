# Aggregate-only 设计自检

日期：2026-08-16
分支：`codex/aggregate-only`
实施 worktree：`/private/tmp/eventfs-v2-aggregate-only`

## 术语

- [x] 公共领域名统一为 `AggregateType`；删除 `EventSet`。
- [x] 聚合实例身份固定为 `(business_space, aggregate_type, aggregate_id)`。
- [x] `stream` 只允许出现在 Rust/tonic 流式传输等技术语境，不作为领域对象。
- [x] FUSE 读句柄使用 `StreamingRead`，避免与已删除领域对象混淆。
- [x] `CONTEXT.md` 仅保存领域词汇；实现与兼容决策分别进入设计文档和 ADR。

## 不变量

- [x] 每个 AggregateType 固定 256 个虚拟分区。
- [x] 同一 aggregate ID 稳定分区，实例内 aggregate version 严格递增。
- [x] 事件追加同时执行 OCC 和 event ID 幂等校验。
- [x] 状态 revision 独立 CAS，正文与 modified HLC 原子提交。
- [x] 类型级 feed 保持实例内顺序，不承诺实例间全序。
- [x] 消费者组只推进连续已结算进度，并保护同实例租约。
- [x] cursor、page token 和 delivery ID 对调用方不透明。

## 数据流

```text
client/FUSE -> AggregateStore -> catalog/partition routing -> Shard Raft
            -> Aggregate state machine -> event/state/group keys
            -> aggregate notification -> FollowAggregateTypeEvents
```

- [x] 公共 listener 只装配 AggregateStore、RaftRpc、RaftAdmin。
- [x] 内部 listener 只装配 AggregateStoreInternal。
- [x] 远程 Shard 定位和 Raft 错误映射位于中性 `rpc_support` 模块。
- [x] `RuntimeTopology` 原子发布 Shard 放置集合与远端节点定位器。
- [x] 事件通知只覆盖 Aggregate append。

## 删除审计

- [x] 删除 EventStore protobuf service 与客户端。
- [x] 删除 PersistentSubscriptions、InternalSubscription、OwnershipInternal、Migration。
- [x] 删除旧事件 model/builder、订阅、ownership、route table 和通用迁移实现。
- [x] 删除对应 CLI 命令、服务装配、示例和测试。
- [x] 状态机不再接受旧领域 Raft command。
- [x] 删除 Stream migration 文档，主文档只描述 AggregateStore。

## 兼容决策

- [x] 不兼容旧数据目录、旧客户端、旧 protobuf 和旧 Raft 日志。
- [x] 不提供在线或离线迁移工具，升级使用全新数据目录。
- [x] 保留 EventFS 品牌、`eventstore/` 目录、`es-*` crate 和三个二进制名称。
- [x] 不增加实例级完整事件历史读取接口。
- [x] ADR-0006 记录选择与后果，ADR-0004 标记为被替代。

## 失败模式

| 失败 | 预期行为 |
|---|---|
| 非法身份/payload | 进入 Raft 前拒绝 |
| OCC/CAS 不匹配 | 原子拒绝且不改变版本/revision |
| 幂等 ID 内容冲突 | 保留原结果并返回冲突 |
| leader 或 Shard 不可用 | 返回 leader hint/Unavailable；幂等客户端可重试 |
| 热更新新增 Shard 失败 | 不发布半成品拓扑，继续使用旧 Shard/peer 快照 |
| 热更新移除 Shard/控制 Shard 变化 | 拒绝更新并保留旧拓扑 |
| feed 部分来源不可用 | degraded，恢复后 recovered，cursor 不倒退 |
| 消费租约或 epoch 过期 | stale lease，不推进进度 |
| 旧数据目录 | 明确不支持，不尝试兼容读取 |

## 验证证据

所有 debug/coverage 构建均使用 `/private/tmp` 下的独立 `CARGO_TARGET_DIR`；分支覆盖率
使用本机 nightly 工具链（stable 不支持 `-Z coverage-options=branch`）。验收结束后删除
这些构建目录、worktree 内意外生成的 `target` 和覆盖率 JSON。

| 检查 | 结果 |
|---|---|
| `cargo fmt --check` | 通过 |
| 严格 clippy | `cargo clippy --workspace --all-targets --locked -- -D warnings` 通过 |
| `cargo test --workspace --locked` | 通过，所有单元、集成、e2e 和文档测试零失败 |
| AggregateStore gRPC / client / CLI / FUSE backend e2e | 通过：client 6、server e2e 3、CLI e2e 4、FUSE contract 4 |
| 显式多节点 Aggregate 测试 | `cargo test -p es-raft --test partition_test --locked`，7/7 通过 |
| Linux 真实 FUSE mount e2e | Darwin 无 `/dev/fuse`，平台不支持 |
| 行覆盖率 | 88.497%（10940/12362），通过 80% 门槛 |
| 分支覆盖率 | 80.230%（767/956），通过 80% 门槛 |
| 协议与术语残留审计 | 指定旧协议、类型、CLI 与配置符号零命中 |
| 临时构建目录清理 | 完成：删除两处 `target` 和覆盖率临时文件 |
