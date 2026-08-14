# Stream 强一致归属设计自检

## 审查范围

已探查：

- `eventstore/es-core/src/route.rs`
- `eventstore/es-server/src/route_table.rs`
- `eventstore/es-server/src/service.rs`
- `eventstore/es-server/src/migration_service.rs`
- `eventstore/es-server/src/watcher.rs`
- `eventstore/es-storage/src/raft_type.rs`
- `eventstore/es-storage/src/state_machine.rs`
- `eventstore/es-proto/proto/eventstore.proto`
- 多节点、watcher 与迁移测试

## Interface 比较

| 方案 | External interface | Depth | Locality | 结论 |
|---|---|---|---|---|
| 最小型 | `view / claim / commit` | 高 | 高 | 概念较多，Append 调用者仍需理解 view 与 permit |
| 可扩展型 | `current / submit / inspect` | 中 | 高 | 批量、审计、历史查询当前均属 YAGNI |
| 常用路径优先 | `for_append / known / change` | 高 | 高 | 采用；Append 最简单，其余变更只有一个入口 |

推荐 interface 的外形：

```rust
pub struct StreamOwnership { /* implementation private */ }

impl StreamOwnership {
    pub async fn for_append(&self, stream: &str) -> Result<AppendTarget, OwnershipError>;
    pub async fn known(&self, stream: &str) -> Option<Owner>;
    pub async fn change(
        &self,
        change: OwnershipChange,
    ) -> Result<ChangeReceipt, OwnershipError>;
}
```

`AppendTarget` 字段私有，只公开读取方法。调用者不能用裸 Shard ID 绕过归属代次。

## 不变量

1. 任一已提交 revision 中，一个 Stream 至多归属一个 Shard。
2. 并发 `for_append` 对同一未知 Stream 返回相同 Shard 与 generation，只有一方报告新建。
3. 无法取得控制 Shard quorum 时返回可重试错误，绝不本地分配。
4. 已知 Stream 走本地投影；Append 仍必须携带 generation，由数据 Shard fencing 验证。
5. `change` 必须携带期望归属或期望 revision，过期请求返回 Conflict。
6. `routes.json` 可读取旧三字段格式，但运行时文件修改不能直接替换权威状态。
7. 控制 Shard 首次选择后持久化；加入更小编号 Shard 或重启不得改变它。
8. 兼容投影不能产生或仲裁归属 revision；广播只能触发权威刷新。
9. 迁移 CAS 使用调用方观察到的归属和稳定 operation ID，不能由服务端临时补读。

## Seam 与 adapter

归属共识是 remote-but-owned 依赖，内部 `OwnershipCommitPort` seam 有两个 adapter：

- production adapter：本地提交，或经仅内部 listener 暴露的 gRPC 转发到控制 Shard leader。
- memory test adapter：串行内存状态，可注入超时、重复、冲突和恢复。

数据 fencing 使用相同原则：production adapter 写数据 Shard Raft，memory test adapter 注入阶段故障。两个 adapter 证明 seam 真实存在。

## 删除测试

删除 `StreamOwnership` 后，线性化裁决、generation、fencing、旧文件投影、迁移条件切换、watcher 导入和错误映射会重新散到写入、迁移、watcher、protobuf 与存储代码中。复杂度不会消失，module 具备足够 depth。

## 失败与恢复自检

| 位置 | 失败结果 | 恢复方式 |
|---|---|---|
| 归属提交前 | 无状态变化 | 原请求重试 |
| Raft 已提交但响应丢失 | 结果未知 | 以 operation ID 幂等重试 |
| 归属已提交但目标 fence 未安装 | Stream 暂不可写 | 重试继续安装，禁止本地回退 |
| 迁移已 fence 源但尚未发布新归属 | Stream 暂不可写 | 相同 operation ID 继续流程 |
| `routes.json` 投影失败 | 权威状态不回滚 | 后台或下次提交重建投影 |
| watcher 读到旧投影 | 不改变权威 | revision/digest 去重 |

## 兼容性自检

- 旧 `routes.json` 可读，新版本继续写原三字段。
- 公开客户端 protobuf 不增加必填字段。
- 内部 protobuf 与 Raft 日志会改变，不支持新旧服务器混跑。
- 既有 `DeleteStream` Raft 日志的枚举编号保持不变，并用旧字节流测试验证。
- 初次升级时，控制 Shard 归属状态为空才允许导入旧文件；已有权威状态时文件仅作投影。

## 测试门槛

- 双节点并发首次归属 e2e。
- 控制 Shard leader 故障与无 quorum e2e。
- 旧 `routes.json` 导入与往返测试。
- generation fencing 与迁移切换窗口测试。
- 归属命令序列 proptest，覆盖幂等、冲突与计数不变量。
- 工作区行覆盖与分支覆盖均不低于 80%。

## 验收证据（2026-08-14）

| 验收项 | 结果 |
|---|---|
| `cargo test --workspace` | 502 项通过，16 项真实多进程用例按设计忽略 |
| 工作区覆盖率 + 2 项关键多进程 e2e 合并 profile | 行 92.77%，分支 80.22%，区域 91.26%，函数 84.20% |
| 归属 catalog | 行 98.47%，分支 90.00% |
| `StreamOwnership` module | 行 95.03%，分支 80.00% |
| 多节点并发首次归属 | 同一 Stream 只产生一个 Shard + generation |
| 控制 Shard 无 quorum | 首次写入被拒绝；quorum 恢复后重试成功 |
| watcher 调和 | 篡改投影被恢复，控制 Shard 不可被热更新移除，重复事件幂等 |
| 模糊测试 | 归属 Ensure 序列与随机 Append/DeleteStream 不变量通过 |
