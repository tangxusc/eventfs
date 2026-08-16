# 多节点 AggregateStore 测试

## 目标

多节点测试验证 Aggregate-only 数据面在独立 Raft group、leader 变化和部分故障下仍保持
领域不变量。默认 workspace 测试运行进程内 gRPC e2e；显式测试覆盖分区、多节点复制和
payload 边界。

## 覆盖矩阵

| 场景 | 核心断言 |
|---|---|
| 多 Shard 注册 AggregateType | 固定 256 个虚拟分区全部激活且 placement 完整 |
| 实例稳定分区 | 同一 `(type, aggregate_id)` 始终命中同一分区和 Shard |
| 多实例追加 | 各实例 version 独立递增，key 不串写 |
| leader 重定向 | 官方客户端跟随 hint，稳定 event ID 不重复写 |
| 网络分区 | 少数派拒绝写；恢复后日志和聚合版本一致 |
| 类型级 feed | 单实例顺序保持，部分来源产生 degraded/recovered |
| 状态 CAS | 并发覆盖只有一个成功，revision 和正文一致 |
| 消费者组 | 租约、重试、结算和连续进度经复制后不丢失 |
| 快照恢复 | catalog、事件、状态和消费者进度全部恢复 |
| payload 上限 | 超限在进入 Raft 前拒绝，合法边界可复制 |

## 默认命令

所有 debug 构建使用临时 target 目录：

```bash
export CARGO_TARGET_DIR=/tmp/eventfs-v2-target
cargo test --workspace --locked
```

重点 e2e：

```bash
cargo test -p es-server --test e2e_test --locked
cargo test -p es-ctl --test e2e_test --locked
cargo test -p eventfs-fuse --test public_contract_test --locked
```

## 显式多节点测试

```bash
cargo test -p es-raft --test partition_test --locked
cargo test -p es-raft --test payload_shrink_test --locked
cargo test -p es-raft --test network_limit_test --locked
cargo test -p es-raft --test manager_test --locked
```

`partition_test` 使用 Aggregate append 驱动真实多节点 Raft，不构造旧领域命令。测试应
观察 quorum 丢失、恢复和重新选主，而不是依赖固定节点永远是 leader。

## 真实 FUSE

Linux 环境检查：

```bash
test -c /dev/fuse
cargo test -p eventfs-fuse --test mount_e2e_test --locked -- --ignored --nocapture
```

容器还需要映射 `/dev/fuse` 并授予挂载能力。Darwin 没有 `/dev/fuse`，应在验证记录中明确
标注“平台不支持，未执行”，不能记为通过。

## 故障诊断流程

1. 保存失败输出、节点日志和随机种子。
2. 区分参数错误、leader 不可用、quorum 丢失、generation fence 和测试超时。
3. 用 Raft state、AggregateType partition 列表和实例 version 验证根因假设。
4. 修复后先重跑失败用例，再运行 workspace 全套。
5. 删除临时 target 和测试数据目录。

禁止通过放宽超时、跳过断言或删除故障阶段把失败变成绿色。
