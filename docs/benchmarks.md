# AggregateStore 基准测试

## 原则

基准只测当前 Aggregate-only 路径。结果必须记录提交、硬件、操作系统、Rust 版本、Shard
数、复制因子、payload、并发度、持久化介质和是否启用 TLS，不能与旧领域接口数据混用。

## 场景

| 场景 | 指标 | 关键变量 |
|---|---|---|
| Aggregate append | ops/s、p50/p95/p99、错误率 | 实例数、OCC 模式、payload、rf |
| 幂等重试 | 首次/重复延迟、重复写数 | event ID、leader 切换 |
| 类型级 feed | events/s、端到端延迟、追平时间 | AggregateType 数、活跃分区、消费者数 |
| 状态 CAS | ops/s、冲突率、p99 | 实例数、正文大小、并发写者 |
| 状态分页 | pages/s、首包延迟、内存 | 状态总数、page size、Shard 数 |
| 消费者组 | deliveries/s、settlement/s、重投率 | consumer 数、credit、租约、payload |
| Snapshot | 生成/安装时间、压缩率 | 数据量、算法、chunk size |
| FUSE | JSONL 吞吐、syscall 延迟、缓冲峰值 | read size、写分块、并发 fd |

## 存储 microbenchmark

```bash
CARGO_TARGET_DIR=/tmp/eventfs-v2-bench-target \
  cargo bench -p es-storage --bench storage_bench --locked
```

存储 benchmark 必须构造 `AggregateAppend`，并分别覆盖多个 AggregateType、同类型多实例和
热点单实例。key 隔离和 OCC 正确性属于测试断言，不由吞吐数字替代。

## gRPC 负载

推荐分开测：

1. 单实例串行 `Exact(n)`，观察 Raft 提交下限。
2. 多实例均匀分布，观察 256 虚拟分区和多个 Shard 的扩展性。
3. 热点实例并发，记录预期 OCC 冲突而非只统计成功请求。
4. 稳定 event ID 的超时重试，确认幂等结果且无版本空洞。
5. feed 从 Beginning 追平，再进入实时阶段，分别记录 backlog 和 steady state。

类型级 feed 不提供跨实例全序，因此 benchmark 不应为排序全部事件增加客户端全局归并。
事件完整性按每个 `aggregate_id` 的 version 连续性校验。

## 消费者组负载

至少记录 Fetch 返回的 `caught_up`、`throttled`、attempt、租约超时数和四类 settlement 结果。
高吞吐不能以跳过结算或关闭实例租约为代价。故障注入后应验证：

- 已 Ack 的连续进度不倒退；
- 未结算 delivery 在租约后可重投；
- 同一实例不会并发租给两个 consumer；
- reset 后旧 epoch token 返回 stale lease。

## 结果格式

每次运行保存结构化 JSON 或 CSV，并附：

```text
git_commit=
rustc=
os=
cpu=
memory=
storage=
shards=
replication_factor=
aggregate_types=
aggregate_instances=
payload_bytes=
concurrency=
tls=
duration=
```

删除临时 `CARGO_TARGET_DIR` 前先保存最终报告；不要提交 target、临时数据目录或运行日志。
