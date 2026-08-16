---
status: superseded by ADR-0006
---

# 聚合类型使用固定虚拟分区

EventFS FUSE 面向每种聚合根类型暴露一个 AggregateType，服务端在类型注册时固定 256 个虚拟分区，并按 `aggregate_id` 稳定选择分区。该设计以有界的分区元数据换取实例级乐观并发与跨 Shard 扩展，同时不向生产者暴露分区或位置；消费通过不透明 cursor 延续，各分区位置仅由服务端提交时生成。

## Consequences

ADR-0006 删除了并行存在的通用事件接口，只保留 AggregateStore。分区数量仍固定；单个极热聚合实例仍受一个 Raft leader 的串行能力限制。
