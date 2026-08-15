# 聚合类型事件集使用固定虚拟分区

EventFS FUSE 面向每种聚合根类型暴露一个聚合类型事件集，服务端在事件集创建时固定 256 个虚拟事件分区，并按 `aggregate_id` 稳定选择分区；分区可独立归属和迁移到不同 Shard。该设计以有界的分区元数据换取实例级乐观并发与跨 Shard 扩展，同时不向生产者暴露分区或位置；消费通过不透明游标延续，各分区位置仅由服务端提交时生成。

## Consequences

现有 `EventStore` 保留单 Shard 严格有序的 Stream 语义；新增 `AggregateStore` Interface 承载聚合类型事件集，并隐藏分区路由、迁移、fencing 与消费者 checkpoint。首期不在线改变事件集的分区数量；单个极热聚合根仍受一个 Raft leader 的串行能力限制。
