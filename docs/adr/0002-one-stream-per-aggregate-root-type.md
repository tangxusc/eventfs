---
status: superseded by ADR-0003
---

# 每种聚合根类型使用一个 Stream

EventFS FUSE 将同一业务空间内某种聚合根类型的全部实例写入一个服务端 Stream，并以事件中的 `aggregate_id` 区分实例，而不是为每个实例创建 Stream。该选择优先控制 Stream 数量并简化用户可见路径；代价是同一类型的写入共享一个 Shard 和 Stream 顺序，因此实现必须另行定义实例级并发控制、索引和容量边界。
