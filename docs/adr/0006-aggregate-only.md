---
status: accepted
---

# 仅保留 AggregateStore 领域模型

EventFS 只保留 AggregateStore：聚合实例由 `(business_space, aggregate_type, aggregate_id)`
标识，按实例执行 OCC，并通过类型级 feed、状态文档和消费者组提供读取能力。通用
EventStore Stream 与其订阅、归属、路由和迁移协议被删除，因为两套相近模型增加了术语、
存储 key、Raft 命令和运维面的重复，而 FUSE 的实际需求全部围绕聚合类型。

## Considered Options

- 继续并存两套接口：兼容性最好，但重复边界和维护成本最高。
- 将 AggregateStore 适配到通用 Stream：减少存储模型，但会暴露实例历史和物理读取语义。
- 仅保留 AggregateStore：领域边界最清晰，代价是明确放弃旧数据和协议兼容。

## Consequences

每个聚合类型固定 256 个虚拟分区；类型级 feed 保证实例内顺序但不提供实例间全序，也不
新增按 `aggregate_id` 回放完整事件历史的接口。旧数据目录、客户端和 Raft 日志不可复用，
升级必须部署全新数据目录；不提供迁移工具。EventFS 品牌、`eventstore/` 目录、`es-*`
crate 和三个二进制名称保持不变。
