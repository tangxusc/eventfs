# EventFS 领域词汇

EventFS 保存聚合事件、当前状态和消费进度。本词汇表定义对外统一语言。

## Language

**业务空间（Business Space）**：
隔离一组业务模型的命名边界；相同聚合类型名可以存在于不同业务空间。
_Avoid_: Namespace、Tenant

**聚合类型（Aggregate Type）**：
同一业务空间内一类聚合根的定义，由 `(business_space, aggregate_type)` 标识。
_Avoid_: 类型集合、聚合类型序列

**聚合实例（Aggregate Instance）**：
某个聚合类型下由 `aggregate_id` 标识的业务实体；完整身份为 `(business_space, aggregate_type, aggregate_id)`。
_Avoid_: Stream、Stream ID、Event ID

**聚合事件（Aggregate Event）**：
描述一个聚合实例已发生事实的不可变记录。
_Avoid_: 状态、消息

**聚合版本（Aggregate Version）**：
单个聚合实例内严格递增的事件版本，用于顺序和乐观并发控制。
_Avoid_: Shard Position、全局位置

**类型级事件 Feed（Aggregate Type Event Feed）**：
一个聚合类型下全部实例事件的持续读取视图；保证实例内顺序，不承诺实例间全序。
_Avoid_: 实例历史、全局事件序列

**Cursor**：
类型级事件 feed 已消费位置的不透明续读凭据。
_Avoid_: 聚合版本、可解析位置

**业务状态文档（Aggregate State Document）**：
一个聚合实例可覆盖的当前状态表示，使用独立 revision 做并发控制。
_Avoid_: Raft Snapshot、事件历史

**聚合消费者组（Aggregate Group）**：
按名称共享类型级事件 feed 消费进度、租约和重试策略的消费边界。
_Avoid_: Persistent Subscription、文件读取者

**投递（Delivery）**：
消费者组在租约期限内交给某个消费成员处理的聚合事件。
_Avoid_: 已确认事件、永久所有权

**结算（Settlement）**：
消费成员对投递作出的 Ack、Retry、Park 或 Skip 决定。
_Avoid_: 文件关闭、读取成功

**虚拟分区（Aggregate Partition）**：
聚合类型内部稳定划分实例的消费与存储单元；同一聚合实例始终落在同一虚拟分区。
_Avoid_: Shard、公共路径层级

**Shard**：
独立复制并提交状态变更的故障隔离单元；属于部署概念，不属于聚合实例身份。
_Avoid_: 聚合类型、虚拟分区

**Raft Snapshot**：
用于复制恢复和日志压缩的 Shard 状态机备份。
_Avoid_: 业务状态文档、业务快照
