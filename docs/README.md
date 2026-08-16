# 文档索引

EventFS v2 —— 基于 Rust + openraft 的分布式事件存储。本目录收录设计与专题文档；
项目概览、快速开始与当前限制见 [根目录 README](../README.md)。

## 设计文档

| 文档 | 内容 |
|---|---|
| [领域词汇](../CONTEXT.md) | Stream、事件分区、聚合版本、归属权威等项目内规范术语 |
| [design.md](design.md) | 架构设计总览：Key 编码与排序性质证明、写入路径、乐观并发、幂等、HLC、流路由表（显式分配）、gRPC 接口、测试策略、本期不实现清单 |
| [eventfs-fuse.md](eventfs-fuse.md) | AggregateStore 与 eventfs-fuse 设计：事件集分区、文件契约、状态 CAS、消费者组、故障语义与验收 |
| [ADR](adr/) | 已确认且难以逆转的架构决策及其取舍 |

## 专题文档

| 文档 | 内容 |
|---|---|
| [deployment.md](deployment.md) | 构建、服务配置、集群组建、TLS、扩容、Release artifact 与 Docker Compose |
| [multi_node_testing.md](multi_node_testing.md) | 多节点与网络分区测试、集群组建流程、实现要点与踩坑记录 |
| [snapshot.md](snapshot.md) | 快照四方法实现要点、参数权衡、测试覆盖 |
| [migrate.md](migrate.md) | 在线迁移设计（状态机、幂等原语、切换窗口、断点续传）、esctl migrate / route 用法 |
| [esctl.md](esctl.md) | esctl 命令行工具完整手册（参数、输出格式、leader 发现策略） |
| [benchmarks.md](benchmarks.md) | 基准结果（读取延迟、端到端写入/读/订阅吞吐）与未覆盖场景 |
