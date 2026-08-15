# 文档索引

EventFS v2 —— 基于 Rust + openraft 的分布式事件存储。本目录收录设计与专题文档;
项目概览、快速开始与路线图见 [根目录 README](../README.md)。

## 设计文档

| 文档 | 内容 |
|---|---|
| [design.md](design.md) | 架构设计总览：Key 编码与排序性质证明、写入路径、乐观并发、幂等、HLC、流路由表（显式分配）、gRPC 接口、测试策略、本期不实现清单 |
| [eventfs-fuse.md](eventfs-fuse.md) | AggregateStore 与 eventfs-fuse 设计：事件集分区、文件契约、状态 CAS、消费者组、故障语义与验收 |
| [eventfs-fuse-self-check.md](eventfs-fuse-self-check.md) | eventfs-fuse 设计自检：需求覆盖、不变量、替代方案、风险与实施门槛 |

## 专题文档

| 文档 | 内容 |
|---|---|
| [multi_node_testing.md](multi_node_testing.md) | 多节点与网络分区测试、集群组建流程、实现要点与踩坑记录 |
| [snapshot.md](snapshot.md) | 快照四方法实现要点、参数权衡、测试覆盖 |
| [migrate.md](migrate.md) | 在线迁移设计（状态机、幂等原语、切换窗口、断点续传）、esctl migrate / route 用法 |
| [esctl.md](esctl.md) | esctl 命令行工具完整手册（参数、输出格式、leader 发现策略） |
| [benchmarks.md](benchmarks.md) | 基准结果（读取延迟、端到端写入/读/订阅吞吐）与未覆盖场景 |

## 归档

| 文档 | 说明 |
|---|---|
| [archive/](archive/) | 历史交付报告（DELIVERY.md、PROJECT_SUMMARY.md），仅备查，内容已过时，不再维护 |
