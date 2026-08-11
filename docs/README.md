# 文档索引

EventFS v2 —— 基于 Rust + openraft 的分布式事件存储。本目录收录设计与专题文档;
项目概览、快速开始与路线图见 [根目录 README](../README.md)。

## 设计文档

| 文档 | 内容 |
|---|---|
| [design.md](design.md) | 架构设计总览：Key 编码与排序性质证明、写入路径、乐观并发、幂等、HLC、分片路由、gRPC 接口、测试策略、本期不实现清单 |

## 专题文档

| 文档 | 内容 |
|---|---|
| [multi_node_testing.md](multi_node_testing.md) | 多节点与网络分区测试、集群组建流程、实现要点与踩坑记录 |
| [snapshot.md](snapshot.md) | 快照四方法实现要点、参数权衡、测试覆盖 |
| [reshard.md](reshard.md) | 分片数变更三种方案对比、离线重分布设计、实现现状与计划 |
| [benchmarks.md](benchmarks.md) | 基准结果（读取延迟、reshard 吞吐）与未覆盖场景 |

## 归档

| 文档 | 说明 |
|---|---|
| [archive/](archive/) | 历史交付报告（DELIVERY.md、PROJECT_SUMMARY.md），仅备查，内容已过时，不再维护 |
