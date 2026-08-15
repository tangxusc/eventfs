# AggregateStore 与 EventStore 保持数据隔离

新增的 `AggregateStore` Interface 使用聚合类型事件集、实例级版本和虚拟事件分区，不自动读取、改写或迁移现有 `EventStore` Stream。两套 Interface 各自保持唯一写入权威；eventfs-fuse 只使用 `AggregateStore`，旧数据如需转换必须通过显式迁移工具完成，以避免双写、身份推断和版本语义冲突。
