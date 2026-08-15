# EventFS FUSE 不维护本地持久化状态

eventfs-fuse 除打开句柄的临时缓冲外不持久化路由、游标、待提交写入、状态 revision 或消费者 checkpoint，所有权威状态均由 `AggregateStore` 保存。该选择简化故障恢复并避免本地状态与集群分叉；代价是 FUSE 自动生成的 `event_id` 只能覆盖当前句柄内的 RPC 重试，调用方若要在模糊提交结果后跨进程、跨句柄或跨挂载安全重试，必须显式复用自己的稳定 UUID。
