//! 系统级消息大小上限(单一事实来源)。
//!
//! 所有 gRPC 消息(客户端 API、节点间 Raft RPC)共用一个上限:
//! 快照分块默认 3MiB/块 + bincode 头,需要比 tonic 默认 4MB 更宽的余量;
//! append 批量超限由 es-raft 网络层映射为 openraft PayloadTooLarge 拆小重试。

/// 系统级 gRPC 消息上限(字节)。
///
/// 服务端解码、客户端编码/解码、节点间 Raft RPC 统一使用该值,
/// 避免各层默认值不一致(tonic 解码默认 4MB)导致 4MB~8MB 区间消息被拒。
pub const MAX_GRPC_MESSAGE_SIZE: usize = 8 * 1024 * 1024;
