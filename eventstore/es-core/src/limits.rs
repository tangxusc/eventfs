//! 应用层大小限制(单一事实来源)。
//!
//! 传输层之上、业务语义上的上限:服务端权威校验、客户端前置校验、
//! 快照分块启动校验共用这些常量,避免散落各处字面量漂移。

/// 单事件 data+metadata 上限(默认值,字节)。
///
/// 一条 append 批在 raft 里是一条日志条目;openraft 对单条超限的
/// AppendEntries 没有拆小路径(返回 PayloadTooLarge 会被解释为
/// Unreachable,复制停滞),因此必须从源头限制单事件大小。
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 1024 * 1024;

/// 单条 Aggregate Raft 日志允许的最大业务 payload（字节）。
///
/// 8MB 传输上限减去 1MiB 余量，用于约束可配置的 `max_event_bytes`，
/// 避免单条不可拆分的 Raft entry 被网络层拒绝。
pub const MAX_AGGREGATE_EVENT_BYTES: usize = 7 * 1024 * 1024;

/// snapshot_max_chunk_size 允许上限(字节)。
///
/// 8MB 传输上限减去 2MiB 余量(InstallSnapshotRequest 头部 + 压缩波动)。
/// openraft 0.9.25 对超限快照块直接放弃传输(无拆小路径),此校验保证
/// 分块永远不会触线。
pub const MAX_SNAPSHOT_CHUNK_BYTES: usize = 6 * 1024 * 1024;
