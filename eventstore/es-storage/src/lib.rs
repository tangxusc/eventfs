//! EventStore 存储层：surrealkv 封装与 openraft v2 存储实现。

pub mod key;
pub mod raft_type;
pub mod reshard;
pub mod snapshot;
pub mod storage;
/// 存储值编解码。pub 供 esctl 离线工具（reshard/snapshot 验证）复用同一格式
pub mod encode;
mod log_storage;
mod state_machine;

#[cfg(test)]
mod tests;

pub use key::*;
pub use raft_type::{EsRequest, EsResponse, TypeConfig};
pub use storage::EsStorage;
