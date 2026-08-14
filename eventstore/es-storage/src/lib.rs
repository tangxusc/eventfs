//! EventStore 存储层：surrealkv 封装与 openraft v2 存储实现。

/// 存储值编解码。pub 供 esctl 离线工具（reshard/snapshot 验证）复用同一格式
pub mod encode;
pub mod key;
mod log_storage;
pub mod raft_type;
pub mod snapshot;
mod state_machine;
pub mod storage;

#[cfg(test)]
mod tests;

pub use key::*;
pub use raft_type::{
    EsRequest, EsResponse, PersistentSubscriptionCommand, PersistentSubscriptionResponse,
    TypeConfig,
};
pub use storage::EsStorage;
