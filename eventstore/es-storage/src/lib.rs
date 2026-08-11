//! EventStore 存储层：surrealkv 封装与 openraft v2 存储实现。

pub mod key;
pub mod raft_type;
pub mod reshard;
pub mod storage;
mod log_storage;
mod state_machine;

#[cfg(test)]
mod tests;

pub use key::*;
pub use raft_type::{EsRequest, EsResponse, TypeConfig};
pub use storage::EsStorage;
