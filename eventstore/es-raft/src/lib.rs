//! EventStore 共识层：multi-raft 分片管理与网络通信。

pub mod admin_service;
pub mod manager;
pub mod network;
pub mod rpc_service;
pub mod shard;

pub use admin_service::RaftAdminService;
pub use manager::ShardManager;
pub use network::{normalize_endpoint, GrpcConnection, GrpcNetwork};
pub use rpc_service::RaftRpcService;
pub use shard::Shard;

// 重导出常用类型
pub use es_storage::TypeConfig;
