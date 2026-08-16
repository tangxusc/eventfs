//! EventFS 共识层：multi-raft Shard 管理与网络通信。

pub mod admin_service;
pub mod manager;
pub mod network;
pub mod rpc_service;
pub mod shard;

pub use admin_service::RaftAdminService;
pub use manager::ShardManager;
pub use network::{GrpcConnection, GrpcNetwork, normalize_endpoint};
pub use rpc_service::RaftRpcService;
pub use shard::Shard;

// 重导出常用类型
pub use es_proto::tls::TlsClientConfig;
pub use es_storage::TypeConfig;
