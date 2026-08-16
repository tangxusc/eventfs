//! EventFS AggregateStore 客户端 SDK。

pub mod aggregate;
mod error;

pub use aggregate::{AggregateFollowStream, AggregateStoreClient};
pub use error::ClientError;

// https 节点信任策略（connect_with_tls 用）
pub use es_proto::tls::TlsClientConfig;
