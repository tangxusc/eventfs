//! EventStore 客户端 SDK。

pub mod client;
pub mod builder;

pub use client::{ClientError, EventStoreClient};
pub use builder::{ExpectedVersionBuilder, EventBuilder};

// https 节点信任策略（connect_with_tls 用）
pub use es_proto::tls::TlsClientConfig;

// 重导出常用类型
pub use es_proto::eventstore::{Direction, Event, NewEvent, ExpectedVersion};
