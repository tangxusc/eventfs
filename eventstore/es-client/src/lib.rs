//! EventStore 客户端 SDK。

pub mod client;
pub mod builder;

pub use client::{ClientError, EventStoreClient, SubscribeStream, SubscribeTarget};
pub use builder::{ExpectedVersionBuilder, EventBuilder};

// https 节点信任策略（connect_with_tls 用）
pub use es_proto::tls::TlsClientConfig;

// 重导出常用类型
pub use es_proto::eventstore::{
    Direction, Event, GetStreamMetaResponse, NewEvent, ExpectedVersion, ShardPosition,
    SubscribeResponse,
};
// oneof 子模块（subscribe 响应的 payload 枚举匹配、构造目标）
pub use es_proto::eventstore::{subscribe_request, subscribe_response};
