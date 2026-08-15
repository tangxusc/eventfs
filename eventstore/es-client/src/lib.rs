//! EventStore 客户端 SDK。

pub mod aggregate;
pub mod builder;
pub mod client;
pub mod persistent;

pub use aggregate::{AggregateFollowStream, AggregateStoreClient};
pub use builder::{EventBuilder, ExpectedVersionBuilder};
pub use client::{ClientError, EventStoreClient, SubscribeStream, SubscribeTarget};
pub use persistent::PersistentSubscriptionsClient;

// https 节点信任策略（connect_with_tls 用）
pub use es_proto::tls::TlsClientConfig;

// 重导出常用类型
pub use es_proto::eventstore::{
    CreatePersistentSubscriptionRequest, DeletePersistentSubscriptionRequest, Direction, Event,
    ExpectedVersion, FetchPersistentSubscriptionRequest, FetchPersistentSubscriptionResponse,
    GetStreamMetaResponse, ListParkedPersistentSubscriptionRequest,
    ListParkedPersistentSubscriptionResponse, NewEvent, PersistentDelivery, PersistentSettlement,
    PersistentSettlementAction, PersistentSettlementStatus, PersistentStartDefault,
    PersistentStartSpec, PersistentStreamReset, PersistentSubscriptionInfo,
    PersistentSubscriptionSettings, PersistentSubscriptionTarget,
    SettlePersistentSubscriptionRequest, SettlePersistentSubscriptionResponse, ShardPosition,
    SubscribeResponse, SubscriptionEvent, UpdatePersistentSubscriptionRequest,
};
// oneof 子模块（subscribe 响应的 payload 枚举匹配、构造目标）
pub use es_proto::eventstore::{subscribe_request, subscribe_response};
