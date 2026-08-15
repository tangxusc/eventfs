//! EventStore 核心领域模型、HLC、分片路由、错误类型、leader 重定向策略。

pub mod aggregate;
pub mod aggregate_group;
pub mod error;
pub mod hlc;
pub mod limits;
pub mod model;
pub mod ownership;
pub mod persistent;
pub mod redirect;
pub mod route;
pub mod routing;

pub use aggregate::{
    AggregateAppendResult, AggregateCatalog, AggregateCatalogApply, AggregateCatalogCommand,
    AggregateCatalogOutcome, AggregateEvent, AggregateEventSet, AggregateMeta, AggregateState,
    AggregateStateDocument, EVENT_PARTITION_COUNT, EventPartitionHash, EventSetId, EventSetStatus,
    ExpectedAggregateVersion, ExpectedStateRevision, NewAggregateEvent, PartitionMove,
    PartitionPlacement, aggregate_append_fingerprint, validate_aggregate_identifier,
};
pub use aggregate_group::{
    AggregateDeliveryCandidate, AggregateDeliveryEvent, AggregateDeliveryToken,
    AggregateGroupCatalog, AggregateGroupCatalogApply, AggregateGroupCatalogCommand,
    AggregateGroupCatalogOutcome, AggregateGroupDefinition, AggregateGroupDelivery,
    AggregateGroupParked, AggregateGroupPartition, AggregateGroupRetry, AggregateGroupSettings,
    AggregateGroupStart, AggregateInstanceLease, AggregateSettlement, AggregateSettlementAction,
    AggregateSettlementResult,
};
pub use error::{Error, Result};
pub use hlc::Hlc;
pub use limits::{MAX_APPEND_BATCH_BYTES, MAX_EVENT_PAYLOAD_BYTES, MAX_SNAPSHOT_CHUNK_BYTES};
pub use model::{Event, ExpectedVersion, NewEvent, StreamMeta};
pub use ownership::{
    Owner, OwnerMatch, OwnershipApply, OwnershipCatalog, OwnershipCommand, OwnershipOutcome,
};
pub use persistent::{
    DeliveryCandidate, ParkedEvent, PendingRetry, PersistentDelivery, PersistentGroup,
    PersistentSettings, PersistentTarget, Settlement, SettlementAction, SettlementResult,
    StreamLease, StreamProgress,
};
pub use redirect::{LeaderRetryPlan, parse_leader_hint};
pub use routing::route;
