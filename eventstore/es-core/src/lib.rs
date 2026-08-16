//! EventFS 聚合领域模型、HLC、错误类型与 leader 重定向策略。

pub mod aggregate;
pub mod aggregate_group;
pub mod error;
pub mod hlc;
pub mod limits;
pub mod redirect;

pub use aggregate::{
    AggregateAppendResult, AggregateCatalog, AggregateCatalogApply, AggregateCatalogCommand,
    AggregateCatalogOutcome, AggregateEvent, AggregateMeta, AggregateState, AggregateStateDocument,
    AggregateTypeDefinition, AggregateTypeId, AggregateTypeStatus, EVENT_PARTITION_COUNT,
    EventPartitionHash, ExpectedAggregateVersion, ExpectedStateRevision, NewAggregateEvent,
    PartitionMove, PartitionPlacement, aggregate_append_fingerprint, validate_aggregate_identifier,
};
pub use aggregate_group::{
    AggregateDeliveryCandidate, AggregateDeliveryEvent, AggregateDeliveryToken,
    AggregateGroupCatalog, AggregateGroupCatalogApply, AggregateGroupCatalogCommand,
    AggregateGroupCatalogOutcome, AggregateGroupDefinition, AggregateGroupDelivery,
    AggregateGroupParked, AggregateGroupPartition, AggregateGroupRetry, AggregateGroupSettings,
    AggregateGroupStart, AggregateInstanceLease, AggregateSettlement, AggregateSettlementAction,
    AggregateSettlementResult, DEFAULT_AGGREGATE_GROUP_FETCH_BYTES,
    DEFAULT_AGGREGATE_GROUP_FETCH_EVENTS, MAX_AGGREGATE_GROUP_FETCH_BYTES,
    MAX_AGGREGATE_GROUP_FETCH_EVENTS, MAX_AGGREGATE_GROUP_FETCH_WAIT_MS,
};
pub use error::{Error, Result};
pub use hlc::Hlc;
pub use limits::{MAX_AGGREGATE_EVENT_BYTES, MAX_EVENT_PAYLOAD_BYTES, MAX_SNAPSHOT_CHUNK_BYTES};
pub use redirect::{LeaderRetryPlan, parse_leader_hint};
