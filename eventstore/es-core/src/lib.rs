//! EventStore 核心领域模型、HLC、分片路由、错误类型、leader 重定向策略。

pub mod error;
pub mod hlc;
pub mod limits;
pub mod model;
pub mod ownership;
pub mod redirect;
pub mod route;
pub mod routing;

pub use error::{Error, Result};
pub use hlc::Hlc;
pub use limits::{MAX_APPEND_BATCH_BYTES, MAX_EVENT_PAYLOAD_BYTES, MAX_SNAPSHOT_CHUNK_BYTES};
pub use model::{Event, ExpectedVersion, NewEvent, StreamMeta};
pub use ownership::{
    Owner, OwnerMatch, OwnershipApply, OwnershipCatalog, OwnershipCommand, OwnershipOutcome,
};
pub use redirect::{LeaderRetryPlan, parse_leader_hint};
pub use routing::route;
