//! EventStore 核心领域模型、HLC、分片路由、错误类型、leader 重定向策略。

pub mod error;
pub mod hlc;
pub mod model;
pub mod redirect;
pub mod routing;

pub use error::{Error, Result};
pub use hlc::Hlc;
pub use model::{Event, ExpectedVersion, NewEvent, StreamMeta};
pub use redirect::{LeaderRetryPlan, parse_leader_hint};
pub use routing::route;
