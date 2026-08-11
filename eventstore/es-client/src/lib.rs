//! EventStore 客户端 SDK。

pub mod client;
pub mod builder;

pub use client::{EventStoreClient, ClientError};
pub use builder::{ExpectedVersionBuilder, EventBuilder};

// 重导出常用类型
pub use es_proto::eventstore::{Direction, Event, NewEvent, ExpectedVersion};
