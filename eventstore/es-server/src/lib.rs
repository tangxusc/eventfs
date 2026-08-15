//! EventStore 服务端库。

pub mod aggregate_service;
pub mod bootstrap;
pub mod config;
pub mod factory;
pub mod migration_service;
pub mod ownership;
pub mod persistent_service;
pub mod route_table;
pub mod server;
pub mod service;
pub mod watcher;

pub use config::Config;
pub use server::Server;
