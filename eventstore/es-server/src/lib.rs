//! EventFS AggregateStore 服务端库。

pub mod aggregate_service;
pub mod bootstrap;
pub mod config;
pub mod factory;
mod rpc_support;
pub mod server;
pub mod watcher;

pub use config::Config;
pub use server::Server;
