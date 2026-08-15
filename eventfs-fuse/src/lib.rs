//! EventFS 的路径、JSON 契约、后端适配与 Linux FUSE 实现。

pub mod backend;
pub mod codec;
pub mod config;
pub mod handle;
pub mod path;

#[cfg(target_os = "linux")]
pub mod fuse;
