//! 核心领域模型：事件、流、乐观并发。

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::hlc::Hlc;

/// 已持久化的事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub stream_id: String,
    pub version: u64,
    pub event_id: Uuid,
    pub event_type: String,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub hlc: Hlc,
    pub position: u64, // 分片内提交位置
}

/// 待写入的新事件，version/position/hlc 由服务端分配
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewEvent {
    pub event_id: Uuid,
    pub event_type: String,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
}

/// 乐观并发控制：追加时的期望版本
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedVersion {
    /// 不校验，直接追加
    Any,
    /// 要求流不存在
    NoStream,
    /// 要求流已存在
    StreamExists,
    /// 要求当前版本恰为该值
    Exact(u64),
}

/// 流元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMeta {
    pub current_version: u64,
}

impl StreamMeta {
    pub fn new() -> Self {
        Self { current_version: 0 }
    }
}

impl Default for StreamMeta {
    fn default() -> Self {
        Self::new()
    }
}
