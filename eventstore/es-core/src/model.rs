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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_meta_new_and_default_version_zero() {
        assert_eq!(StreamMeta::new().current_version, 0);
        assert_eq!(StreamMeta::default().current_version, 0);
        assert_eq!(StreamMeta::default().current_version, StreamMeta::new().current_version);
    }

    #[test]
    fn event_serde_roundtrip() {
        let e = Event {
            stream_id: "s1".to_string(),
            version: 3,
            event_id: Uuid::new_v4(),
            event_type: "UserCreated".to_string(),
            data: b"data".to_vec(),
            metadata: b"meta".to_vec(),
            hlc: Hlc::next(None, 42),
            position: 7,
        };
        let bytes = serde_json::to_vec(&e).expect("序列化");
        let back: Event = serde_json::from_slice(&bytes).expect("反序列化");
        assert_eq!(back.stream_id, e.stream_id);
        assert_eq!(back.version, e.version);
        assert_eq!(back.event_id, e.event_id);
        assert_eq!(back.event_type, e.event_type);
        assert_eq!(back.data, e.data);
        assert_eq!(back.metadata, e.metadata);
        assert_eq!(back.hlc, e.hlc);
        assert_eq!(back.position, e.position);
    }

    #[test]
    fn new_event_serde_roundtrip() {
        let ne = NewEvent {
            event_id: Uuid::new_v4(),
            event_type: "X".to_string(),
            data: vec![1, 2, 3],
            metadata: vec![],
        };
        let bytes = serde_json::to_vec(&ne).expect("序列化");
        let back: NewEvent = serde_json::from_slice(&bytes).expect("反序列化");
        assert_eq!(back.event_id, ne.event_id);
        assert_eq!(back.event_type, ne.event_type);
        assert_eq!(back.data, ne.data);
    }

    #[test]
    fn expected_version_all_variants_serde_roundtrip() {
        for v in [
            ExpectedVersion::Any,
            ExpectedVersion::NoStream,
            ExpectedVersion::StreamExists,
            ExpectedVersion::Exact(99),
        ] {
            let bytes = serde_json::to_vec(&v).expect("序列化");
            let back: ExpectedVersion = serde_json::from_slice(&bytes).expect("反序列化");
            assert_eq!(back, v);
        }
    }

    #[test]
    fn stream_meta_serde_roundtrip() {
        let m = StreamMeta { current_version: 5 };
        let back: StreamMeta = serde_json::from_slice(&serde_json::to_vec(&m).unwrap()).unwrap();
        assert_eq!(back.current_version, 5);
    }
}
