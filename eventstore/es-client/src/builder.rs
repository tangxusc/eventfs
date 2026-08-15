//! 辅助构建器，简化事件创建。

use es_proto::eventstore::{Empty, ExpectedVersion, NewEvent, expected_version};

/// 期望版本构建器
pub struct ExpectedVersionBuilder;

impl ExpectedVersionBuilder {
    /// 任意版本（不校验）
    pub fn any() -> ExpectedVersion {
        ExpectedVersion {
            kind: Some(expected_version::Kind::Any(Empty {})),
        }
    }

    /// 流不存在
    pub fn no_stream() -> ExpectedVersion {
        ExpectedVersion {
            kind: Some(expected_version::Kind::NoStream(Empty {})),
        }
    }

    /// 流已存在
    pub fn stream_exists() -> ExpectedVersion {
        ExpectedVersion {
            kind: Some(expected_version::Kind::StreamExists(Empty {})),
        }
    }

    /// 精确版本
    pub fn exact(version: u64) -> ExpectedVersion {
        ExpectedVersion {
            kind: Some(expected_version::Kind::Exact(version)),
        }
    }
}

/// 事件构建器
pub struct EventBuilder {
    event_id: Vec<u8>,
    event_type: String,
    data: Vec<u8>,
    metadata: Vec<u8>,
}

impl EventBuilder {
    /// 创建新事件
    pub fn new(event_type: impl Into<String>) -> Self {
        Self {
            event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            event_type: event_type.into(),
            data: Vec::new(),
            metadata: Vec::new(),
        }
    }

    /// 设置事件 ID
    pub fn event_id(mut self, id: uuid::Uuid) -> Self {
        self.event_id = id.as_bytes().to_vec();
        self
    }

    /// 设置数据（JSON 编码）
    pub fn data_json<T: serde::Serialize>(mut self, data: &T) -> Result<Self, String> {
        self.data = serde_json::to_vec(data).map_err(|e| e.to_string())?;
        Ok(self)
    }

    /// 设置原始数据
    pub fn data(mut self, data: Vec<u8>) -> Self {
        self.data = data;
        self
    }

    /// 设置元数据（JSON 编码）
    pub fn metadata_json<T: serde::Serialize>(mut self, metadata: &T) -> Result<Self, String> {
        self.metadata = serde_json::to_vec(metadata).map_err(|e| e.to_string())?;
        Ok(self)
    }

    /// 设置原始元数据
    pub fn metadata(mut self, metadata: Vec<u8>) -> Self {
        self.metadata = metadata;
        self
    }

    /// 构建事件
    pub fn build(self) -> NewEvent {
        NewEvent {
            event_id: self.event_id,
            event_type: self.event_type,
            data: self.data,
            metadata: self.metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use es_proto::eventstore::expected_version::Kind;

    #[test]
    fn expected_version_four_constructors() {
        assert!(matches!(
            ExpectedVersionBuilder::any().kind,
            Some(Kind::Any(_))
        ));
        assert!(matches!(
            ExpectedVersionBuilder::no_stream().kind,
            Some(Kind::NoStream(_))
        ));
        assert!(matches!(
            ExpectedVersionBuilder::stream_exists().kind,
            Some(Kind::StreamExists(_))
        ));
        assert!(matches!(
            ExpectedVersionBuilder::exact(7).kind,
            Some(Kind::Exact(7))
        ));
    }

    #[test]
    fn event_builder_defaults_empty() {
        let e = EventBuilder::new("T").build();
        assert_eq!(e.event_type, "T");
        assert!(e.data.is_empty());
        assert!(e.metadata.is_empty());
        assert_eq!(e.event_id.len(), 16, "默认生成 v4 UUID 的 16 字节");
    }

    #[test]
    fn event_builder_overrides_event_id() {
        let id = uuid::Uuid::new_v4();
        let e = EventBuilder::new("T").event_id(id).build();
        assert_eq!(e.event_id, id.as_bytes().to_vec());
    }

    /// 序列化必然失败的类型：验证 data_json/metadata_json 的 Err 分支
    struct BadSerde;
    impl serde::Serialize for BadSerde {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("故意失败"))
        }
    }

    #[test]
    fn data_json_ok_and_err() {
        let ok = EventBuilder::new("T")
            .data_json(&vec![1u8, 2])
            .expect("序列化成功");
        assert_eq!(ok.data, b"[1,2]");
        assert!(EventBuilder::new("T").data_json(&BadSerde).is_err());
    }

    #[test]
    fn metadata_json_ok_and_err() {
        let ok = EventBuilder::new("T")
            .metadata_json(&"meta")
            .expect("序列化成功");
        assert_eq!(ok.metadata, b"\"meta\"");
        assert!(EventBuilder::new("T").metadata_json(&BadSerde).is_err());
    }

    #[test]
    fn raw_data_and_metadata_passthrough() {
        let e = EventBuilder::new("T")
            .data(vec![9, 9])
            .metadata(vec![8])
            .build();
        assert_eq!(e.data, vec![9, 9]);
        assert_eq!(e.metadata, vec![8]);
    }

    #[test]
    fn chained_builder_all_effective() {
        let id = uuid::Uuid::new_v4();
        let e = EventBuilder::new("Chained")
            .event_id(id)
            .data_json(&"d")
            .unwrap()
            .metadata_json(&"m")
            .unwrap()
            .build();
        assert_eq!(e.event_type, "Chained");
        assert_eq!(e.event_id, id.as_bytes().to_vec());
        assert_eq!(e.data, b"\"d\"");
        assert_eq!(e.metadata, b"\"m\"");
    }
}
