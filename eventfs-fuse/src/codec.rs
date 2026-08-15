//! 严格 JSON 输入与紧凑 JSONL 输出。

use std::fmt;

use es_proto::eventstore::{AggregateEvent, AggregateGroupDelivery};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// 事件追加 envelope。
#[derive(Debug, Clone, PartialEq)]
pub struct EventEnvelope {
    pub aggregate_id: String,
    pub event_type: String,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub event_id: Uuid,
    pub expected_version: ExpectedVersion,
}

/// 实例级 OCC 条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedVersion {
    Any,
    NoAggregate,
    Exists,
    Exact(u64),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireEventEnvelope {
    spec_version: String,
    aggregate_id: String,
    event_type: String,
    data: Value,
    event_id: Option<Uuid>,
    expected_version: Option<WireExpectedVersion>,
    #[serde(default = "empty_object")]
    metadata: Value,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum WireExpectedVersion {
    Any,
    NoAggregate,
    Exists,
    Exact { version: u64 },
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

/// 解析一个完整事件 envelope。
///
/// # 参数
/// `bytes` 必须只包含一个 JSON 值；`max_bytes` 是句柄配置的输入上限。
///
/// # 返回
/// 返回可稳定重试的事件输入，缺省 event ID 在本次解析时生成一次。
///
/// # 错误
/// 超限、字段缺失/重复/未知、JSON 非法或协议版本不支持时返回 [`CodecError`]。
pub fn parse_event(bytes: &[u8], max_bytes: usize) -> Result<EventEnvelope, CodecError> {
    check_size(bytes, max_bytes)?;
    let wire: WireEventEnvelope = serde_json::from_slice(bytes).map_err(CodecError::Json)?;
    if wire.spec_version != "1.0" {
        return Err(CodecError::UnsupportedSpec(wire.spec_version));
    }
    es_core::validate_aggregate_identifier("aggregate_id", &wire.aggregate_id)
        .map_err(|_| CodecError::InvalidField("aggregate_id"))?;
    if wire.event_type.is_empty() {
        return Err(CodecError::InvalidField("event_type"));
    }
    if !wire.metadata.is_object() {
        return Err(CodecError::InvalidField("metadata"));
    }
    let expected_version = match wire.expected_version {
        None | Some(WireExpectedVersion::Any) => ExpectedVersion::Any,
        Some(WireExpectedVersion::NoAggregate) => ExpectedVersion::NoAggregate,
        Some(WireExpectedVersion::Exists) => ExpectedVersion::Exists,
        Some(WireExpectedVersion::Exact { version }) => ExpectedVersion::Exact(version),
    };
    Ok(EventEnvelope {
        aggregate_id: wire.aggregate_id,
        event_type: wire.event_type,
        data: serde_json::to_vec(&wire.data).expect("JSON Value 序列化不会失败"),
        metadata: serde_json::to_vec(&wire.metadata).expect("JSON Value 序列化不会失败"),
        event_id: wire.event_id.unwrap_or_else(Uuid::new_v4),
        expected_version,
    })
}

/// 消费结算 envelope。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettlementEnvelope {
    pub settlements: Vec<Settlement>,
}

/// 单条消费结算。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settlement {
    pub delivery_id: Vec<u8>,
    pub action: SettlementAction,
    pub reason: String,
}

/// 消费结算动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SettlementAction {
    Ack,
    Retry,
    Park,
    Skip,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSettlementEnvelope {
    settlements: Vec<WireSettlement>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSettlement {
    delivery_id: String,
    action: SettlementAction,
    #[serde(default)]
    reason: String,
}

/// 解析一个完整结算 envelope。
///
/// # 参数
/// `bytes` 是 JSON；`max_bytes` 是句柄输入上限。
///
/// # 返回
/// 返回至少一条已解码 opaque token 的结算。
///
/// # 错误
/// JSON、字段、动作或十六进制 token 非法时返回 [`CodecError`]。
pub fn parse_settlements(bytes: &[u8], max_bytes: usize) -> Result<SettlementEnvelope, CodecError> {
    check_size(bytes, max_bytes)?;
    let wire: WireSettlementEnvelope = serde_json::from_slice(bytes).map_err(CodecError::Json)?;
    if wire.settlements.is_empty() {
        return Err(CodecError::InvalidField("settlements"));
    }
    let settlements = wire
        .settlements
        .into_iter()
        .map(|settlement| {
            Ok(Settlement {
                delivery_id: decode_hex(&settlement.delivery_id)?,
                action: settlement.action,
                reason: settlement.reason,
            })
        })
        .collect::<Result<_, CodecError>>()?;
    Ok(SettlementEnvelope { settlements })
}

/// 编码公开事件 frame，不包含物理位置。
pub fn event_frame(event: &AggregateEvent) -> Result<Vec<u8>, CodecError> {
    #[derive(Serialize)]
    struct Frame<'a> {
        kind: &'static str,
        aggregate_id: &'a str,
        aggregate_version: u64,
        event_id: String,
        event_type: &'a str,
        data: Value,
        metadata: Value,
    }
    json_line(&Frame {
        kind: "event",
        aggregate_id: &event.aggregate_id,
        aggregate_version: event.aggregate_version,
        event_id: hex(&event.event_id),
        event_type: &event.event_type,
        data: json_value(&event.data),
        metadata: json_value(&event.metadata),
    })
}

/// 编码消费者组 delivery frame。
pub fn delivery_frame(delivery: &AggregateGroupDelivery) -> Result<Vec<u8>, CodecError> {
    #[derive(Serialize)]
    struct Frame<'a> {
        kind: &'static str,
        delivery_id: String,
        attempt: u32,
        deadline_ms: u64,
        replayed: bool,
        aggregate_id: &'a str,
        aggregate_version: u64,
        event_id: String,
        event_type: &'a str,
        data: Value,
        metadata: Value,
    }
    let event = delivery.event.as_ref().ok_or(CodecError::MissingEvent)?;
    json_line(&Frame {
        kind: "delivery",
        delivery_id: hex(&delivery.delivery_id),
        attempt: delivery.attempt,
        deadline_ms: delivery.deadline_ms,
        replayed: delivery.replayed,
        aggregate_id: &event.aggregate_id,
        aggregate_version: event.aggregate_version,
        event_id: hex(&event.event_id),
        event_type: &event.event_type,
        data: json_value(&event.data),
        metadata: json_value(&event.metadata),
    })
}

/// 编码不携带业务事件的状态 frame。
pub fn status_frame(kind: &'static str, unavailable_sources: Option<u32>) -> Vec<u8> {
    let value = match unavailable_sources {
        Some(count) => serde_json::json!({
            "kind": kind,
            "unavailable_source_count": count,
            "retrying": true,
        }),
        None => serde_json::json!({"kind": kind}),
    };
    let mut bytes = serde_json::to_vec(&value).expect("JSON Value 序列化不会失败");
    bytes.push(b'\n');
    bytes
}

fn json_line(value: &impl Serialize) -> Result<Vec<u8>, CodecError> {
    let mut bytes = serde_json::to_vec(value).map_err(CodecError::Json)?;
    bytes.push(b'\n');
    Ok(bytes)
}

fn json_value(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into()))
}

fn check_size(bytes: &[u8], max_bytes: usize) -> Result<(), CodecError> {
    if bytes.len() > max_bytes {
        Err(CodecError::TooLarge)
    } else {
        Ok(())
    }
}

fn decode_hex(value: &str) -> Result<Vec<u8>, CodecError> {
    if value.is_empty() || !value.len().is_multiple_of(2) {
        return Err(CodecError::InvalidToken);
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16).map_err(|_| CodecError::InvalidToken)
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// JSON codec 失败原因。
#[derive(Debug)]
pub enum CodecError {
    TooLarge,
    Json(serde_json::Error),
    UnsupportedSpec(String),
    InvalidField(&'static str),
    InvalidToken,
    MissingEvent,
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for CodecError {}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn event_is_strict_and_compacts_payload() {
        let event = parse_event(
            br#"{
                "spec_version":"1.0",
                "aggregate_id":"order-1",
                "event_type":"pay",
                "data":{"amount": 50},
                "expected_version":{"kind":"exact","version":0}
            }"#,
            1024,
        )
        .unwrap();
        assert_eq!(event.data, br#"{"amount":50}"#);
        assert_eq!(event.metadata, b"{}");
        assert_eq!(event.expected_version, ExpectedVersion::Exact(0));

        assert!(
            parse_event(
                br#"{"spec_version":"2.0","aggregate_id":"a","event_type":"e","data":{}}"#,
                1024
            )
            .is_err()
        );
        assert!(parse_event(br#"{"spec_version":"1.0","aggregate_id":"a","aggregate_id":"b","event_type":"e","data":{}}"#, 1024).is_err());
        assert!(parse_event(br#"{"spec_version":"1.0","aggregate_id":"a","event_type":"e","data":{},"extra":1}"#, 1024).is_err());
    }

    #[test]
    fn settlements_require_nonempty_hex_tokens() {
        let value = parse_settlements(
            br#"{"settlements":[{"delivery_id":"00ff","action":"retry","reason":"later"}]}"#,
            1024,
        )
        .unwrap();
        assert_eq!(value.settlements[0].delivery_id, vec![0, 255]);
        assert_eq!(value.settlements[0].action, SettlementAction::Retry);
        assert!(parse_settlements(br#"{"settlements":[]}"#, 1024).is_err());
        assert!(
            parse_settlements(
                br#"{"settlements":[{"delivery_id":"x","action":"ack"}]}"#,
                1024
            )
            .is_err()
        );
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
            let _ = parse_event(&bytes, 1024);
            let _ = parse_settlements(&bytes, 1024);
        }
    }
}
