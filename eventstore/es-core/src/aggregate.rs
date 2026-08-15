//! AggregateStore 领域模型：事件集、实例版本、虚拟分区与业务状态。

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::{Hlc, Result};

/// 首期事件集固定使用的虚拟事件分区数。
pub const EVENT_PARTITION_COUNT: u16 = 256;

const MAX_IDENTIFIER_BYTES: usize = 128;
const RESERVED_IDENTIFIERS: [&str; 3] = ["events.jsonl", "states", "groups"];

/// 校验文件路径和 AggregateStore 公共接口共用的标识符。
///
/// - `kind`：错误信息中的字段名称。
/// - `value`：待校验的 ASCII 标识符。
/// - 返回：校验成功时返回 `Ok(())`。
/// - 错误：空值、超长、保留名称或非法字符返回 `InvalidInput`。
pub fn validate_aggregate_identifier(kind: &str, value: &str) -> Result<()> {
    let bytes = value.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_IDENTIFIER_BYTES {
        return Err(crate::Error::InvalidInput(format!(
            "{kind} 长度必须为 1..={MAX_IDENTIFIER_BYTES} 字节"
        )));
    }
    if RESERVED_IDENTIFIERS.contains(&value) {
        return Err(crate::Error::InvalidInput(format!(
            "{kind} 不能使用保留名称 {value}"
        )));
    }
    let first = bytes[0];
    if !first.is_ascii_alphanumeric()
        || bytes
            .iter()
            .any(|byte| !(byte.is_ascii_alphanumeric() || b"._-".contains(byte)))
    {
        return Err(crate::Error::InvalidInput(format!(
            "{kind} 必须匹配 [A-Za-z0-9][A-Za-z0-9._-]{{0,127}}"
        )));
    }
    Ok(())
}

/// 聚合类型事件集身份，由业务空间和聚合根类型共同构成。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventSetId {
    business_space: String,
    aggregate_type: String,
}

impl EventSetId {
    /// 构造并校验事件集身份。
    ///
    /// - `business_space`：业务空间，例如 `orders`。
    /// - `aggregate_type`：聚合根类型，例如 `order`。
    /// - 返回：合法的事件集身份。
    /// - 错误：任一段不符合公共标识符规则时返回 `InvalidInput`。
    pub fn new(
        business_space: impl Into<String>,
        aggregate_type: impl Into<String>,
    ) -> Result<Self> {
        let business_space = business_space.into();
        let aggregate_type = aggregate_type.into();
        validate_aggregate_identifier("business_space", &business_space)?;
        validate_aggregate_identifier("aggregate_type", &aggregate_type)?;
        Ok(Self {
            business_space,
            aggregate_type,
        })
    }

    /// 重新校验从持久化或网络反序列化得到的事件集身份。
    ///
    /// - 返回：两段均符合公共标识符规则时为 `Ok(())`。
    /// - 错误：发现旧数据或恶意输入绕过构造器时返回 `InvalidInput`。
    pub fn validate(&self) -> Result<()> {
        validate_aggregate_identifier("business_space", &self.business_space)?;
        validate_aggregate_identifier("aggregate_type", &self.aggregate_type)
    }

    /// 返回业务空间。
    pub fn business_space(&self) -> &str {
        &self.business_space
    }

    /// 返回聚合根类型。
    pub fn aggregate_type(&self) -> &str {
        &self.aggregate_type
    }

    /// 返回稳定的 `business_space/aggregate_type` 编码。
    pub fn canonical_name(&self) -> String {
        format!("{}/{}", self.business_space, self.aggregate_type)
    }
}

impl fmt::Display for EventSetId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.business_space, self.aggregate_type)
    }
}

impl FromStr for EventSetId {
    type Err = crate::Error;

    /// 从严格的 `business_space/aggregate_type` 形式解析事件集身份。
    fn from_str(value: &str) -> Result<Self> {
        let mut parts = value.split('/');
        let business_space = parts.next().unwrap_or_default();
        let aggregate_type = parts.next().unwrap_or_default();
        if parts.next().is_some() || business_space.is_empty() || aggregate_type.is_empty() {
            return Err(crate::Error::InvalidInput(
                "事件集身份必须是 business_space/aggregate_type".into(),
            ));
        }
        Self::new(business_space, aggregate_type)
    }
}

/// 持久化的分区哈希算法标识；新增算法必须追加变体。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventPartitionHash {
    /// `xxh3(seed || aggregate_id) % partition_count`。
    Xxh3V1,
}

impl EventPartitionHash {
    /// 为聚合实例计算稳定的虚拟事件分区。
    ///
    /// - `seed`：事件集创建时持久化的 128 位随机种子。
    /// - `aggregate_id`：聚合根实例 ID。
    /// - `partition_count`：事件集不可变分区数。
    /// - 返回：`0..partition_count` 内的分区编号。
    /// - 错误：实例 ID 非法或分区数为零时返回 `InvalidInput`。
    pub fn partition(
        self,
        seed: &[u8; 16],
        aggregate_id: &str,
        partition_count: u16,
    ) -> Result<u16> {
        validate_aggregate_identifier("aggregate_id", aggregate_id)?;
        if partition_count == 0 {
            return Err(crate::Error::InvalidInput(
                "partition_count 必须大于 0".into(),
            ));
        }
        let hash = match self {
            Self::Xxh3V1 => {
                let mut hasher = Xxh3::new();
                hasher.update(seed);
                hasher.update(aggregate_id.as_bytes());
                hasher.digest()
            }
        };
        Ok((hash % u64::from(partition_count)) as u16)
    }
}

/// 待写入的聚合事件；版本、分区位置和 HLC 由服务端分配。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewAggregateEvent {
    pub event_id: Uuid,
    pub event_type: String,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
}

/// 已持久化的聚合事件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateEvent {
    pub event_set: EventSetId,
    pub partition_id: u16,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_id: Uuid,
    pub event_type: String,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub hlc: Hlc,
    /// 仅用于内部游标和消费者进度，不属于生产者输入。
    pub partition_position: u64,
}

/// 聚合实例追加时的乐观并发条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedAggregateVersion {
    /// 不校验当前版本。
    Any,
    /// 要求聚合实例尚无事件。
    NoAggregate,
    /// 要求聚合实例已经存在。
    AggregateExists,
    /// 要求当前聚合版本恰好等于给定值。
    Exact(u64),
}

/// 单个聚合实例的事件元数据。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateMeta {
    pub current_version: u64,
}

/// 业务状态文档写入时的 revision 条件。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedStateRevision {
    /// 要求状态文档尚不存在。
    Absent,
    /// 要求当前 revision 恰好等于给定值。
    Exact(u64),
}

/// 已持久化的业务状态文档。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateState {
    pub revision: u64,
    pub data: Vec<u8>,
}

/// 带最后提交时间的业务状态文档。
///
/// 状态内容和修改时间在同一次 Raft apply 与存储事务中提交。旧版本状态没有时间
/// 元数据时使用 `Hlc { wall: 0, logical: 0 }`，调用方应映射为 Unix epoch。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateStateDocument {
    /// 状态 CAS revision。
    pub revision: u64,
    /// 原始业务 JSON 字节。
    pub data: Vec<u8>,
    /// 服务端提交时分配的混合逻辑时钟。
    pub modified_hlc: Hlc,
}

/// 聚合事件追加成功后的稳定结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateAppendResult {
    pub aggregate_version: u64,
    pub partition_position: u64,
}

/// 计算追加请求的稳定幂等指纹。
///
/// - 参数包含事件集、实例、期望版本和完整事件内容。
/// - 返回：基于 XXH3-128 的稳定指纹，用于识别同一 `event_id` 的内容冲突。
/// - 错误：本函数不失败；调用方仍须独立校验公共输入。
pub fn aggregate_append_fingerprint(
    event_set: &EventSetId,
    aggregate_id: &str,
    expected: ExpectedAggregateVersion,
    event: &NewAggregateEvent,
) -> u128 {
    fn update_bytes(hasher: &mut Xxh3, bytes: &[u8]) {
        hasher.update(&(bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }

    let mut hasher = Xxh3::new();
    update_bytes(&mut hasher, event_set.business_space().as_bytes());
    update_bytes(&mut hasher, event_set.aggregate_type().as_bytes());
    update_bytes(&mut hasher, aggregate_id.as_bytes());
    match expected {
        ExpectedAggregateVersion::Any => hasher.update(&[0]),
        ExpectedAggregateVersion::NoAggregate => hasher.update(&[1]),
        ExpectedAggregateVersion::AggregateExists => hasher.update(&[2]),
        ExpectedAggregateVersion::Exact(version) => {
            hasher.update(&[3]);
            hasher.update(&version.to_be_bytes());
        }
    }
    hasher.update(event.event_id.as_bytes());
    update_bytes(&mut hasher, event.event_type.as_bytes());
    update_bytes(&mut hasher, &event.data);
    update_bytes(&mut hasher, &event.metadata);
    hasher.digest128()
}

/// 聚合类型事件集生命周期。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EventSetStatus {
    /// catalog 已提交，所有分区 fence 尚未确认安装。
    Creating,
    /// 全部分区可路由和写入。
    Active,
}

/// 正在进行的事件分区迁移。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionMove {
    pub operation_id: Uuid,
    pub source_shard: u64,
    pub target_shard: u64,
    pub next_generation: u64,
}

/// 单个虚拟事件分区的权威放置。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionPlacement {
    pub shard_id: u64,
    pub generation: u64,
    pub pending_move: Option<PartitionMove>,
    pub last_completed_operation: Option<Uuid>,
}

/// catalog 中的聚合类型事件集定义。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateEventSet {
    pub id: EventSetId,
    pub create_operation_id: Uuid,
    /// 原始创建请求的稳定指纹；后续迁移不能改变创建重试判定。
    pub create_plan_fingerprint: u128,
    pub seed: [u8; 16],
    pub partition_count: u16,
    pub hash_algorithm: EventPartitionHash,
    pub status: EventSetStatus,
    pub placements: BTreeMap<u16, PartitionPlacement>,
}

impl AggregateEventSet {
    /// 计算聚合实例所属分区。
    ///
    /// - `aggregate_id`：合法聚合实例 ID。
    /// - 返回：稳定分区编号。
    /// - 错误：实例 ID 或事件集分区配置非法时返回 `InvalidInput`。
    pub fn partition_for(&self, aggregate_id: &str) -> Result<u16> {
        self.hash_algorithm
            .partition(&self.seed, aggregate_id, self.partition_count)
    }

    fn create(
        id: EventSetId,
        operation_id: Uuid,
        seed: [u8; 16],
        placements: &BTreeMap<u16, u64>,
    ) -> std::result::Result<Self, String> {
        if placements.len() != usize::from(EVENT_PARTITION_COUNT)
            || (0..EVENT_PARTITION_COUNT).any(|partition| !placements.contains_key(&partition))
        {
            return Err(format!(
                "事件集必须完整提供 {EVENT_PARTITION_COUNT} 个分区放置"
            ));
        }
        let create_plan_fingerprint = create_plan_fingerprint(&seed, placements);
        let placements = placements
            .iter()
            .map(|(partition, shard_id)| {
                (
                    *partition,
                    PartitionPlacement {
                        shard_id: *shard_id,
                        generation: 1,
                        pending_move: None,
                        last_completed_operation: None,
                    },
                )
            })
            .collect();
        Ok(Self {
            id,
            create_operation_id: operation_id,
            create_plan_fingerprint,
            seed,
            partition_count: EVENT_PARTITION_COUNT,
            hash_algorithm: EventPartitionHash::Xxh3V1,
            status: EventSetStatus::Creating,
            placements,
        })
    }
}

/// 控制 Shard 持久化的事件集 catalog。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateCatalog {
    pub revision: u64,
    pub event_sets: BTreeMap<EventSetId, AggregateEventSet>,
}

/// catalog 的线性化变更命令。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateCatalogCommand {
    /// 创建处于 `Creating` 状态的事件集定义和固定放置计划。
    Create {
        event_set: EventSetId,
        operation_id: Uuid,
        seed: [u8; 16],
        placements: BTreeMap<u16, u64>,
    },
    /// 全部分区安装初始 fence 后激活事件集。
    Activate {
        event_set: EventSetId,
        operation_id: Uuid,
    },
    /// 条件准备迁移一个虚拟事件分区。
    PrepareMove {
        event_set: EventSetId,
        partition_id: u16,
        expected_generation: u64,
        target_shard: u64,
        operation_id: Uuid,
    },
    /// 目标复制、追尾及双方新 fence 安装完成后切换权威归属。
    CompleteMove {
        event_set: EventSetId,
        partition_id: u16,
        operation_id: Uuid,
    },
}

/// catalog 命令结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateCatalogOutcome {
    /// 返回当前定义；`changed` 表示本次命令是否推进 revision。
    EventSet {
        event_set: AggregateEventSet,
        changed: bool,
    },
    /// 目标不存在。
    NotFound,
    /// 条件或幂等操作 ID 与当前状态冲突。
    Conflict { reason: String },
    /// 命令违反 catalog 不变量。
    Invalid { reason: String },
}

/// catalog 命令的完整应用结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateCatalogApply {
    pub revision: u64,
    pub outcome: AggregateCatalogOutcome,
}

impl AggregateCatalog {
    /// 在控制 Shard 串行应用事件集 catalog 命令。
    ///
    /// - `command`：携带 CAS generation 和幂等 operation ID 的变更。
    /// - 返回：当前 catalog revision 与命令结果；业务冲突不会修改状态。
    /// - 错误：该纯领域转换不返回存储错误，非法输入通过 `Invalid` 表达。
    pub fn apply(&mut self, command: AggregateCatalogCommand) -> AggregateCatalogApply {
        let outcome = match command {
            AggregateCatalogCommand::Create {
                event_set,
                operation_id,
                seed,
                placements,
            } => match event_set.validate() {
                Err(error) => AggregateCatalogOutcome::Invalid {
                    reason: error.to_string(),
                },
                Ok(()) => match self.event_sets.get(&event_set) {
                    Some(existing)
                        if existing.create_operation_id == operation_id
                            && existing.create_plan_fingerprint
                                == create_plan_fingerprint(&seed, &placements) =>
                    {
                        AggregateCatalogOutcome::EventSet {
                            event_set: existing.clone(),
                            changed: false,
                        }
                    }
                    Some(_) => AggregateCatalogOutcome::Conflict {
                        reason: "事件集已由另一创建操作定义".into(),
                    },
                    None => match AggregateEventSet::create(
                        event_set.clone(),
                        operation_id,
                        seed,
                        &placements,
                    ) {
                        Ok(created) => {
                            self.revision += 1;
                            self.event_sets.insert(event_set, created.clone());
                            AggregateCatalogOutcome::EventSet {
                                event_set: created,
                                changed: true,
                            }
                        }
                        Err(reason) => AggregateCatalogOutcome::Invalid { reason },
                    },
                },
            },
            AggregateCatalogCommand::Activate {
                event_set,
                operation_id,
            } => match self.event_sets.get_mut(&event_set) {
                None => AggregateCatalogOutcome::NotFound,
                Some(existing) if existing.create_operation_id != operation_id => {
                    AggregateCatalogOutcome::Conflict {
                        reason: "激活操作与创建 operation_id 不一致".into(),
                    }
                }
                Some(existing) if existing.status == EventSetStatus::Active => {
                    AggregateCatalogOutcome::EventSet {
                        event_set: existing.clone(),
                        changed: false,
                    }
                }
                Some(existing) => {
                    existing.status = EventSetStatus::Active;
                    let event_set = existing.clone();
                    self.revision += 1;
                    AggregateCatalogOutcome::EventSet {
                        event_set,
                        changed: true,
                    }
                }
            },
            AggregateCatalogCommand::PrepareMove {
                event_set,
                partition_id,
                expected_generation,
                target_shard,
                operation_id,
            } => self.prepare_move(
                &event_set,
                partition_id,
                expected_generation,
                target_shard,
                operation_id,
            ),
            AggregateCatalogCommand::CompleteMove {
                event_set,
                partition_id,
                operation_id,
            } => self.complete_move(&event_set, partition_id, operation_id),
        };
        AggregateCatalogApply {
            revision: self.revision,
            outcome,
        }
    }

    fn prepare_move(
        &mut self,
        event_set_id: &EventSetId,
        partition_id: u16,
        expected_generation: u64,
        target_shard: u64,
        operation_id: Uuid,
    ) -> AggregateCatalogOutcome {
        let Some(event_set) = self.event_sets.get_mut(event_set_id) else {
            return AggregateCatalogOutcome::NotFound;
        };
        if event_set.status != EventSetStatus::Active {
            return AggregateCatalogOutcome::Invalid {
                reason: "只有 Active 事件集可以迁移分区".into(),
            };
        }
        let Some(placement) = event_set.placements.get_mut(&partition_id) else {
            return AggregateCatalogOutcome::Invalid {
                reason: "partition_id 超出事件集范围".into(),
            };
        };
        if let Some(pending) = &placement.pending_move {
            if pending.operation_id == operation_id
                && pending.target_shard == target_shard
                && pending.source_shard == placement.shard_id
            {
                return AggregateCatalogOutcome::EventSet {
                    event_set: event_set.clone(),
                    changed: false,
                };
            }
            return AggregateCatalogOutcome::Conflict {
                reason: "该分区已有迁移操作".into(),
            };
        }
        if placement.generation != expected_generation {
            return AggregateCatalogOutcome::Conflict {
                reason: format!(
                    "分区 generation 已变化: expected={expected_generation}, actual={}",
                    placement.generation
                ),
            };
        }
        if placement.shard_id == target_shard {
            return AggregateCatalogOutcome::Invalid {
                reason: "迁移目标不能是当前 Shard".into(),
            };
        }
        let Some(next_generation) = placement.generation.checked_add(1) else {
            return AggregateCatalogOutcome::Invalid {
                reason: "分区 generation 已耗尽".into(),
            };
        };
        placement.pending_move = Some(PartitionMove {
            operation_id,
            source_shard: placement.shard_id,
            target_shard,
            next_generation,
        });
        self.revision += 1;
        AggregateCatalogOutcome::EventSet {
            event_set: event_set.clone(),
            changed: true,
        }
    }

    fn complete_move(
        &mut self,
        event_set_id: &EventSetId,
        partition_id: u16,
        operation_id: Uuid,
    ) -> AggregateCatalogOutcome {
        let Some(event_set) = self.event_sets.get_mut(event_set_id) else {
            return AggregateCatalogOutcome::NotFound;
        };
        let Some(placement) = event_set.placements.get_mut(&partition_id) else {
            return AggregateCatalogOutcome::Invalid {
                reason: "partition_id 超出事件集范围".into(),
            };
        };
        let Some(pending) = placement.pending_move.clone() else {
            return if placement.last_completed_operation == Some(operation_id) {
                AggregateCatalogOutcome::EventSet {
                    event_set: event_set.clone(),
                    changed: false,
                }
            } else {
                AggregateCatalogOutcome::Conflict {
                    reason: "该分区没有匹配的待完成迁移".into(),
                }
            };
        };
        if pending.operation_id != operation_id {
            return AggregateCatalogOutcome::Conflict {
                reason: "完成操作与待迁移 operation_id 不一致".into(),
            };
        }
        placement.shard_id = pending.target_shard;
        placement.generation = pending.next_generation;
        placement.pending_move = None;
        placement.last_completed_operation = Some(operation_id);
        self.revision += 1;
        AggregateCatalogOutcome::EventSet {
            event_set: event_set.clone(),
            changed: true,
        }
    }
}

fn create_plan_fingerprint(seed: &[u8; 16], placements: &BTreeMap<u16, u64>) -> u128 {
    let mut hasher = Xxh3::new();
    hasher.update(seed);
    hasher.update(&(placements.len() as u64).to_be_bytes());
    for (partition_id, shard_id) in placements {
        hasher.update(&partition_id.to_be_bytes());
        hasher.update(&shard_id.to_be_bytes());
    }
    hasher.digest128()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn event_set() -> EventSetId {
        EventSetId::new("orders", "order").expect("合法事件集")
    }

    fn placements() -> BTreeMap<u16, u64> {
        (0..EVENT_PARTITION_COUNT)
            .map(|partition| (partition, u64::from(partition % 4)))
            .collect()
    }

    fn create_command(operation_id: Uuid) -> AggregateCatalogCommand {
        AggregateCatalogCommand::Create {
            event_set: event_set(),
            operation_id,
            seed: [7; 16],
            placements: placements(),
        }
    }

    #[test]
    fn event_set_id_roundtrip_and_validation() {
        let id = event_set();
        assert_eq!(id.business_space(), "orders");
        assert_eq!(id.aggregate_type(), "order");
        assert_eq!(id.to_string(), "orders/order");
        assert_eq!(id.to_string().parse::<EventSetId>().unwrap(), id);

        for invalid in ["", "/order", "orders/", "a/b/c", "orders/订单"] {
            assert!(invalid.parse::<EventSetId>().is_err(), "{invalid}");
        }
        for reserved in RESERVED_IDENTIFIERS {
            assert!(EventSetId::new("orders", reserved).is_err());
        }
        let bypassed: EventSetId =
            serde_json::from_str(r#"{"business_space":"orders/bad","aggregate_type":"order"}"#)
                .expect("serde 可读取旧持久化形态");
        assert!(bypassed.validate().is_err(), "反序列化后仍必须重新校验");
    }

    #[test]
    fn partition_is_stable_and_seeded() {
        let hash = EventPartitionHash::Xxh3V1;
        let first = hash.partition(&[1; 16], "order-1", 256).unwrap();
        assert_eq!(first, hash.partition(&[1; 16], "order-1", 256).unwrap());
        assert_ne!(first, hash.partition(&[2; 16], "order-1", 256).unwrap());
        assert!(first < 256);
    }

    #[test]
    fn identifier_and_partition_reject_boundary_inputs() {
        assert!(validate_aggregate_identifier("id", "").is_err());
        assert!(validate_aggregate_identifier("id", &"a".repeat(129)).is_err());
        assert!(validate_aggregate_identifier("id", "_leading").is_err());
        assert!(validate_aggregate_identifier("id", "bad/name").is_err());
        assert!(
            EventPartitionHash::Xxh3V1
                .partition(&[0; 16], "order-1", 0)
                .is_err()
        );
    }

    #[test]
    fn append_fingerprint_covers_request_content() {
        let base = NewAggregateEvent {
            event_id: Uuid::from_u128(1),
            event_type: "created".into(),
            data: b"{}".to_vec(),
            metadata: b"{}".to_vec(),
        };
        let fingerprint = aggregate_append_fingerprint(
            &event_set(),
            "order-1",
            ExpectedAggregateVersion::NoAggregate,
            &base,
        );
        let mut changed = base.clone();
        changed.data = b"{\"amount\":1}".to_vec();
        assert_ne!(
            fingerprint,
            aggregate_append_fingerprint(
                &event_set(),
                "order-1",
                ExpectedAggregateVersion::NoAggregate,
                &changed
            )
        );
        assert_ne!(
            fingerprint,
            aggregate_append_fingerprint(
                &event_set(),
                "order-1",
                ExpectedAggregateVersion::Any,
                &base
            )
        );
        let exists = aggregate_append_fingerprint(
            &event_set(),
            "order-1",
            ExpectedAggregateVersion::AggregateExists,
            &base,
        );
        let exact = aggregate_append_fingerprint(
            &event_set(),
            "order-1",
            ExpectedAggregateVersion::Exact(3),
            &base,
        );
        assert_ne!(fingerprint, exists);
        assert_ne!(exists, exact);
    }

    #[test]
    fn catalog_create_activate_and_retry_are_idempotent() {
        let operation_id = Uuid::new_v4();
        let mut catalog = AggregateCatalog::default();
        let created = catalog.apply(create_command(operation_id));
        assert_eq!(created.revision, 1);
        assert!(matches!(
            created.outcome,
            AggregateCatalogOutcome::EventSet { changed: true, .. }
        ));
        let retried = catalog.apply(create_command(operation_id));
        assert_eq!(retried.revision, 1);
        assert!(matches!(
            retried.outcome,
            AggregateCatalogOutcome::EventSet { changed: false, .. }
        ));
        let activated = catalog.apply(AggregateCatalogCommand::Activate {
            event_set: event_set(),
            operation_id,
        });
        assert_eq!(activated.revision, 2);
        assert!(matches!(
            activated.outcome,
            AggregateCatalogOutcome::EventSet {
                event_set: AggregateEventSet {
                    status: EventSetStatus::Active,
                    ..
                },
                changed: true
            }
        ));
    }

    #[test]
    fn catalog_rejects_incomplete_placement_without_mutation() {
        let mut catalog = AggregateCatalog::default();
        let result = catalog.apply(AggregateCatalogCommand::Create {
            event_set: event_set(),
            operation_id: Uuid::new_v4(),
            seed: [0; 16],
            placements: BTreeMap::from([(0, 1)]),
        });
        assert_eq!(catalog.revision, 0);
        assert!(catalog.event_sets.is_empty());
        assert!(matches!(
            result.outcome,
            AggregateCatalogOutcome::Invalid { .. }
        ));
    }

    #[test]
    fn partition_move_uses_generation_cas_and_idempotent_completion() {
        let create_id = Uuid::new_v4();
        let move_id = Uuid::new_v4();
        let mut catalog = AggregateCatalog::default();
        catalog.apply(create_command(create_id));
        catalog.apply(AggregateCatalogCommand::Activate {
            event_set: event_set(),
            operation_id: create_id,
        });
        let prepared = catalog.apply(AggregateCatalogCommand::PrepareMove {
            event_set: event_set(),
            partition_id: 3,
            expected_generation: 1,
            target_shard: 9,
            operation_id: move_id,
        });
        assert_eq!(prepared.revision, 3);
        let completed = catalog.apply(AggregateCatalogCommand::CompleteMove {
            event_set: event_set(),
            partition_id: 3,
            operation_id: move_id,
        });
        assert_eq!(completed.revision, 4);
        let placement = &catalog.event_sets[&event_set()].placements[&3];
        assert_eq!((placement.shard_id, placement.generation), (9, 2));
        assert!(placement.pending_move.is_none());

        let retried = catalog.apply(AggregateCatalogCommand::CompleteMove {
            event_set: event_set(),
            partition_id: 3,
            operation_id: move_id,
        });
        assert_eq!(retried.revision, 4);
        assert!(matches!(
            retried.outcome,
            AggregateCatalogOutcome::EventSet { changed: false, .. }
        ));

        let create_retry = catalog.apply(create_command(create_id));
        assert_eq!(create_retry.revision, 4);
        assert!(matches!(
            create_retry.outcome,
            AggregateCatalogOutcome::EventSet { changed: false, .. }
        ));
    }

    #[test]
    fn catalog_create_and_activate_reject_conflicting_operations() {
        let create_id = Uuid::new_v4();
        let mut catalog = AggregateCatalog::default();

        let invalid_id = EventSetId {
            business_space: "bad/path".into(),
            aggregate_type: "order".into(),
        };
        let invalid = catalog.apply(AggregateCatalogCommand::Create {
            event_set: invalid_id,
            operation_id: create_id,
            seed: [0; 16],
            placements: placements(),
        });
        assert!(matches!(
            invalid.outcome,
            AggregateCatalogOutcome::Invalid { .. }
        ));
        assert_eq!(catalog.revision, 0);

        assert!(matches!(
            catalog
                .apply(AggregateCatalogCommand::Activate {
                    event_set: event_set(),
                    operation_id: create_id,
                })
                .outcome,
            AggregateCatalogOutcome::NotFound
        ));
        catalog.apply(create_command(create_id));

        let conflicting_create = catalog.apply(create_command(Uuid::new_v4()));
        assert!(matches!(
            conflicting_create.outcome,
            AggregateCatalogOutcome::Conflict { .. }
        ));
        let mut changed_plan = placements();
        changed_plan.insert(0, 99);
        let changed_plan = catalog.apply(AggregateCatalogCommand::Create {
            event_set: event_set(),
            operation_id: create_id,
            seed: [7; 16],
            placements: changed_plan,
        });
        assert!(matches!(
            changed_plan.outcome,
            AggregateCatalogOutcome::Conflict { .. }
        ));

        let wrong_activate = catalog.apply(AggregateCatalogCommand::Activate {
            event_set: event_set(),
            operation_id: Uuid::new_v4(),
        });
        assert!(matches!(
            wrong_activate.outcome,
            AggregateCatalogOutcome::Conflict { .. }
        ));
        catalog.apply(AggregateCatalogCommand::Activate {
            event_set: event_set(),
            operation_id: create_id,
        });
        let retry = catalog.apply(AggregateCatalogCommand::Activate {
            event_set: event_set(),
            operation_id: create_id,
        });
        assert!(matches!(
            retry.outcome,
            AggregateCatalogOutcome::EventSet { changed: false, .. }
        ));
        assert_eq!(catalog.revision, 2);
    }

    #[test]
    fn partition_move_rejects_invalid_or_stale_transitions() {
        let create_id = Uuid::new_v4();
        let mut catalog = AggregateCatalog::default();
        catalog.apply(create_command(create_id));

        let prepare = |event_set, partition_id, expected_generation, target_shard, operation_id| {
            AggregateCatalogCommand::PrepareMove {
                event_set,
                partition_id,
                expected_generation,
                target_shard,
                operation_id,
            }
        };
        assert!(matches!(
            catalog
                .apply(prepare(event_set(), 0, 1, 9, Uuid::new_v4()))
                .outcome,
            AggregateCatalogOutcome::Invalid { .. }
        ));
        assert!(matches!(
            catalog
                .apply(prepare(
                    EventSetId::new("orders", "missing").unwrap(),
                    0,
                    1,
                    9,
                    Uuid::new_v4(),
                ))
                .outcome,
            AggregateCatalogOutcome::NotFound
        ));
        catalog.apply(AggregateCatalogCommand::Activate {
            event_set: event_set(),
            operation_id: create_id,
        });

        for command in [
            prepare(event_set(), EVENT_PARTITION_COUNT, 1, 9, Uuid::new_v4()),
            prepare(event_set(), 0, 2, 9, Uuid::new_v4()),
            prepare(event_set(), 0, 1, 0, Uuid::new_v4()),
        ] {
            assert!(!matches!(
                catalog.apply(command).outcome,
                AggregateCatalogOutcome::EventSet { changed: true, .. }
            ));
        }

        catalog
            .event_sets
            .get_mut(&event_set())
            .unwrap()
            .placements
            .get_mut(&1)
            .unwrap()
            .generation = u64::MAX;
        assert!(matches!(
            catalog
                .apply(prepare(event_set(), 1, u64::MAX, 9, Uuid::new_v4()))
                .outcome,
            AggregateCatalogOutcome::Invalid { .. }
        ));

        let move_id = Uuid::new_v4();
        catalog.apply(prepare(event_set(), 2, 1, 9, move_id));
        let retry = catalog.apply(prepare(event_set(), 2, 1, 9, move_id));
        assert!(matches!(
            retry.outcome,
            AggregateCatalogOutcome::EventSet { changed: false, .. }
        ));
        assert!(matches!(
            catalog
                .apply(prepare(event_set(), 2, 1, 10, Uuid::new_v4()))
                .outcome,
            AggregateCatalogOutcome::Conflict { .. }
        ));

        assert!(matches!(
            catalog
                .apply(AggregateCatalogCommand::CompleteMove {
                    event_set: EventSetId::new("orders", "missing").unwrap(),
                    partition_id: 0,
                    operation_id: move_id,
                })
                .outcome,
            AggregateCatalogOutcome::NotFound
        ));
        assert!(matches!(
            catalog
                .apply(AggregateCatalogCommand::CompleteMove {
                    event_set: event_set(),
                    partition_id: EVENT_PARTITION_COUNT,
                    operation_id: move_id,
                })
                .outcome,
            AggregateCatalogOutcome::Invalid { .. }
        ));
        assert!(matches!(
            catalog
                .apply(AggregateCatalogCommand::CompleteMove {
                    event_set: event_set(),
                    partition_id: 3,
                    operation_id: move_id,
                })
                .outcome,
            AggregateCatalogOutcome::Conflict { .. }
        ));
        assert!(matches!(
            catalog
                .apply(AggregateCatalogCommand::CompleteMove {
                    event_set: event_set(),
                    partition_id: 2,
                    operation_id: Uuid::new_v4(),
                })
                .outcome,
            AggregateCatalogOutcome::Conflict { .. }
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_valid_ids_always_route_in_range(
            suffix in "[A-Za-z0-9._-]{0,40}",
            seed in any::<[u8; 16]>()
        ) {
            let aggregate_id = format!("a{suffix}");
            let partition = EventPartitionHash::Xxh3V1
                .partition(&seed, &aggregate_id, EVENT_PARTITION_COUNT)
                .unwrap();
            prop_assert!(partition < EVENT_PARTITION_COUNT);
        }
    }
}
