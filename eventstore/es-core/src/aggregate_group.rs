//! 聚合事件集消费者组领域模型。
//!
//! 控制 Shard 仅保存组定义；高频 checkpoint、delivery 与实例 lease 按虚拟
//! 事件分区保存。该拆分让组状态规模随固定分区数扩展，而不是集中到单个控制流。

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{EventSetId, Hlc, validate_aggregate_identifier};

/// 消费者组创建起点。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateGroupStart {
    /// 从各分区位置 0 开始。
    Beginning,
    /// 从创建时捕获的各分区 head 开始。
    Now,
}

/// 聚合消费者组额度、租约与重试参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupSettings {
    pub max_unacked_per_consumer: u32,
    pub max_unacked_per_group: u32,
    pub ack_timeout_ms: u64,
    pub max_retries: u32,
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
}

impl Default for AggregateGroupSettings {
    fn default() -> Self {
        Self {
            max_unacked_per_consumer: 128,
            max_unacked_per_group: 4096,
            ack_timeout_ms: 10_000,
            max_retries: 5,
            retry_min_ms: 100,
            retry_max_ms: 5_000,
        }
    }
}

impl AggregateGroupSettings {
    /// 校验消费者组额度与时间边界。
    ///
    /// # 返回
    /// 所有参数合法时返回 `Ok(())`。
    ///
    /// # 错误
    /// 额度为零、消费者额度大于组额度、租约为零或退避范围倒置时返回原因。
    pub fn validate(&self) -> Result<(), String> {
        if self.max_unacked_per_consumer == 0 || self.max_unacked_per_group == 0 {
            return Err("未确认额度必须大于 0".into());
        }
        if self.max_unacked_per_consumer > self.max_unacked_per_group {
            return Err("消费者未确认额度不能大于组额度".into());
        }
        if self.ack_timeout_ms == 0 {
            return Err("ack timeout 必须大于 0".into());
        }
        if self.retry_min_ms == 0 || self.retry_min_ms > self.retry_max_ms {
            return Err("重试退避要求 0 < min <= max".into());
        }
        Ok(())
    }

    /// 计算给定重试次数的指数退避，结果饱和到上限。
    pub fn retry_delay_ms(&self, attempt: u32) -> u64 {
        let shift = attempt.saturating_sub(1).min(62);
        self.retry_min_ms
            .saturating_mul(1u64 << shift)
            .min(self.retry_max_ms)
    }
}

/// 控制 Shard 保存的消费者组定义。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupDefinition {
    pub event_set: EventSetId,
    pub name: String,
    pub revision: u64,
    pub epoch: u64,
    pub start: AggregateGroupStart,
    /// 创建或 reset 时捕获的每分区 inclusive 起点。
    pub partition_starts: BTreeMap<u16, u64>,
    pub settings: AggregateGroupSettings,
    pub create_operation_id: Uuid,
    pub last_operation_id: Uuid,
}

impl AggregateGroupDefinition {
    fn validate(&self, partition_count: u16) -> Result<(), String> {
        self.event_set
            .validate()
            .map_err(|error| error.to_string())?;
        validate_aggregate_identifier("group_name", &self.name)
            .map_err(|error| error.to_string())?;
        self.settings.validate()?;
        if self.partition_starts.len() != usize::from(partition_count)
            || (0..partition_count).any(|partition| !self.partition_starts.contains_key(&partition))
        {
            return Err(format!("消费者组必须提供 {partition_count} 个分区起点"));
        }
        Ok(())
    }
}

/// 控制 Shard 上的消费者组 catalog。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupCatalog {
    pub revision: u64,
    pub groups: BTreeMap<(EventSetId, String), AggregateGroupDefinition>,
    /// 每个已删除组保留最后一次操作，保证删除 RPC 重试幂等。
    pub deleted_operations: BTreeMap<(EventSetId, String), Uuid>,
}

/// 消费者组 catalog 命令。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateGroupCatalogCommand {
    Create {
        definition: AggregateGroupDefinition,
        partition_count: u16,
    },
    Replace {
        definition: AggregateGroupDefinition,
        expected_revision: u64,
        partition_count: u16,
        /// 显式重置起点时提升 epoch；仅修改 settings 不丢弃消费进度。
        reset: bool,
    },
    Delete {
        event_set: EventSetId,
        name: String,
        expected_revision: u64,
        operation_id: Uuid,
    },
}

/// 消费者组 catalog 命令结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateGroupCatalogOutcome {
    Group(AggregateGroupDefinition),
    Deleted,
    NotFound,
    Conflict { actual_revision: Option<u64> },
    Invalid { reason: String },
}

/// 带 catalog revision 的消费者组命令结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupCatalogApply {
    pub revision: u64,
    pub outcome: AggregateGroupCatalogOutcome,
}

impl AggregateGroupCatalog {
    /// 串行应用消费者组定义变更。
    ///
    /// # 参数
    /// `command` 携带 revision CAS 与稳定 operation ID。
    ///
    /// # 返回
    /// 返回 catalog revision 和领域结果；业务冲突不修改状态。
    ///
    /// # 错误
    /// 纯领域转换不返回存储错误，非法输入通过 `Invalid` 表达。
    pub fn apply(&mut self, command: AggregateGroupCatalogCommand) -> AggregateGroupCatalogApply {
        let outcome = match command {
            AggregateGroupCatalogCommand::Create {
                mut definition,
                partition_count,
            } => {
                let key = (definition.event_set.clone(), definition.name.clone());
                match definition.validate(partition_count) {
                    Err(reason) => AggregateGroupCatalogOutcome::Invalid { reason },
                    Ok(()) => match self.groups.get(&key) {
                        Some(existing)
                            if existing.create_operation_id == definition.create_operation_id =>
                        {
                            AggregateGroupCatalogOutcome::Group(existing.clone())
                        }
                        Some(existing) => AggregateGroupCatalogOutcome::Conflict {
                            actual_revision: Some(existing.revision),
                        },
                        None => {
                            definition.revision = 1;
                            definition.epoch = 1;
                            definition.last_operation_id = definition.create_operation_id;
                            self.deleted_operations.remove(&key);
                            self.groups.insert(key, definition.clone());
                            self.revision = self.revision.saturating_add(1);
                            AggregateGroupCatalogOutcome::Group(definition)
                        }
                    },
                }
            }
            AggregateGroupCatalogCommand::Replace {
                mut definition,
                expected_revision,
                partition_count,
                reset,
            } => {
                let key = (definition.event_set.clone(), definition.name.clone());
                match definition.validate(partition_count) {
                    Err(reason) => AggregateGroupCatalogOutcome::Invalid { reason },
                    Ok(()) => match self.groups.get(&key) {
                        None => AggregateGroupCatalogOutcome::NotFound,
                        Some(existing)
                            if existing.last_operation_id == definition.last_operation_id =>
                        {
                            AggregateGroupCatalogOutcome::Group(existing.clone())
                        }
                        Some(existing) if existing.revision != expected_revision => {
                            AggregateGroupCatalogOutcome::Conflict {
                                actual_revision: Some(existing.revision),
                            }
                        }
                        Some(existing) => {
                            definition.create_operation_id = existing.create_operation_id;
                            definition.revision = existing.revision.saturating_add(1);
                            definition.epoch = if reset {
                                existing.epoch.saturating_add(1)
                            } else {
                                existing.epoch
                            };
                            self.groups.insert(key, definition.clone());
                            self.revision = self.revision.saturating_add(1);
                            AggregateGroupCatalogOutcome::Group(definition)
                        }
                    },
                }
            }
            AggregateGroupCatalogCommand::Delete {
                event_set,
                name,
                expected_revision,
                operation_id,
            } => {
                let key = (event_set, name);
                let invalid = key
                    .0
                    .validate()
                    .map_err(|error| error.to_string())
                    .and_then(|()| {
                        validate_aggregate_identifier("group_name", &key.1)
                            .map_err(|error| error.to_string())
                    });
                match (invalid, self.groups.get(&key)) {
                    (Err(reason), _) => AggregateGroupCatalogOutcome::Invalid { reason },
                    (Ok(()), None) if self.deleted_operations.get(&key) == Some(&operation_id) => {
                        AggregateGroupCatalogOutcome::Deleted
                    }
                    (Ok(()), None) => AggregateGroupCatalogOutcome::NotFound,
                    (Ok(()), Some(existing)) if existing.last_operation_id == operation_id => {
                        AggregateGroupCatalogOutcome::Deleted
                    }
                    (Ok(()), Some(existing)) if existing.revision != expected_revision => {
                        AggregateGroupCatalogOutcome::Conflict {
                            actual_revision: Some(existing.revision),
                        }
                    }
                    (Ok(()), Some(_)) => {
                        self.groups.remove(&key);
                        self.deleted_operations.insert(key, operation_id);
                        self.revision = self.revision.saturating_add(1);
                        AggregateGroupCatalogOutcome::Deleted
                    }
                }
            }
        };
        AggregateGroupCatalogApply {
            revision: self.revision,
            outcome,
        }
    }
}

/// 进入分区 claim 命令的事件引用；payload 仍从事件索引读取。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDeliveryCandidate {
    pub delivery_id: Uuid,
    pub partition_position: u64,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_id: Uuid,
    /// 事件 data 与 metadata 的总字节数，用于在状态机内执行批次额度。
    pub payload_bytes: u64,
    pub replayed: bool,
}

/// 分区内已提交、尚未结算的 delivery。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupDelivery {
    pub delivery_id: Uuid,
    pub consumer_id: String,
    pub partition_position: u64,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_id: Uuid,
    pub attempt: u32,
    pub deadline_ms: u64,
    pub group_epoch: u64,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupRetry {
    pub partition_position: u64,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_id: Uuid,
    pub attempt: u32,
    pub not_before_ms: u64,
    pub replayed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupParked {
    pub partition_position: u64,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_id: Uuid,
    pub attempts: u32,
    pub reason: String,
    pub parked_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateInstanceLease {
    pub consumer_id: String,
    pub group_epoch: u64,
    pub deadline_ms: u64,
}

/// 单个 `(事件集, group, partition)` 的高频消费状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateGroupPartition {
    pub epoch: u64,
    pub next_position: u64,
    pub resolved_gaps: BTreeSet<u64>,
    pub leases: BTreeMap<String, AggregateInstanceLease>,
    pub deliveries: BTreeMap<Uuid, AggregateGroupDelivery>,
    pub pending_retries: BTreeMap<u64, AggregateGroupRetry>,
    pub parked: BTreeMap<Uuid, AggregateGroupParked>,
}

/// 聚合 delivery 结算动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateSettlementAction {
    Ack,
    Retry,
    Park,
    Skip,
}

/// 聚合 delivery 结算输入。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateSettlement {
    pub delivery_id: Uuid,
    pub action: AggregateSettlementAction,
    pub reason: String,
}

/// 聚合 delivery 结算结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateSettlementResult {
    Applied,
    AlreadySettled,
    StaleLease,
    WrongConsumer,
}

impl AggregateGroupPartition {
    /// 创建指定 epoch 与 inclusive 起点的空分区进度。
    pub fn new(epoch: u64, next_position: u64) -> Self {
        Self {
            epoch,
            next_position,
            resolved_gaps: BTreeSet::new(),
            leases: BTreeMap::new(),
            deliveries: BTreeMap::new(),
            pending_retries: BTreeMap::new(),
            parked: BTreeMap::new(),
        }
    }

    /// 在组 epoch 更新后丢弃旧租约并从新起点重置分区状态。
    pub fn reset(&mut self, epoch: u64, next_position: u64) {
        *self = Self::new(epoch, next_position);
    }

    /// 返回组和指定消费者共同允许的剩余额度。
    pub fn available_credit(&self, consumer_id: &str, settings: &AggregateGroupSettings) -> u32 {
        let consumer_used = self
            .deliveries
            .values()
            .filter(|delivery| delivery.consumer_id == consumer_id)
            .count() as u32;
        settings
            .max_unacked_per_consumer
            .saturating_sub(consumer_used)
            .min(
                settings
                    .max_unacked_per_group
                    .saturating_sub(self.deliveries.len() as u32),
            )
    }

    /// 原子 claim 候选，保证同一聚合实例最多一个未结算 delivery。
    ///
    /// # 参数
    /// `max_claim` 和 `max_bytes` 是本次调用额度；被租约或重试状态过滤的候选不消耗额度。
    ///
    /// # 返回
    /// 返回成功领取的 delivery。为避免超大单事件永久饥饿，首个事件允许超过字节额度。
    ///
    /// # 错误
    /// 本方法不返回错误；非法消费者、时间或零额度会返回空结果。
    pub fn claim(
        &mut self,
        consumer_id: &str,
        now_ms: u64,
        deadline_ms: u64,
        settings: &AggregateGroupSettings,
        max_claim: u32,
        max_bytes: u64,
        candidates: Vec<AggregateDeliveryCandidate>,
    ) -> Vec<AggregateGroupDelivery> {
        if consumer_id.is_empty()
            || deadline_ms <= now_ms
            || max_claim == 0
            || max_bytes == 0
            || settings.validate().is_err()
        {
            return Vec::new();
        }
        self.expire(now_ms, settings);
        let mut credit = self.available_credit(consumer_id, settings).min(max_claim);
        let mut claimed_bytes = 0u64;
        let mut claimed = Vec::new();
        for candidate in candidates {
            if credit == 0 || candidate.partition_position < self.next_position {
                break;
            }
            if self.resolved_gaps.contains(&candidate.partition_position)
                || self.deliveries.contains_key(&candidate.delivery_id)
                || self
                    .deliveries
                    .values()
                    .any(|delivery| delivery.partition_position == candidate.partition_position)
            {
                continue;
            }
            let retry = self
                .pending_retries
                .get(&candidate.partition_position)
                .cloned();
            if let Some(retry) = &retry {
                if retry.not_before_ms > now_ms || retry.event_id != candidate.event_id {
                    continue;
                }
            } else if candidate.replayed {
                continue;
            }
            let blocked = self.deliveries.values().any(|delivery| {
                delivery.aggregate_id == candidate.aggregate_id
                    && delivery.partition_position != candidate.partition_position
            }) || self.pending_retries.values().any(|pending| {
                pending.aggregate_id == candidate.aggregate_id
                    && pending.partition_position != candidate.partition_position
            });
            if blocked {
                continue;
            }
            if self
                .leases
                .get(&candidate.aggregate_id)
                .is_some_and(|lease| lease.consumer_id != consumer_id && lease.deadline_ms > now_ms)
            {
                continue;
            }
            if !claimed.is_empty()
                && claimed_bytes.saturating_add(candidate.payload_bytes) > max_bytes
            {
                break;
            }
            self.pending_retries.remove(&candidate.partition_position);
            let delivery = AggregateGroupDelivery {
                delivery_id: candidate.delivery_id,
                consumer_id: consumer_id.into(),
                partition_position: candidate.partition_position,
                aggregate_id: candidate.aggregate_id,
                aggregate_version: candidate.aggregate_version,
                event_id: candidate.event_id,
                attempt: retry.as_ref().map(|value| value.attempt).unwrap_or(0),
                deadline_ms,
                group_epoch: self.epoch,
                replayed: retry
                    .as_ref()
                    .map(|value| value.replayed)
                    .unwrap_or(candidate.replayed),
            };
            self.leases.insert(
                delivery.aggregate_id.clone(),
                AggregateInstanceLease {
                    consumer_id: consumer_id.into(),
                    group_epoch: self.epoch,
                    deadline_ms,
                },
            );
            self.deliveries
                .insert(delivery.delivery_id, delivery.clone());
            claimed_bytes = claimed_bytes.saturating_add(candidate.payload_bytes);
            claimed.push(delivery);
            credit -= 1;
        }
        claimed
    }

    /// 按输入顺序结算 delivery；单条错误不阻止其它条目。
    pub fn settle(
        &mut self,
        consumer_id: &str,
        epoch: u64,
        now_ms: u64,
        settings: &AggregateGroupSettings,
        settlements: &[AggregateSettlement],
    ) -> Vec<AggregateSettlementResult> {
        if epoch != self.epoch {
            return vec![AggregateSettlementResult::StaleLease; settlements.len()];
        }
        let mut results = Vec::with_capacity(settlements.len());
        for settlement in settlements {
            let Some(delivery) = self.deliveries.get(&settlement.delivery_id).cloned() else {
                results.push(AggregateSettlementResult::AlreadySettled);
                continue;
            };
            if delivery.consumer_id != consumer_id {
                results.push(AggregateSettlementResult::WrongConsumer);
                continue;
            }
            self.deliveries.remove(&settlement.delivery_id);
            match settlement.action {
                AggregateSettlementAction::Ack | AggregateSettlementAction::Skip => {
                    self.resolve(delivery.partition_position);
                }
                AggregateSettlementAction::Retry => {
                    self.retry_or_park(delivery, settlement.reason.clone(), now_ms, settings);
                }
                AggregateSettlementAction::Park => {
                    self.park(delivery, settlement.reason.clone(), now_ms);
                }
            }
            results.push(AggregateSettlementResult::Applied);
        }
        self.release_unused_leases();
        results
    }

    /// 续租属于该消费者和当前 epoch 的 delivery。
    pub fn renew(
        &mut self,
        consumer_id: &str,
        epoch: u64,
        deadline_ms: u64,
        delivery_ids: &[Uuid],
    ) -> Vec<AggregateSettlementResult> {
        if epoch != self.epoch {
            return vec![AggregateSettlementResult::StaleLease; delivery_ids.len()];
        }
        delivery_ids
            .iter()
            .map(|delivery_id| {
                let Some(delivery) = self.deliveries.get_mut(delivery_id) else {
                    return AggregateSettlementResult::AlreadySettled;
                };
                if delivery.consumer_id != consumer_id {
                    return AggregateSettlementResult::WrongConsumer;
                }
                delivery.deadline_ms = delivery.deadline_ms.max(deadline_ms);
                if let Some(lease) = self.leases.get_mut(&delivery.aggregate_id) {
                    lease.deadline_ms = lease.deadline_ms.max(deadline_ms);
                }
                AggregateSettlementResult::Applied
            })
            .collect()
    }

    /// 回收超时 delivery 并转入 retry/park。
    pub fn expire(&mut self, now_ms: u64, settings: &AggregateGroupSettings) -> usize {
        let expired = self
            .deliveries
            .iter()
            .filter_map(|(id, value)| (value.deadline_ms <= now_ms).then_some(*id))
            .collect::<Vec<_>>();
        for id in &expired {
            if let Some(delivery) = self.deliveries.remove(id) {
                self.retry_or_park(delivery, "ack timeout".into(), now_ms, settings);
            }
        }
        self.release_unused_leases();
        expired.len()
    }

    fn resolve(&mut self, position: u64) {
        if position < self.next_position {
            return;
        }
        self.resolved_gaps.insert(position);
        while self.resolved_gaps.remove(&self.next_position) {
            self.next_position = self.next_position.saturating_add(1);
        }
    }

    fn retry_or_park(
        &mut self,
        delivery: AggregateGroupDelivery,
        reason: String,
        now_ms: u64,
        settings: &AggregateGroupSettings,
    ) {
        let attempt = delivery.attempt.saturating_add(1);
        if attempt > settings.max_retries {
            self.park(delivery, reason, now_ms);
            return;
        }
        self.pending_retries.insert(
            delivery.partition_position,
            AggregateGroupRetry {
                partition_position: delivery.partition_position,
                aggregate_id: delivery.aggregate_id,
                aggregate_version: delivery.aggregate_version,
                event_id: delivery.event_id,
                attempt,
                not_before_ms: now_ms.saturating_add(settings.retry_delay_ms(attempt)),
                replayed: delivery.replayed,
            },
        );
    }

    fn park(&mut self, delivery: AggregateGroupDelivery, reason: String, now_ms: u64) {
        self.resolve(delivery.partition_position);
        self.parked.insert(
            delivery.delivery_id,
            AggregateGroupParked {
                partition_position: delivery.partition_position,
                aggregate_id: delivery.aggregate_id,
                aggregate_version: delivery.aggregate_version,
                event_id: delivery.event_id,
                attempts: delivery.attempt,
                reason,
                parked_at_ms: now_ms,
            },
        );
    }

    fn release_unused_leases(&mut self) {
        let active = self
            .deliveries
            .values()
            .map(|delivery| delivery.aggregate_id.clone())
            .chain(
                self.pending_retries
                    .values()
                    .map(|retry| retry.aggregate_id.clone()),
            )
            .collect::<BTreeSet<_>>();
        self.leases
            .retain(|aggregate_id, _| active.contains(aggregate_id));
    }
}

/// delivery token 内部载荷；公共接口只传输序列化 bytes。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDeliveryToken {
    pub version: u8,
    pub event_set: EventSetId,
    pub group_name: String,
    pub partition_id: u16,
    pub group_epoch: u64,
    pub delivery_id: Uuid,
}

/// 供投递响应携带的公开事件内容。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregateDeliveryEvent {
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub event_id: Uuid,
    pub event_type: String,
    pub data: Vec<u8>,
    pub metadata: Vec<u8>,
    pub hlc: Hlc,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> AggregateGroupSettings {
        AggregateGroupSettings {
            max_unacked_per_consumer: 8,
            max_unacked_per_group: 16,
            ack_timeout_ms: 10,
            max_retries: 2,
            retry_min_ms: 1,
            retry_max_ms: 4,
        }
    }

    fn candidate(position: u64, aggregate: &str) -> AggregateDeliveryCandidate {
        AggregateDeliveryCandidate {
            delivery_id: Uuid::new_v4(),
            partition_position: position,
            aggregate_id: aggregate.into(),
            aggregate_version: position,
            event_id: Uuid::new_v4(),
            payload_bytes: 1,
            replayed: false,
        }
    }

    fn definition(operation_id: Uuid) -> AggregateGroupDefinition {
        AggregateGroupDefinition {
            event_set: EventSetId::new("orders", "order").unwrap(),
            name: "workers".into(),
            revision: 0,
            epoch: 0,
            start: AggregateGroupStart::Beginning,
            partition_starts: (0..crate::EVENT_PARTITION_COUNT)
                .map(|partition| (partition, 0))
                .collect(),
            settings: settings(),
            create_operation_id: operation_id,
            last_operation_id: operation_id,
        }
    }

    #[test]
    fn settings_and_group_definition_reject_invalid_boundaries() {
        let mut invalid = settings();
        invalid.max_unacked_per_consumer = 0;
        assert!(invalid.validate().is_err());
        invalid = settings();
        invalid.max_unacked_per_group = 0;
        assert!(invalid.validate().is_err());
        invalid = settings();
        invalid.max_unacked_per_consumer = 17;
        assert!(invalid.validate().is_err());
        invalid = settings();
        invalid.ack_timeout_ms = 0;
        assert!(invalid.validate().is_err());
        invalid = settings();
        invalid.retry_min_ms = 0;
        assert!(invalid.validate().is_err());
        invalid = settings();
        invalid.retry_min_ms = invalid.retry_max_ms + 1;
        assert!(invalid.validate().is_err());

        let mut catalog = AggregateGroupCatalog::default();
        let mut invalid_definition = definition(Uuid::new_v4());
        invalid_definition.partition_starts.remove(&0);
        let result = catalog.apply(AggregateGroupCatalogCommand::Create {
            definition: invalid_definition,
            partition_count: crate::EVENT_PARTITION_COUNT,
        });
        assert!(matches!(
            result.outcome,
            AggregateGroupCatalogOutcome::Invalid { .. }
        ));
        assert_eq!(catalog.revision, 0);
    }

    #[test]
    fn catalog_reports_idempotency_not_found_and_revision_conflicts() {
        let mut catalog = AggregateGroupCatalog::default();
        let create_id = Uuid::new_v4();
        let create = || AggregateGroupCatalogCommand::Create {
            definition: definition(create_id),
            partition_count: crate::EVENT_PARTITION_COUNT,
        };
        catalog.apply(create());
        assert!(matches!(
            catalog.apply(create()).outcome,
            AggregateGroupCatalogOutcome::Group(_)
        ));
        assert!(matches!(
            catalog
                .apply(AggregateGroupCatalogCommand::Create {
                    definition: definition(Uuid::new_v4()),
                    partition_count: crate::EVENT_PARTITION_COUNT,
                })
                .outcome,
            AggregateGroupCatalogOutcome::Conflict { .. }
        ));

        let mut replacement = definition(create_id);
        replacement.last_operation_id = Uuid::new_v4();
        assert!(matches!(
            catalog
                .apply(AggregateGroupCatalogCommand::Replace {
                    definition: replacement.clone(),
                    expected_revision: 9,
                    partition_count: crate::EVENT_PARTITION_COUNT,
                    reset: false,
                })
                .outcome,
            AggregateGroupCatalogOutcome::Conflict { .. }
        ));
        let mut missing = replacement;
        missing.name = "missing".into();
        assert!(matches!(
            catalog
                .apply(AggregateGroupCatalogCommand::Replace {
                    definition: missing,
                    expected_revision: 1,
                    partition_count: crate::EVENT_PARTITION_COUNT,
                    reset: false,
                })
                .outcome,
            AggregateGroupCatalogOutcome::NotFound
        ));

        let event_set = EventSetId::new("orders", "order").unwrap();
        assert!(matches!(
            catalog
                .apply(AggregateGroupCatalogCommand::Delete {
                    event_set: event_set.clone(),
                    name: "missing".into(),
                    expected_revision: 1,
                    operation_id: Uuid::new_v4(),
                })
                .outcome,
            AggregateGroupCatalogOutcome::NotFound
        ));
        assert!(matches!(
            catalog
                .apply(AggregateGroupCatalogCommand::Delete {
                    event_set,
                    name: "workers".into(),
                    expected_revision: 9,
                    operation_id: Uuid::new_v4(),
                })
                .outcome,
            AggregateGroupCatalogOutcome::Conflict { .. }
        ));
    }

    #[test]
    fn settings_update_keeps_epoch_while_reset_advances_it() {
        let mut catalog = AggregateGroupCatalog::default();
        let create_id = Uuid::new_v4();
        let created = catalog.apply(AggregateGroupCatalogCommand::Create {
            definition: definition(create_id),
            partition_count: crate::EVENT_PARTITION_COUNT,
        });
        let AggregateGroupCatalogOutcome::Group(created) = created.outcome else {
            panic!("创建必须返回组");
        };

        let mut settings_update = created.clone();
        settings_update.settings.ack_timeout_ms = 20;
        settings_update.last_operation_id = Uuid::new_v4();
        let updated = catalog.apply(AggregateGroupCatalogCommand::Replace {
            definition: settings_update,
            expected_revision: 1,
            partition_count: crate::EVENT_PARTITION_COUNT,
            reset: false,
        });
        let AggregateGroupCatalogOutcome::Group(updated) = updated.outcome else {
            panic!("更新必须返回组");
        };
        assert_eq!((updated.revision, updated.epoch), (2, 1));

        let mut reset = updated.clone();
        reset.last_operation_id = Uuid::new_v4();
        let reset = catalog.apply(AggregateGroupCatalogCommand::Replace {
            definition: reset,
            expected_revision: 2,
            partition_count: crate::EVENT_PARTITION_COUNT,
            reset: true,
        });
        let AggregateGroupCatalogOutcome::Group(reset) = reset.outcome else {
            panic!("重置必须返回组");
        };
        assert_eq!((reset.revision, reset.epoch), (3, 2));
    }

    #[test]
    fn delete_operation_is_idempotent_after_group_is_absent() {
        let mut catalog = AggregateGroupCatalog::default();
        let create_id = Uuid::new_v4();
        let delete_id = Uuid::new_v4();
        catalog.apply(AggregateGroupCatalogCommand::Create {
            definition: definition(create_id),
            partition_count: crate::EVENT_PARTITION_COUNT,
        });
        let command = || AggregateGroupCatalogCommand::Delete {
            event_set: EventSetId::new("orders", "order").unwrap(),
            name: "workers".into(),
            expected_revision: 1,
            operation_id: delete_id,
        };

        let first = catalog.apply(command());
        let revision = first.revision;
        assert_eq!(first.outcome, AggregateGroupCatalogOutcome::Deleted);
        let retry = catalog.apply(command());
        assert_eq!(retry.outcome, AggregateGroupCatalogOutcome::Deleted);
        assert_eq!(
            retry.revision, revision,
            "幂等重试不能推进 catalog revision"
        );
    }

    #[test]
    fn same_instance_has_at_most_one_unsettled_delivery() {
        let mut state = AggregateGroupPartition::new(1, 0);
        let claimed = state.claim(
            "consumer-a",
            0,
            10,
            &settings(),
            8,
            1024,
            vec![candidate(0, "a"), candidate(1, "a"), candidate(2, "b")],
        );
        assert_eq!(claimed.len(), 2);
        assert_eq!(
            claimed
                .iter()
                .map(|value| value.aggregate_id.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["a", "b"])
        );
    }

    #[test]
    fn out_of_order_ack_only_advances_contiguous_checkpoint() {
        let mut state = AggregateGroupPartition::new(1, 0);
        let claimed = state.claim(
            "consumer-a",
            0,
            10,
            &settings(),
            8,
            1024,
            vec![candidate(0, "a"), candidate(1, "b")],
        );
        state.settle(
            "consumer-a",
            1,
            1,
            &settings(),
            &[AggregateSettlement {
                delivery_id: claimed[1].delivery_id,
                action: AggregateSettlementAction::Ack,
                reason: String::new(),
            }],
        );
        assert_eq!(state.next_position, 0);
        state.settle(
            "consumer-a",
            1,
            2,
            &settings(),
            &[AggregateSettlement {
                delivery_id: claimed[0].delivery_id,
                action: AggregateSettlementAction::Ack,
                reason: String::new(),
            }],
        );
        assert_eq!(state.next_position, 2);
    }

    #[test]
    fn retry_blocks_only_its_instance_and_expiry_redelivers() {
        let config = settings();
        let mut state = AggregateGroupPartition::new(1, 0);
        let original = candidate(0, "a");
        let event_id = original.event_id;
        let claimed = state.claim(
            "consumer-a",
            0,
            10,
            &config,
            8,
            1024,
            vec![original, candidate(1, "b")],
        );
        assert_eq!(state.expire(10, &config), 2);
        let redelivery = state.claim(
            "consumer-b",
            11,
            20,
            &config,
            8,
            1024,
            vec![
                AggregateDeliveryCandidate {
                    delivery_id: Uuid::new_v4(),
                    partition_position: 0,
                    aggregate_id: "a".into(),
                    aggregate_version: 0,
                    event_id,
                    payload_bytes: 1,
                    replayed: false,
                },
                candidate(2, "a"),
                candidate(3, "c"),
            ],
        );
        assert!(redelivery.iter().any(|value| value.aggregate_id == "a"));
        assert!(!redelivery.iter().any(|value| value.partition_position == 2));
        assert!(redelivery.iter().any(|value| value.aggregate_id == "c"));
        assert_eq!(claimed.len(), 2);
    }

    #[test]
    fn blocked_candidate_does_not_consume_claim_or_byte_budget() {
        let config = settings();
        let mut state = AggregateGroupPartition::new(1, 0);
        state.claim(
            "consumer-a",
            0,
            10,
            &config,
            1,
            1,
            vec![candidate(0, "blocked")],
        );

        let claimed = state.claim(
            "consumer-b",
            1,
            11,
            &config,
            1,
            1,
            vec![candidate(0, "blocked"), candidate(1, "available")],
        );

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].aggregate_id, "available");
        assert_eq!(claimed[0].partition_position, 1);
    }

    #[test]
    fn claim_guards_filters_and_enforces_byte_budget() {
        let config = settings();
        for (consumer, now, deadline, count, bytes) in [
            ("", 0, 10, 1, 1),
            ("consumer", 10, 10, 1, 1),
            ("consumer", 0, 10, 0, 1),
            ("consumer", 0, 10, 1, 0),
        ] {
            let mut state = AggregateGroupPartition::new(1, 0);
            assert!(
                state
                    .claim(
                        consumer,
                        now,
                        deadline,
                        &config,
                        count,
                        bytes,
                        vec![candidate(0, "a")],
                    )
                    .is_empty()
            );
        }
        let mut invalid_config = config.clone();
        invalid_config.ack_timeout_ms = 0;
        assert!(
            AggregateGroupPartition::new(1, 0)
                .claim(
                    "consumer",
                    0,
                    10,
                    &invalid_config,
                    1,
                    1,
                    vec![candidate(0, "a")],
                )
                .is_empty()
        );

        let mut state = AggregateGroupPartition::new(1, 1);
        let old = candidate(0, "old");
        assert!(
            state
                .claim("consumer", 0, 10, &config, 2, 10, vec![old])
                .is_empty()
        );

        let first = candidate(1, "a");
        let duplicate_id = first.clone();
        let same_position = AggregateDeliveryCandidate {
            delivery_id: Uuid::new_v4(),
            aggregate_id: "b".into(),
            ..first.clone()
        };
        let large = AggregateDeliveryCandidate {
            payload_bytes: 8,
            ..candidate(2, "c")
        };
        let claimed = state.claim(
            "consumer",
            0,
            10,
            &config,
            4,
            4,
            vec![first, duplicate_id, same_position, large],
        );
        assert_eq!(claimed.len(), 1, "第二个事件超过剩余字节额度时停止");

        state.resolved_gaps.insert(4);
        let replayed = AggregateDeliveryCandidate {
            replayed: true,
            ..candidate(3, "replayed")
        };
        assert!(
            state
                .claim(
                    "consumer",
                    0,
                    10,
                    &config,
                    2,
                    10,
                    vec![candidate(4, "resolved"), replayed],
                )
                .is_empty()
        );
    }

    #[test]
    fn settle_and_renew_report_stale_missing_and_wrong_consumer() {
        let config = settings();
        let mut state = AggregateGroupPartition::new(2, 0);
        let claimed = state.claim("consumer-a", 0, 10, &config, 1, 10, vec![candidate(0, "a")]);
        let delivery_id = claimed[0].delivery_id;
        let settlement = AggregateSettlement {
            delivery_id,
            action: AggregateSettlementAction::Ack,
            reason: String::new(),
        };
        assert_eq!(
            state.settle("consumer-a", 1, 1, &config, &[settlement.clone()]),
            vec![AggregateSettlementResult::StaleLease]
        );
        assert_eq!(
            state.settle("consumer-b", 2, 1, &config, &[settlement.clone()]),
            vec![AggregateSettlementResult::WrongConsumer]
        );
        assert_eq!(
            state.renew("consumer-a", 1, 20, &[delivery_id]),
            vec![AggregateSettlementResult::StaleLease]
        );
        assert_eq!(
            state.renew("consumer-b", 2, 20, &[delivery_id]),
            vec![AggregateSettlementResult::WrongConsumer]
        );
        assert_eq!(
            state.settle("consumer-a", 2, 1, &config, &[settlement.clone()]),
            vec![AggregateSettlementResult::Applied]
        );
        assert_eq!(
            state.settle("consumer-a", 2, 1, &config, &[settlement]),
            vec![AggregateSettlementResult::AlreadySettled]
        );
        assert_eq!(
            state.renew("consumer-a", 2, 20, &[Uuid::new_v4()]),
            vec![AggregateSettlementResult::AlreadySettled]
        );

        let lease_free = candidate(1, "lease-free");
        let lease_free_id = lease_free.delivery_id;
        state.deliveries.insert(
            lease_free_id,
            AggregateGroupDelivery {
                delivery_id: lease_free_id,
                consumer_id: "consumer-a".into(),
                partition_position: lease_free.partition_position,
                aggregate_id: lease_free.aggregate_id,
                aggregate_version: lease_free.aggregate_version,
                event_id: lease_free.event_id,
                attempt: 0,
                deadline_ms: 10,
                group_epoch: 2,
                replayed: false,
            },
        );
        assert_eq!(
            state.renew("consumer-a", 2, 20, &[lease_free_id]),
            vec![AggregateSettlementResult::Applied]
        );

        state.reset(3, 7);
        assert_eq!((state.epoch, state.next_position), (3, 7));
        assert!(state.deliveries.is_empty());
        assert!(state.leases.is_empty());
    }
}
