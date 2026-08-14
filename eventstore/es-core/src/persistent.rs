//! 持久化拉取订阅的纯领域状态。
//!
//! 本 module 不执行 I/O。控制 Shard Raft 状态机通过这里完成 claim、settle、
//! 租约过期与 parked 重放，保证所有副本对同一命令得到相同结果。

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Fetch 默认最多返回的事件数。
pub const DEFAULT_FETCH_EVENTS: u32 = 100;
/// Fetch 允许的最大事件数。
pub const MAX_FETCH_EVENTS: u32 = 1000;
/// Fetch 默认字节预算。
pub const DEFAULT_FETCH_BYTES: u64 = 4 * 1024 * 1024;
/// Fetch 最大字节预算，为 gRPC 信封保留 1 MiB。
pub const MAX_FETCH_BYTES: u64 = 7 * 1024 * 1024;
/// Fetch 默认长轮询时间。
pub const DEFAULT_FETCH_WAIT_MS: u64 = 15_000;
/// Fetch 最大长轮询时间。
pub const MAX_FETCH_WAIT_MS: u64 = 30_000;

/// 持久化订阅目标。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistentTarget {
    /// 一个或多个显式 Stream。
    Streams(BTreeSet<String>),
    /// 当前及未来的全部 Stream。
    All,
}

impl PersistentTarget {
    /// 判断 Stream 是否属于目标。
    pub fn contains(&self, stream: &str) -> bool {
        match self {
            Self::Streams(streams) => streams.contains(stream),
            Self::All => true,
        }
    }
}

/// 持久化订阅投递参数。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentSettings {
    /// 单个消费者最多持有的未确认事件数。
    pub max_unacked_per_consumer: u32,
    /// 整个组最多持有的未确认事件数。
    pub max_unacked_per_group: u32,
    /// delivery 租约时长。
    pub ack_timeout_ms: u64,
    /// 初次投递之后允许的重试次数。
    pub max_retries: u32,
    /// 指数退避起点。
    pub retry_min_ms: u64,
    /// 指数退避上限。
    pub retry_max_ms: u64,
}

impl Default for PersistentSettings {
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

impl PersistentSettings {
    /// 校验所有额度与时间参数。
    ///
    /// # 错误
    /// 任一额度为零、消费者额度超过组额度、退避范围倒置时返回原因。
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

    /// 计算第 `attempt` 次重试的指数退避，结果饱和到配置上限。
    pub fn retry_delay_ms(&self, attempt: u32) -> u64 {
        let shift = attempt.saturating_sub(1).min(62);
        self.retry_min_ms
            .saturating_mul(1u64 << shift)
            .min(self.retry_max_ms)
    }
}

/// 同一 Stream 的当前租约。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamLease {
    /// 持有者。
    pub consumer_id: String,
    /// 组 epoch，目标/reset 更新后递增。
    pub group_epoch: u64,
    /// 最晚有效时间（Unix 毫秒，由 leader 写入命令）。
    pub deadline_ms: u64,
}

/// 单个 Stream 的连续 checkpoint 与确认缺口。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamProgress {
    /// 下一条尚未解决的 version。
    pub next_version: u64,
    /// 已解决但尚未与 next_version 连续的 version。
    pub resolved_gaps: BTreeSet<u64>,
    /// 最近观察到的归属 generation，用于迁移对账。
    pub ownership_generation: u64,
    /// 当前 stream lease。
    pub lease: Option<StreamLease>,
}

impl StreamProgress {
    /// 从指定 inclusive version 创建进度。
    pub fn new(next_version: u64, ownership_generation: u64) -> Self {
        Self {
            next_version,
            resolved_gaps: BTreeSet::new(),
            ownership_generation,
            lease: None,
        }
    }

    fn resolve(&mut self, version: u64) {
        if version < self.next_version {
            return;
        }
        self.resolved_gaps.insert(version);
        while self.resolved_gaps.remove(&self.next_version) {
            self.next_version = self.next_version.saturating_add(1);
        }
    }
}

/// 进入 Raft claim 命令的事件引用；不包含 payload，避免复制事件本体。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryCandidate {
    /// leader 生成的幂等 delivery ID。
    pub delivery_id: Uuid,
    /// Stream ID。
    pub stream_id: String,
    /// Stream version。
    pub version: u64,
    /// 原事件 ID。
    pub event_id: Uuid,
    /// 是否来自 parked 重放。
    pub replayed: bool,
}

/// 已提交且尚未结算的 delivery。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentDelivery {
    /// 不透明投递 ID。
    pub delivery_id: Uuid,
    /// 消费者 ID。
    pub consumer_id: String,
    /// Stream ID。
    pub stream_id: String,
    /// Stream version。
    pub version: u64,
    /// 原事件 ID。
    pub event_id: Uuid,
    /// 当前投递尝试次数，初次为 0。
    pub attempt: u32,
    /// 租约截止 Unix 毫秒。
    pub deadline_ms: u64,
    /// claim 时的组 epoch。
    pub group_epoch: u64,
    /// 是否来自 parked 重放。
    pub replayed: bool,
}

/// 等待重试的事件引用。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRetry {
    pub stream_id: String,
    pub version: u64,
    pub event_id: Uuid,
    pub attempt: u32,
    pub not_before_ms: u64,
    pub replayed: bool,
}

/// 已停放事件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParkedEvent {
    pub stream_id: String,
    pub version: u64,
    pub event_id: Uuid,
    pub attempts: u32,
    pub reason: String,
    pub parked_at_ms: u64,
}

/// 持久化订阅组的完整复制状态。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentGroup {
    pub name: String,
    pub revision: u64,
    pub epoch: u64,
    pub target: PersistentTarget,
    pub settings: PersistentSettings,
    pub progress: BTreeMap<String, StreamProgress>,
    pub deliveries: BTreeMap<Uuid, PersistentDelivery>,
    pub pending_retries: BTreeMap<(String, u64), PendingRetry>,
    pub parked: BTreeMap<Uuid, ParkedEvent>,
    /// 公平扫描的最后一个 Stream；只影响性能，不参与 checkpoint 正确性。
    pub scan_after: Option<String>,
}

/// Settle 动作。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementAction {
    Ack,
    Retry,
    Park,
    Skip,
}

/// 单条 Settle 输入。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settlement {
    pub delivery_id: Uuid,
    pub action: SettlementAction,
    pub reason: String,
}

/// 单条 Settle 结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettlementResult {
    Applied,
    AlreadySettled,
    StaleLease,
    WrongConsumer,
}

impl PersistentGroup {
    /// 创建一个无活动 delivery 的订阅组。
    ///
    /// # 参数
    /// `progress` 必须覆盖显式目标；`$all` 可在后续 reconcile 时增加 Stream。
    ///
    /// # 错误
    /// 名称、目标、设置或初始进度违反不变量时返回原因。
    pub fn new(
        name: String,
        target: PersistentTarget,
        settings: PersistentSettings,
        progress: BTreeMap<String, StreamProgress>,
    ) -> Result<Self, String> {
        validate_group_name(&name)?;
        settings.validate()?;
        if let PersistentTarget::Streams(streams) = &target {
            if streams.is_empty() {
                return Err("Streams 目标不能为空".into());
            }
            if streams.iter().any(|stream| !progress.contains_key(stream)) {
                return Err("显式目标必须有初始进度".into());
            }
        }
        Ok(Self {
            name,
            revision: 1,
            epoch: 1,
            target,
            settings,
            progress,
            deliveries: BTreeMap::new(),
            pending_retries: BTreeMap::new(),
            parked: BTreeMap::new(),
            scan_after: None,
        })
    }

    /// 当前组与消费者剩余额度。
    pub fn available_credit(&self, consumer_id: &str) -> u32 {
        let consumer_used = self
            .deliveries
            .values()
            .filter(|delivery| delivery.consumer_id == consumer_id)
            .count() as u32;
        let group_used = self.deliveries.len() as u32;
        self.settings
            .max_unacked_per_consumer
            .saturating_sub(consumer_used)
            .min(
                self.settings
                    .max_unacked_per_group
                    .saturating_sub(group_used),
            )
    }

    /// 将新发现的 `$all` Stream 纳入组；已有进度不变。
    pub fn ensure_streams(&mut self, streams: BTreeMap<String, StreamProgress>) -> bool {
        let mut changed = false;
        for (stream, progress) in streams {
            if self.target.contains(&stream) && !self.progress.contains_key(&stream) {
                self.progress.insert(stream, progress);
                changed = true;
            }
        }
        if changed {
            self.revision = self.revision.saturating_add(1);
        }
        changed
    }

    /// 提升组 epoch，并让保留 Stream 的活动 delivery 立即重新可投递。
    ///
    /// # 参数
    /// `preserve_streams` 是配置变更后仍保留且未 reset 的 Stream。其它 Stream 的
    /// delivery 直接失效；保留 Stream 的 delivery 保持 attempt 与 event ID，转入 retry。
    pub fn begin_new_epoch(&mut self, preserve_streams: &BTreeSet<String>) {
        let deliveries = std::mem::take(&mut self.deliveries);
        for delivery in deliveries.into_values() {
            if !preserve_streams.contains(&delivery.stream_id) {
                continue;
            }
            self.pending_retries.insert(
                (delivery.stream_id.clone(), delivery.version),
                PendingRetry {
                    stream_id: delivery.stream_id,
                    version: delivery.version,
                    event_id: delivery.event_id,
                    attempt: delivery.attempt,
                    not_before_ms: 0,
                    replayed: delivery.replayed,
                },
            );
        }
        self.epoch = self.epoch.saturating_add(1);
        for progress in self.progress.values_mut() {
            progress.lease = None;
        }
    }

    /// 对账 Stream 当前 ownership generation。
    ///
    /// generation 变化意味着在线迁移可能重排目标 Shard 的 version。为保证
    /// at-least-once，此处只重置受影响 Stream 并从 0 重扫；允许重复，禁止漏投。
    /// 返回是否实际修改状态。
    pub fn reconcile_ownership(&mut self, generations: BTreeMap<String, u64>) -> bool {
        let mut changed = BTreeSet::new();
        for (stream, generation) in generations {
            if self
                .progress
                .get(&stream)
                .is_some_and(|progress| generation > progress.ownership_generation)
            {
                self.progress
                    .insert(stream.clone(), StreamProgress::new(0, generation));
                changed.insert(stream);
            }
        }
        if changed.is_empty() {
            return false;
        }
        self.deliveries
            .retain(|_, delivery| !changed.contains(&delivery.stream_id));
        self.pending_retries
            .retain(|(stream, _), _| !changed.contains(stream));
        // 旧 version 已不再稳定；从 0 重扫会把这些事件重新送回主消费路径。
        self.parked
            .retain(|_, event| !changed.contains(&event.stream_id));
        if self
            .scan_after
            .as_ref()
            .is_some_and(|stream| changed.contains(stream))
        {
            self.scan_after = None;
        }
        self.revision = self.revision.saturating_add(1);
        true
    }

    /// 原子 claim 候选引用，返回真正取得租约的 delivery。
    ///
    /// `now_ms/deadline_ms` 必须由 leader 固化在 Raft 命令中，apply 不读取本地时钟。
    pub fn claim(
        &mut self,
        consumer_id: &str,
        now_ms: u64,
        deadline_ms: u64,
        candidates: Vec<DeliveryCandidate>,
    ) -> Vec<PersistentDelivery> {
        if consumer_id.is_empty() || deadline_ms <= now_ms {
            return Vec::new();
        }
        self.expire(now_ms);
        let mut credit = self.available_credit(consumer_id);
        let mut claimed = Vec::new();
        for candidate in candidates {
            if credit == 0 || self.deliveries.contains_key(&candidate.delivery_id) {
                break;
            }
            let Some(progress) = self.progress.get_mut(&candidate.stream_id) else {
                continue;
            };
            let retry_key = (candidate.stream_id.clone(), candidate.version);
            let retry = self.pending_retries.get(&retry_key).cloned();
            if let Some(retry) = &retry {
                if retry.not_before_ms > now_ms || retry.event_id != candidate.event_id {
                    continue;
                }
            } else if candidate.version < progress.next_version || candidate.replayed {
                continue;
            }
            if progress.resolved_gaps.contains(&candidate.version)
                || self.deliveries.values().any(|delivery| {
                    delivery.stream_id == candidate.stream_id
                        && delivery.version == candidate.version
                })
            {
                continue;
            }
            if progress
                .lease
                .as_ref()
                .is_some_and(|lease| lease.consumer_id != consumer_id && lease.deadline_ms > now_ms)
            {
                continue;
            }
            let attempt = retry.as_ref().map(|item| item.attempt).unwrap_or(0);
            let replayed = retry
                .as_ref()
                .map(|item| item.replayed)
                .unwrap_or(candidate.replayed);
            self.pending_retries.remove(&retry_key);
            progress.lease = Some(StreamLease {
                consumer_id: consumer_id.to_string(),
                group_epoch: self.epoch,
                deadline_ms,
            });
            let delivery = PersistentDelivery {
                delivery_id: candidate.delivery_id,
                consumer_id: consumer_id.to_string(),
                stream_id: candidate.stream_id,
                version: candidate.version,
                event_id: candidate.event_id,
                attempt,
                deadline_ms,
                group_epoch: self.epoch,
                replayed,
            };
            self.deliveries
                .insert(delivery.delivery_id, delivery.clone());
            self.scan_after = Some(delivery.stream_id.clone());
            claimed.push(delivery);
            credit -= 1;
        }
        claimed
    }

    /// 批量结算 delivery。
    ///
    /// 返回值与输入顺序一致；单条陈旧或非法归属不阻止其它条目生效。
    pub fn settle(
        &mut self,
        consumer_id: &str,
        group_epoch: u64,
        now_ms: u64,
        settlements: &[Settlement],
    ) -> Vec<SettlementResult> {
        if group_epoch != self.epoch {
            return vec![SettlementResult::StaleLease; settlements.len()];
        }
        let mut results = Vec::with_capacity(settlements.len());
        for settlement in settlements {
            let Some(delivery) = self.deliveries.get(&settlement.delivery_id).cloned() else {
                results.push(SettlementResult::AlreadySettled);
                continue;
            };
            if delivery.consumer_id != consumer_id {
                results.push(SettlementResult::WrongConsumer);
                continue;
            }
            self.deliveries.remove(&settlement.delivery_id);
            match settlement.action {
                SettlementAction::Ack | SettlementAction::Skip => {
                    self.resolve_delivery(&delivery);
                }
                SettlementAction::Park => {
                    self.park_delivery(&delivery, settlement.reason.clone(), now_ms);
                }
                SettlementAction::Retry => {
                    self.retry_or_park(delivery, settlement.reason.clone(), now_ms);
                }
            }
            results.push(SettlementResult::Applied);
        }
        self.release_empty_leases(now_ms);
        results
    }

    /// 过期所有 deadline 不晚于 `now_ms` 的 delivery，返回过期数量。
    pub fn expire(&mut self, now_ms: u64) -> usize {
        let expired: Vec<Uuid> = self
            .deliveries
            .iter()
            .filter_map(|(id, delivery)| (delivery.deadline_ms <= now_ms).then_some(*id))
            .collect();
        for id in &expired {
            if let Some(delivery) = self.deliveries.remove(id) {
                self.retry_or_park(delivery, "ack timeout".into(), now_ms);
            }
        }
        self.release_empty_leases(now_ms);
        expired.len()
    }

    /// 将全部 parked 事件重新放入 retry 队列，返回重放数量。
    pub fn replay_parked(&mut self, now_ms: u64) -> usize {
        let parked = std::mem::take(&mut self.parked);
        let count = parked.len();
        for (_, event) in parked {
            self.pending_retries.insert(
                (event.stream_id.clone(), event.version),
                PendingRetry {
                    stream_id: event.stream_id,
                    version: event.version,
                    event_id: event.event_id,
                    attempt: 0,
                    not_before_ms: now_ms,
                    replayed: true,
                },
            );
        }
        if count > 0 {
            self.revision = self.revision.saturating_add(1);
        }
        count
    }

    fn retry_or_park(&mut self, delivery: PersistentDelivery, reason: String, now_ms: u64) {
        let next_attempt = delivery.attempt.saturating_add(1);
        if next_attempt > self.settings.max_retries {
            self.park_delivery(&delivery, reason, now_ms);
            return;
        }
        let delay = self.settings.retry_delay_ms(next_attempt);
        self.pending_retries.insert(
            (delivery.stream_id.clone(), delivery.version),
            PendingRetry {
                stream_id: delivery.stream_id,
                version: delivery.version,
                event_id: delivery.event_id,
                attempt: next_attempt,
                not_before_ms: now_ms.saturating_add(delay),
                replayed: delivery.replayed,
            },
        );
    }

    fn park_delivery(&mut self, delivery: &PersistentDelivery, reason: String, now_ms: u64) {
        self.parked.insert(
            delivery.delivery_id,
            ParkedEvent {
                stream_id: delivery.stream_id.clone(),
                version: delivery.version,
                event_id: delivery.event_id,
                attempts: delivery.attempt.saturating_add(1),
                reason,
                parked_at_ms: now_ms,
            },
        );
        self.resolve_delivery(delivery);
    }

    fn resolve_delivery(&mut self, delivery: &PersistentDelivery) {
        if delivery.replayed {
            return;
        }
        if let Some(progress) = self.progress.get_mut(&delivery.stream_id) {
            progress.resolve(delivery.version);
        }
    }

    fn release_empty_leases(&mut self, now_ms: u64) {
        for (stream, progress) in &mut self.progress {
            let has_delivery = self
                .deliveries
                .values()
                .any(|delivery| delivery.stream_id == *stream);
            if !has_delivery
                || progress
                    .lease
                    .as_ref()
                    .is_some_and(|lease| lease.deadline_ms <= now_ms)
            {
                progress.lease = None;
            }
        }
    }
}

/// 校验公开组名，限制字符集便于日志、CLI 与持久化 key 稳定表达。
pub fn validate_group_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 128 {
        return Err("组名长度必须为 1..=128".into());
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("组名只能包含 ASCII 字母、数字、点、下划线和连字符".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn group() -> PersistentGroup {
        PersistentGroup::new(
            "orders".into(),
            PersistentTarget::Streams(BTreeSet::from(["s".into()])),
            PersistentSettings::default(),
            BTreeMap::from([("s".into(), StreamProgress::new(0, 1))]),
        )
        .unwrap()
    }

    fn candidate(version: u64) -> DeliveryCandidate {
        DeliveryCandidate {
            delivery_id: Uuid::new_v4(),
            stream_id: "s".into(),
            version,
            event_id: Uuid::new_v4(),
            replayed: false,
        }
    }

    #[test]
    fn claim_respects_stream_lease_and_credit() {
        let mut g = group();
        g.settings.max_unacked_per_consumer = 2;
        let claimed = g.claim("a", 10, 20, vec![candidate(0), candidate(1), candidate(2)]);
        assert_eq!(claimed.len(), 2);
        assert!(g.claim("b", 10, 20, vec![candidate(2)]).is_empty());
        assert_eq!(g.available_credit("a"), 0);
    }

    #[test]
    fn out_of_order_ack_advances_only_when_gap_closes() {
        let mut g = group();
        let claimed = g.claim("a", 10, 20, vec![candidate(0), candidate(1)]);
        let second = Settlement {
            delivery_id: claimed[1].delivery_id,
            action: SettlementAction::Ack,
            reason: String::new(),
        };
        g.settle("a", 1, 11, &[second]);
        assert_eq!(g.progress["s"].next_version, 0);
        let first = Settlement {
            delivery_id: claimed[0].delivery_id,
            action: SettlementAction::Ack,
            reason: String::new(),
        };
        g.settle("a", 1, 12, &[first]);
        assert_eq!(g.progress["s"].next_version, 2);
    }

    #[test]
    fn new_epoch_requeues_retained_delivery_without_losing_gap() {
        let mut g = group();
        let claimed = g.claim("a", 10, 20, vec![candidate(0), candidate(1)]);
        g.settle(
            "a",
            1,
            11,
            &[Settlement {
                delivery_id: claimed[1].delivery_id,
                action: SettlementAction::Ack,
                reason: String::new(),
            }],
        );
        assert_eq!(g.progress["s"].resolved_gaps, BTreeSet::from([1]));

        g.begin_new_epoch(&BTreeSet::from(["s".into()]));
        assert_eq!(g.epoch, 2);
        assert!(g.deliveries.is_empty());
        let pending = g.pending_retries.get(&("s".into(), 0)).unwrap();
        assert_eq!(pending.event_id, claimed[0].event_id);
        assert_eq!(pending.attempt, 0);
        assert_eq!(pending.not_before_ms, 0);

        let redelivered = g.claim(
            "a",
            21,
            30,
            vec![DeliveryCandidate {
                delivery_id: Uuid::new_v4(),
                stream_id: "s".into(),
                version: 0,
                event_id: claimed[0].event_id,
                replayed: false,
            }],
        );
        assert_eq!(redelivered.len(), 1);
        g.settle(
            "a",
            2,
            22,
            &[Settlement {
                delivery_id: redelivered[0].delivery_id,
                action: SettlementAction::Ack,
                reason: String::new(),
            }],
        );
        assert_eq!(g.progress["s"].next_version, 2);
    }

    #[test]
    fn ownership_reconcile_rewinds_only_changed_stream() {
        let mut g = group();
        g.target = PersistentTarget::All;
        g.progress.insert("other".into(), StreamProgress::new(7, 1));
        g.claim("a", 10, 20, vec![candidate(0)]);
        g.pending_retries.insert(
            ("s".into(), 1),
            PendingRetry {
                stream_id: "s".into(),
                version: 1,
                event_id: Uuid::new_v4(),
                attempt: 1,
                not_before_ms: 20,
                replayed: false,
            },
        );
        let parked_id = Uuid::new_v4();
        g.parked.insert(
            parked_id,
            ParkedEvent {
                stream_id: "s".into(),
                version: 2,
                event_id: Uuid::new_v4(),
                attempts: 3,
                reason: "bad".into(),
                parked_at_ms: 11,
            },
        );
        let revision = g.revision;

        assert!(g.reconcile_ownership(BTreeMap::from([("s".into(), 2)])));
        assert_eq!(g.progress["s"], StreamProgress::new(0, 2));
        assert_eq!(g.progress["other"], StreamProgress::new(7, 1));
        assert!(g.deliveries.is_empty());
        assert!(g.pending_retries.is_empty());
        assert!(g.parked.is_empty());
        assert_eq!(g.revision, revision + 1);
        assert!(!g.reconcile_ownership(BTreeMap::from([("s".into(), 2)])));
        assert_eq!(g.revision, revision + 1);
        assert!(!g.reconcile_ownership(BTreeMap::from([("s".into(), 1)])));
        assert_eq!(g.progress["s"].ownership_generation, 2);
        assert_eq!(g.revision, revision + 1);
    }

    #[test]
    fn retry_exhaustion_parks_and_replay_does_not_rewind_checkpoint() {
        let mut g = group();
        g.settings.max_retries = 0;
        let claimed = g.claim("a", 10, 20, vec![candidate(0)]);
        g.settle(
            "a",
            1,
            11,
            &[Settlement {
                delivery_id: claimed[0].delivery_id,
                action: SettlementAction::Retry,
                reason: "bad".into(),
            }],
        );
        assert_eq!(g.progress["s"].next_version, 1);
        assert_eq!(g.parked.len(), 1);
        assert_eq!(g.replay_parked(12), 1);
        assert_eq!(g.progress["s"].next_version, 1);
    }

    #[test]
    fn settings_targets_and_group_inputs_are_validated() {
        assert!(PersistentTarget::All.contains("anything"));
        assert!(PersistentTarget::Streams(BTreeSet::from(["s".into()])).contains("s"));
        assert!(!PersistentTarget::Streams(BTreeSet::from(["s".into()])).contains("other"));

        let invalid_settings = [
            PersistentSettings {
                max_unacked_per_consumer: 0,
                ..Default::default()
            },
            PersistentSettings {
                max_unacked_per_group: 0,
                ..Default::default()
            },
            PersistentSettings {
                max_unacked_per_consumer: 2,
                max_unacked_per_group: 1,
                ..Default::default()
            },
            PersistentSettings {
                ack_timeout_ms: 0,
                ..Default::default()
            },
            PersistentSettings {
                retry_min_ms: 0,
                ..Default::default()
            },
            PersistentSettings {
                retry_min_ms: 2,
                retry_max_ms: 1,
                ..Default::default()
            },
        ];
        for settings in invalid_settings {
            assert!(settings.validate().is_err());
        }

        assert!(PersistentGroup::new(
            "all".into(),
            PersistentTarget::All,
            PersistentSettings::default(),
            BTreeMap::new(),
        )
        .is_ok());
        assert!(PersistentGroup::new(
            "empty".into(),
            PersistentTarget::Streams(BTreeSet::new()),
            PersistentSettings::default(),
            BTreeMap::new(),
        )
        .is_err());
        assert!(PersistentGroup::new(
            "missing-progress".into(),
            PersistentTarget::Streams(BTreeSet::from(["s".into()])),
            PersistentSettings::default(),
            BTreeMap::new(),
        )
        .is_err());

        for name in ["", &"x".repeat(129), "contains/slash"] {
            assert!(validate_group_name(name).is_err());
        }
        assert!(validate_group_name("letters.NUMBERS_1-ok").is_ok());
    }

    #[test]
    fn ensure_streams_ignores_out_of_target_and_existing_entries() {
        let mut streams = group();
        let revision = streams.revision;
        assert!(!streams.ensure_streams(BTreeMap::from([
            ("s".into(), StreamProgress::new(10, 2)),
            ("other".into(), StreamProgress::new(0, 1)),
        ])));
        assert_eq!(streams.revision, revision);

        let mut all = PersistentGroup::new(
            "all".into(),
            PersistentTarget::All,
            PersistentSettings::default(),
            BTreeMap::new(),
        )
        .unwrap();
        assert!(all.ensure_streams(BTreeMap::from([(
            "new".into(),
            StreamProgress::new(0, 1),
        )])));
    }

    #[test]
    fn claim_rejects_invalid_duplicate_and_ineligible_candidates() {
        let mut g = group();
        assert!(g.claim("", 10, 20, vec![candidate(0)]).is_empty());
        assert!(g.claim("a", 10, 10, vec![candidate(0)]).is_empty());

        let mut unknown = candidate(0);
        unknown.stream_id = "unknown".into();
        assert!(g.claim("a", 10, 20, vec![unknown]).is_empty());

        let retry_event_id = Uuid::new_v4();
        g.pending_retries.insert(
            ("s".into(), 0),
            PendingRetry {
                stream_id: "s".into(),
                version: 0,
                event_id: retry_event_id,
                attempt: 2,
                not_before_ms: 11,
                replayed: true,
            },
        );
        assert!(g.claim("a", 10, 20, vec![candidate(0)]).is_empty());
        assert!(g.claim("a", 11, 20, vec![candidate(0)]).is_empty());
        let retry = g.claim(
            "a",
            11,
            20,
            vec![DeliveryCandidate {
                delivery_id: Uuid::new_v4(),
                stream_id: "s".into(),
                version: 0,
                event_id: retry_event_id,
                replayed: false,
            }],
        );
        assert_eq!(retry[0].attempt, 2);
        assert!(retry[0].replayed);

        let duplicate_id = DeliveryCandidate {
            delivery_id: retry[0].delivery_id,
            ..candidate(1)
        };
        assert!(g.claim("a", 11, 20, vec![duplicate_id]).is_empty());

        let mut filtered = group();
        filtered.progress.get_mut("s").unwrap().next_version = 1;
        assert!(filtered.claim("a", 1, 2, vec![candidate(0)]).is_empty());
        let mut replayed = candidate(1);
        replayed.replayed = true;
        assert!(filtered.claim("a", 1, 2, vec![replayed]).is_empty());
        filtered.progress.get_mut("s").unwrap().resolved_gaps.insert(1);
        assert!(filtered.claim("a", 1, 2, vec![candidate(1)]).is_empty());

        let mut duplicate_version = group();
        duplicate_version.claim("a", 1, 10, vec![candidate(0)]);
        assert!(duplicate_version
            .claim("a", 1, 10, vec![candidate(0)])
            .is_empty());
    }

    #[test]
    fn settle_and_expiration_report_all_boundary_results() {
        let mut g = group();
        let claimed = g.claim("a", 10, 20, vec![candidate(0), candidate(1)]);
        let first = Settlement {
            delivery_id: claimed[0].delivery_id,
            action: SettlementAction::Park,
            reason: "manual".into(),
        };
        assert_eq!(
            g.settle("a", 2, 11, std::slice::from_ref(&first)),
            vec![SettlementResult::StaleLease]
        );
        assert_eq!(
            g.settle("other", 1, 11, std::slice::from_ref(&first)),
            vec![SettlementResult::WrongConsumer]
        );
        assert_eq!(
            g.settle("a", 1, 11, std::slice::from_ref(&first)),
            vec![SettlementResult::Applied]
        );
        assert_eq!(g.parked.len(), 1);
        assert_eq!(
            g.settle(
                "a",
                1,
                11,
                &[Settlement {
                    delivery_id: claimed[1].delivery_id,
                    action: SettlementAction::Skip,
                    reason: String::new(),
                }],
            ),
            vec![SettlementResult::Applied]
        );

        let mut expiring = group();
        expiring.settings.max_retries = 1;
        expiring.claim("a", 10, 20, vec![candidate(0)]);
        assert_eq!(expiring.expire(19), 0);
        assert_eq!(expiring.expire(20), 1);
        assert_eq!(expiring.pending_retries.len(), 1);
        assert_eq!(expiring.replay_parked(21), 0);
    }

    #[test]
    fn resolving_old_or_unknown_delivery_is_idempotent() {
        let mut progress = StreamProgress::new(2, 1);
        progress.resolve(1);
        assert_eq!(progress.next_version, 2);

        let mut g = group();
        let delivery = g.claim("a", 1, 100, vec![candidate(0)])[0].clone();
        g.progress.remove("s");
        g.resolve_delivery(&delivery);
        assert!(g.progress.is_empty());

        let mut lease_group = group();
        let delivery = lease_group.claim("a", 1, 100, vec![candidate(0)])[0].clone();
        lease_group.progress.get_mut("s").unwrap().lease = Some(StreamLease {
            consumer_id: "a".into(),
            group_epoch: 1,
            deadline_ms: 5,
        });
        lease_group.settle(
            "a",
            1,
            10,
            &[Settlement {
                delivery_id: Uuid::new_v4(),
                action: SettlementAction::Ack,
                reason: String::new(),
            }],
        );
        assert!(lease_group.deliveries.contains_key(&delivery.delivery_id));
        assert!(lease_group.progress["s"].lease.is_none());
    }

    proptest! {
        #[test]
        fn random_ack_order_never_moves_checkpoint_back(order in prop::collection::vec(0usize..16, 1..64)) {
            let mut g = group();
            let candidates: Vec<_> = (0..16).map(candidate).collect();
            let claimed = g.claim("a", 1, 100, candidates);
            let mut previous = 0;
            for index in order {
                if let Some(delivery) = claimed.get(index) {
                    g.settle("a", 1, 2, &[Settlement {
                        delivery_id: delivery.delivery_id,
                        action: SettlementAction::Ack,
                        reason: String::new(),
                    }]);
                    let current = g.progress["s"].next_version;
                    prop_assert!(current >= previous);
                    previous = current;
                }
            }
        }

        #[test]
        fn random_ownership_snapshots_never_regress_generation(
            generations in prop::collection::vec(0u64..64, 1..128)
        ) {
            let mut g = group();
            let mut expected_generation = 1u64;
            let mut expected_revision = g.revision;
            for generation in generations {
                let changed = g.reconcile_ownership(BTreeMap::from([("s".into(), generation)]));
                if generation > expected_generation {
                    expected_generation = generation;
                    expected_revision = expected_revision.saturating_add(1);
                    prop_assert!(changed);
                } else {
                    prop_assert!(!changed);
                }
                prop_assert_eq!(g.progress["s"].ownership_generation, expected_generation);
                prop_assert_eq!(g.revision, expected_revision);
            }
        }
    }
}
