//! 持久化拉取订阅公共服务。
//!
//! 控制 Shard leader 负责组协调；事件仍从当前归属的数据 Shard leader 读取。
//! Fetch 先读取候选，再通过控制 Shard Raft 原子 claim，响应中不暴露 shard。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use es_core::persistent::{
    DEFAULT_FETCH_BYTES, DEFAULT_FETCH_EVENTS, DEFAULT_FETCH_WAIT_MS, MAX_FETCH_BYTES,
    MAX_FETCH_EVENTS, MAX_FETCH_WAIT_MS,
};
use es_core::{
    DeliveryCandidate, PersistentGroup, PersistentSettings, PersistentTarget,
    Settlement as CoreSettlement, SettlementAction, SettlementResult as CoreSettlementResult,
    StreamProgress,
};
use es_proto::eventstore::persistent_subscriptions_server::PersistentSubscriptions;
use es_proto::eventstore::*;
use es_storage::{
    EsRequest, EsResponse, PersistentSubscriptionCommand, PersistentSubscriptionResponse,
};
use prost::Message as _;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::service::EsService;
use crate::service::{client_write_to_status, public_subscription_event, read_persistent_local};

const MAX_EVENTS_PER_STREAM_PER_FETCH: u32 = 32;
const POLL_INTERVAL: Duration = Duration::from_millis(50);

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn parse_uuid(bytes: &[u8], field: &str) -> Result<Uuid, Status> {
    Uuid::from_slice(bytes)
        .map_err(|_| Status::invalid_argument(format!("{field} 必须是 16 字节 UUID")))
}

fn parse_target(target: PersistentSubscriptionTarget) -> Result<PersistentTarget, Status> {
    match target.target {
        Some(persistent_subscription_target::Target::Streams(streams)) => {
            let streams: BTreeSet<String> = streams
                .stream_ids
                .into_iter()
                .filter(|stream| !stream.is_empty())
                .collect();
            if streams.is_empty() {
                return Err(Status::invalid_argument("Streams 目标不能为空"));
            }
            Ok(PersistentTarget::Streams(streams))
        }
        Some(persistent_subscription_target::Target::All(_)) => Ok(PersistentTarget::All),
        None => Err(Status::invalid_argument("target is required")),
    }
}

fn proto_target(target: &PersistentTarget) -> PersistentSubscriptionTarget {
    let target = match target {
        PersistentTarget::Streams(streams) => {
            persistent_subscription_target::Target::Streams(SubscribeStreams {
                stream_ids: streams.iter().cloned().collect(),
            })
        }
        PersistentTarget::All => persistent_subscription_target::Target::All(Empty {}),
    };
    PersistentSubscriptionTarget {
        target: Some(target),
    }
}

fn parse_settings(
    settings: Option<PersistentSubscriptionSettings>,
) -> Result<PersistentSettings, Status> {
    let defaults = PersistentSettings::default();
    let settings = settings.unwrap_or_default();
    let parsed = PersistentSettings {
        max_unacked_per_consumer: if settings.max_unacked_per_consumer == 0 {
            defaults.max_unacked_per_consumer
        } else {
            settings.max_unacked_per_consumer
        },
        max_unacked_per_group: if settings.max_unacked_per_group == 0 {
            defaults.max_unacked_per_group
        } else {
            settings.max_unacked_per_group
        },
        ack_timeout_ms: if settings.ack_timeout_ms == 0 {
            defaults.ack_timeout_ms
        } else {
            settings.ack_timeout_ms
        },
        max_retries: if settings.max_retries == 0 {
            defaults.max_retries
        } else {
            settings.max_retries
        },
        retry_min_ms: if settings.retry_min_ms == 0 {
            defaults.retry_min_ms
        } else {
            settings.retry_min_ms
        },
        retry_max_ms: if settings.retry_max_ms == 0 {
            defaults.retry_max_ms
        } else {
            settings.retry_max_ms
        },
    };
    parsed.validate().map_err(Status::invalid_argument)?;
    Ok(parsed)
}

fn proto_settings(settings: &PersistentSettings) -> PersistentSubscriptionSettings {
    PersistentSubscriptionSettings {
        max_unacked_per_consumer: settings.max_unacked_per_consumer,
        max_unacked_per_group: settings.max_unacked_per_group,
        ack_timeout_ms: settings.ack_timeout_ms,
        max_retries: settings.max_retries,
        retry_min_ms: settings.retry_min_ms,
        retry_max_ms: settings.retry_max_ms,
    }
}

fn group_info(group: &PersistentGroup) -> PersistentSubscriptionInfo {
    PersistentSubscriptionInfo {
        name: group.name.clone(),
        revision: group.revision,
        epoch: group.epoch,
        target: Some(proto_target(&group.target)),
        settings: Some(proto_settings(&group.settings)),
        stream_count: group.progress.len() as u64,
        active_delivery_count: group.deliveries.len() as u64,
        parked_count: group.parked.len() as u64,
    }
}

impl EsService {
    async fn persistent_control_leader(&self) -> Result<Arc<es_raft::Shard>, Status> {
        let shard_id = self.ownership.control_shard_id();
        let shard = self
            .shard_manager
            .get_shard(shard_id)
            .await
            .map_err(|_| Status::unavailable("control shard not on this node"))?;
        if !shard.raft.metrics().borrow().state.is_leader() {
            return Err(self.remote.leader_hint_status(shard_id).await);
        }
        Ok(shard)
    }

    async fn submit_persistent(
        &self,
        command: PersistentSubscriptionCommand,
    ) -> Result<PersistentSubscriptionResponse, Status> {
        let shard = self.persistent_control_leader().await?;
        let response = shard
            .raft
            .client_write(EsRequest::PersistentSubscription { command })
            .await
            .map_err(client_write_to_status)?;
        match response.data {
            EsResponse::PersistentSubscription(response) => Ok(response),
            other => Err(Status::internal(format!(
                "持久化订阅返回意外结果: {other:?}"
            ))),
        }
    }

    async fn read_persistent_group(&self, name: &str) -> Result<PersistentGroup, Status> {
        let shard = self.persistent_control_leader().await?;
        shard
            .storage
            .read_persistent_group(name)
            .map_err(|error| Status::internal(format!("读取持久化订阅组失败: {error}")))?
            .ok_or_else(|| Status::not_found(format!("persistent subscription '{name}' not found")))
    }

    async fn read_source(
        &self,
        shard_id: u64,
        cursors: Vec<InternalPersistentCursor>,
        max_events: u32,
        max_bytes: u64,
    ) -> Result<InternalPersistentReadResponse, Status> {
        if let Ok(shard) = self.shard_manager.get_shard(shard_id).await {
            if shard.raft.metrics().borrow().state.is_leader() {
                return read_persistent_local(
                    shard_id,
                    &shard.storage,
                    &cursors,
                    max_events,
                    max_bytes,
                );
            }
        }
        let mut client = self.remote.internal_client(shard_id).await?;
        client
            .read_persistent_batch(InternalPersistentReadRequest {
                shard_id,
                cursors,
                max_events,
                max_bytes,
            })
            .await
            .map(|response| response.into_inner())
    }

    async fn heads_for(
        &self,
        streams: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, (u64, u64)>, Status> {
        let table = self.route_table.snapshot().await;
        let mut by_shard: BTreeMap<u64, Vec<String>> = BTreeMap::new();
        for stream in streams {
            let shard = table
                .lookup(stream)
                .ok_or_else(|| Status::not_found(format!("stream '{stream}' not found")))?;
            by_shard.entry(shard).or_default().push(stream.clone());
        }
        let mut result = BTreeMap::new();
        for (shard, streams) in by_shard {
            let cursors = streams
                .iter()
                .map(|stream| InternalPersistentCursor {
                    stream_id: stream.clone(),
                    from_version: 0,
                    max_count: 0,
                })
                .collect();
            let response = self.read_source(shard, cursors, 0, 0).await?;
            for head in response.heads {
                if !head.exists {
                    return Err(Status::not_found(format!(
                        "stream '{}' has ownership but no data",
                        head.stream_id
                    )));
                }
                let generation = table
                    .stream_generations
                    .get(&head.stream_id)
                    .copied()
                    .unwrap_or(1)
                    .max(1);
                result.insert(head.stream_id, (head.current_version, generation));
            }
        }
        Ok(result)
    }

    async fn initial_progress(
        &self,
        target: &PersistentTarget,
        start: Option<PersistentStartSpec>,
    ) -> Result<BTreeMap<String, StreamProgress>, Status> {
        let table = self.route_table.snapshot().await;
        let streams: BTreeSet<String> = match target {
            PersistentTarget::Streams(streams) => streams.clone(),
            PersistentTarget::All => table.streams.keys().cloned().collect(),
        };
        let start = start.unwrap_or_default();
        if start
            .next_versions
            .keys()
            .any(|stream| !streams.contains(stream))
        {
            return Err(Status::invalid_argument(
                "next_versions 只能覆盖当前目标中的 Stream",
            ));
        }
        let heads = self.heads_for(&streams).await?;
        let mut progress = BTreeMap::new();
        for stream in streams {
            let (head, generation) = heads[&stream];
            let next = match start.next_versions.get(&stream) {
                Some(next) if *next <= head.saturating_add(1) => *next,
                Some(_) => {
                    return Err(Status::invalid_argument(format!(
                        "stream '{stream}' 的 next_version 超过 head + 1"
                    )));
                }
                None if start.default == PersistentStartDefault::PersistentStartNow as i32 => {
                    head.saturating_add(1)
                }
                None => 0,
            };
            progress.insert(stream, StreamProgress::new(next, generation));
        }
        Ok(progress)
    }

    async fn reconcile_all(&self, mut group: PersistentGroup) -> Result<PersistentGroup, Status> {
        if !matches!(group.target, PersistentTarget::All) {
            return Ok(group);
        }
        let table = self.route_table.snapshot().await;
        let missing: BTreeSet<String> = table
            .streams
            .keys()
            .filter(|stream| !group.progress.contains_key(*stream))
            .cloned()
            .collect();
        if missing.is_empty() {
            return Ok(group);
        }
        let heads = self.heads_for(&missing).await?;
        let streams = heads
            .into_iter()
            .map(|(stream, (_, generation))| (stream, StreamProgress::new(0, generation)))
            .collect();
        match self
            .submit_persistent(PersistentSubscriptionCommand::EnsureStreams {
                name: group.name.clone(),
                streams,
            })
            .await?
        {
            PersistentSubscriptionResponse::Group(updated) => {
                group = updated;
                Ok(group)
            }
            PersistentSubscriptionResponse::NotFound => Err(Status::not_found("group not found")),
            other => Err(Status::internal(format!(
                "EnsureStreams 返回意外结果: {other:?}"
            ))),
        }
    }

    async fn reconcile_ownership(
        &self,
        mut group: PersistentGroup,
    ) -> Result<(PersistentGroup, es_core::route::RouteTable), Status> {
        let table = self.route_table.snapshot().await;
        let generations: BTreeMap<String, u64> = group
            .progress
            .iter()
            .filter_map(|(stream, progress)| {
                table.lookup(stream)?;
                let generation = table
                    .stream_generations
                    .get(stream)
                    .copied()
                    .unwrap_or(1)
                    .max(1);
                (generation > progress.ownership_generation).then(|| (stream.clone(), generation))
            })
            .collect();
        if generations.is_empty() {
            return Ok((group, table));
        }
        match self
            .submit_persistent(PersistentSubscriptionCommand::ReconcileOwnership {
                name: group.name.clone(),
                generations,
            })
            .await?
        {
            PersistentSubscriptionResponse::Group(updated) => {
                group = updated;
                Ok((group, table))
            }
            PersistentSubscriptionResponse::NotFound => Err(Status::not_found("group not found")),
            other => Err(Status::internal(format!(
                "ReconcileOwnership 返回意外结果: {other:?}"
            ))),
        }
    }

    async fn collect_candidates(
        &self,
        group: &PersistentGroup,
        table: &es_core::route::RouteTable,
        consumer_id: &str,
        max_events: u32,
        max_bytes: u64,
        now: u64,
    ) -> Result<
        (
            Vec<DeliveryCandidate>,
            BTreeMap<(String, u64), Event>,
            bool,
            u64,
        ),
        Status,
    > {
        let mut requested: BTreeMap<u64, Vec<InternalPersistentCursor>> = BTreeMap::new();
        let mut seen = BTreeSet::new();
        let mut future_retry = None::<u64>;

        for retry in group.pending_retries.values() {
            if retry.not_before_ms > now {
                future_retry = Some(
                    future_retry
                        .map_or(retry.not_before_ms, |value| value.min(retry.not_before_ms)),
                );
                continue;
            }
            let Some(shard) = table.lookup(&retry.stream_id) else {
                continue;
            };
            requested
                .entry(shard)
                .or_default()
                .push(InternalPersistentCursor {
                    stream_id: retry.stream_id.clone(),
                    from_version: retry.version,
                    max_count: 1,
                });
            seen.insert(retry.stream_id.clone());
        }

        let mut streams: Vec<String> = group.progress.keys().cloned().collect();
        if let Some(after) = &group.scan_after {
            let split = streams.partition_point(|stream| stream <= after);
            let stream_count = streams.len();
            streams.rotate_left(split.min(stream_count));
        }
        let remaining =
            max_events.saturating_sub(requested.values().map(Vec::len).sum::<usize>() as u32);
        let mut selected = 0u32;
        for stream in streams {
            if selected >= remaining {
                break;
            }
            if seen.contains(&stream) {
                continue;
            }
            let progress = &group.progress[&stream];
            if progress
                .lease
                .as_ref()
                .is_some_and(|lease| lease.consumer_id != consumer_id && lease.deadline_ms > now)
            {
                continue;
            }
            if group
                .pending_retries
                .contains_key(&(stream.clone(), progress.next_version))
            {
                continue;
            }
            let Some(shard) = table.lookup(&stream) else {
                continue;
            };
            let mut from_version = progress.next_version;
            for version in progress.resolved_gaps.iter().copied().chain(
                group
                    .deliveries
                    .values()
                    .filter(|delivery| delivery.stream_id == stream)
                    .map(|delivery| delivery.version),
            ) {
                from_version = from_version.max(version.saturating_add(1));
            }
            requested
                .entry(shard)
                .or_default()
                .push(InternalPersistentCursor {
                    stream_id: stream,
                    from_version,
                    max_count: MAX_EVENTS_PER_STREAM_PER_FETCH.min(remaining.max(1)),
                });
            selected = selected.saturating_add(1);
        }

        let mut events = BTreeMap::new();
        let mut used_bytes = 0u64;
        for (shard, cursors) in requested {
            let remaining_events = max_events.saturating_sub(events.len() as u32);
            if remaining_events == 0 {
                break;
            }
            let response = self
                .read_source(
                    shard,
                    cursors,
                    remaining_events,
                    max_bytes.saturating_sub(used_bytes),
                )
                .await?;
            for event in response.events {
                let encoded = event.encoded_len() as u64;
                if !events.is_empty() && used_bytes.saturating_add(encoded) > max_bytes {
                    break;
                }
                used_bytes = used_bytes.saturating_add(encoded);
                events.insert((event.stream_id.clone(), event.version), event);
                if events.len() as u32 >= max_events {
                    break;
                }
            }
        }

        let mut candidates = Vec::with_capacity(events.len());
        for ((stream, version), event) in &events {
            let event_id = parse_uuid(&event.event_id, "event_id")?;
            let replayed = group
                .pending_retries
                .get(&(stream.clone(), *version))
                .is_some_and(|retry| retry.replayed);
            candidates.push(DeliveryCandidate {
                delivery_id: Uuid::new_v4(),
                stream_id: stream.clone(),
                version: *version,
                event_id,
                replayed,
            });
        }
        let caught_up = candidates.is_empty() && future_retry.is_none();
        let retry_after_ms = future_retry
            .map(|deadline| deadline.saturating_sub(now))
            .unwrap_or(0);
        Ok((candidates, events, caught_up, retry_after_ms))
    }

    async fn fetch_once(
        &self,
        name: &str,
        consumer_id: &str,
        max_events: u32,
        max_bytes: u64,
    ) -> Result<FetchPersistentSubscriptionResponse, Status> {
        let now = now_ms();
        let mut group = self.read_persistent_group(name).await?;
        if group
            .deliveries
            .values()
            .any(|delivery| delivery.deadline_ms <= now)
        {
            self.submit_persistent(PersistentSubscriptionCommand::Expire {
                name: name.to_string(),
                now_ms: now,
            })
            .await?;
            group = self.read_persistent_group(name).await?;
        }
        group = self.reconcile_all(group).await?;
        let (group, table) = self.reconcile_ownership(group).await?;
        let credit = group.available_credit(consumer_id);
        if credit == 0 {
            let retry_after_ms = group
                .deliveries
                .values()
                .filter(|delivery| delivery.consumer_id == consumer_id)
                .map(|delivery| delivery.deadline_ms.saturating_sub(now))
                .min()
                .unwrap_or(group.settings.ack_timeout_ms);
            return Ok(FetchPersistentSubscriptionResponse {
                deliveries: Vec::new(),
                caught_up: false,
                throttled: true,
                retry_after_ms,
            });
        }
        let limit = max_events.min(credit);
        let (candidates, mut event_map, caught_up, retry_after_ms) = self
            .collect_candidates(&group, &table, consumer_id, limit, max_bytes, now)
            .await?;
        if candidates.is_empty() {
            return Ok(FetchPersistentSubscriptionResponse {
                deliveries: Vec::new(),
                caught_up,
                throttled: false,
                retry_after_ms,
            });
        }
        let deadline_ms = now.saturating_add(group.settings.ack_timeout_ms);
        let claimed = match self
            .submit_persistent(PersistentSubscriptionCommand::Claim {
                name: name.to_string(),
                consumer_id: consumer_id.to_string(),
                now_ms: now,
                deadline_ms,
                candidates,
            })
            .await?
        {
            PersistentSubscriptionResponse::Claimed(claimed) => claimed,
            PersistentSubscriptionResponse::NotFound => {
                return Err(Status::not_found("group not found"));
            }
            other => return Err(Status::internal(format!("Claim 返回意外结果: {other:?}"))),
        };
        let deliveries = claimed
            .into_iter()
            .filter_map(|delivery| {
                let event = event_map.remove(&(delivery.stream_id.clone(), delivery.version))?;
                Some(PersistentDelivery {
                    delivery_id: delivery.delivery_id.as_bytes().to_vec(),
                    event: Some(public_subscription_event(event)),
                    attempt: delivery.attempt,
                    lease_deadline_ms: delivery.deadline_ms,
                    group_epoch: delivery.group_epoch,
                    replayed: delivery.replayed,
                })
            })
            .collect();
        Ok(FetchPersistentSubscriptionResponse {
            deliveries,
            caught_up: false,
            throttled: false,
            retry_after_ms: 0,
        })
    }
}

#[tonic::async_trait]
impl PersistentSubscriptions for EsService {
    async fn create_persistent_subscription(
        &self,
        request: Request<CreatePersistentSubscriptionRequest>,
    ) -> Result<Response<PersistentSubscriptionInfo>, Status> {
        let request = request.into_inner();
        let target = parse_target(
            request
                .target
                .ok_or_else(|| Status::invalid_argument("target is required"))?,
        )?;
        let settings = parse_settings(request.settings)?;
        let progress = self.initial_progress(&target, request.start).await?;
        let group = PersistentGroup::new(request.name, target, settings, progress)
            .map_err(Status::invalid_argument)?;
        match self
            .submit_persistent(PersistentSubscriptionCommand::Create { group })
            .await?
        {
            PersistentSubscriptionResponse::Group(group) => Ok(Response::new(group_info(&group))),
            PersistentSubscriptionResponse::Conflict { .. } => Err(Status::already_exists(
                "persistent subscription already exists",
            )),
            PersistentSubscriptionResponse::Invalid { reason } => {
                Err(Status::invalid_argument(reason))
            }
            other => Err(Status::internal(format!("Create 返回意外结果: {other:?}"))),
        }
    }

    async fn update_persistent_subscription(
        &self,
        request: Request<UpdatePersistentSubscriptionRequest>,
    ) -> Result<Response<PersistentSubscriptionInfo>, Status> {
        let request = request.into_inner();
        let mut group = self.read_persistent_group(&request.name).await?;
        let target = request
            .target
            .map(parse_target)
            .transpose()?
            .unwrap_or_else(|| group.target.clone());
        let settings = match request.settings {
            Some(settings) => parse_settings(Some(settings))?,
            None => group.settings.clone(),
        };
        let target_changed = target != group.target;
        let mut resets = BTreeMap::new();
        for reset in request.resets {
            if !target.contains(&reset.stream_id) {
                return Err(Status::invalid_argument(format!(
                    "reset Stream '{}' 不在更新后的目标中",
                    reset.stream_id
                )));
            }
            resets.insert(reset.stream_id.clone(), reset.start);
        }

        let desired: BTreeSet<String> = match &target {
            PersistentTarget::Streams(streams) => streams.clone(),
            PersistentTarget::All => self
                .route_table
                .snapshot()
                .await
                .streams
                .keys()
                .cloned()
                .collect(),
        };
        let reset_streams: BTreeSet<String> = resets.keys().cloned().collect();
        let added: BTreeSet<String> = desired
            .iter()
            .filter(|stream| !group.progress.contains_key(*stream))
            .cloned()
            .collect();
        let implicit_all: BTreeSet<String> =
            if !target_changed && matches!(target, PersistentTarget::All) {
                added.difference(&reset_streams).cloned().collect()
            } else {
                BTreeSet::new()
            };
        if added
            .iter()
            .any(|stream| !resets.contains_key(stream) && !implicit_all.contains(stream))
        {
            return Err(Status::invalid_argument(
                "新增目标 Stream 必须在 resets 中显式指定起点",
            ));
        }
        if !implicit_all.is_empty() {
            let heads = self.heads_for(&implicit_all).await?;
            for (stream, (_, generation)) in heads {
                group
                    .progress
                    .insert(stream, StreamProgress::new(0, generation));
            }
        }

        let heads = self.heads_for(&reset_streams).await?;
        for (stream, start) in resets {
            let (head, generation) = heads[&stream];
            let next = match start {
                Some(persistent_stream_reset::Start::Beginning(_)) => 0,
                Some(persistent_stream_reset::Start::Now(_)) => head.saturating_add(1),
                Some(persistent_stream_reset::Start::NextVersion(next))
                    if next <= head.saturating_add(1) =>
                {
                    next
                }
                Some(persistent_stream_reset::Start::NextVersion(_)) => {
                    return Err(Status::invalid_argument(format!(
                        "stream '{stream}' 的 reset next_version 超过 head + 1"
                    )));
                }
                None => return Err(Status::invalid_argument("reset start is required")),
            };
            group
                .progress
                .insert(stream.clone(), StreamProgress::new(next, generation));
            group.pending_retries.retain(|(item, _), _| item != &stream);
            group.parked.retain(|_, event| event.stream_id != stream);
        }
        group.progress.retain(|stream, _| desired.contains(stream));
        group
            .pending_retries
            .retain(|(stream, _), _| desired.contains(stream));
        group
            .parked
            .retain(|_, event| desired.contains(&event.stream_id));
        if target_changed || !reset_streams.is_empty() {
            let preserve_streams = desired.difference(&reset_streams).cloned().collect();
            group.begin_new_epoch(&preserve_streams);
        }
        group.target = target;
        group.settings = settings;
        group.revision = group.revision.saturating_add(1);
        match self
            .submit_persistent(PersistentSubscriptionCommand::Replace {
                name: request.name,
                expected_revision: request.expected_revision,
                group,
            })
            .await?
        {
            PersistentSubscriptionResponse::Group(group) => Ok(Response::new(group_info(&group))),
            PersistentSubscriptionResponse::Conflict { actual_revision } => Err(Status::aborted(
                format!("subscription revision conflict: actual_revision={actual_revision}"),
            )),
            PersistentSubscriptionResponse::NotFound => Err(Status::not_found("group not found")),
            PersistentSubscriptionResponse::Invalid { reason } => {
                Err(Status::invalid_argument(reason))
            }
            other => Err(Status::internal(format!("Update 返回意外结果: {other:?}"))),
        }
    }

    async fn delete_persistent_subscription(
        &self,
        request: Request<DeletePersistentSubscriptionRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        match self
            .submit_persistent(PersistentSubscriptionCommand::Delete {
                name: request.name,
                expected_revision: request.expected_revision,
            })
            .await?
        {
            PersistentSubscriptionResponse::Deleted => Ok(Response::new(Empty {})),
            PersistentSubscriptionResponse::Conflict { actual_revision } => Err(Status::aborted(
                format!("subscription revision conflict: actual_revision={actual_revision}"),
            )),
            PersistentSubscriptionResponse::NotFound => Err(Status::not_found("group not found")),
            other => Err(Status::internal(format!("Delete 返回意外结果: {other:?}"))),
        }
    }

    async fn get_persistent_subscription(
        &self,
        request: Request<GetPersistentSubscriptionRequest>,
    ) -> Result<Response<PersistentSubscriptionInfo>, Status> {
        let group = self
            .read_persistent_group(&request.into_inner().name)
            .await?;
        Ok(Response::new(group_info(&group)))
    }

    async fn list_persistent_subscriptions(
        &self,
        _request: Request<ListPersistentSubscriptionsRequest>,
    ) -> Result<Response<ListPersistentSubscriptionsResponse>, Status> {
        let shard = self.persistent_control_leader().await?;
        let subscriptions = shard
            .storage
            .list_persistent_groups()
            .map_err(|error| Status::internal(format!("枚举持久化订阅组失败: {error}")))?
            .iter()
            .map(group_info)
            .collect();
        Ok(Response::new(ListPersistentSubscriptionsResponse {
            subscriptions,
        }))
    }

    async fn fetch_persistent_subscription(
        &self,
        request: Request<FetchPersistentSubscriptionRequest>,
    ) -> Result<Response<FetchPersistentSubscriptionResponse>, Status> {
        let request = request.into_inner();
        if request.name.is_empty() || request.consumer_id.is_empty() {
            return Err(Status::invalid_argument(
                "name and consumer_id are required",
            ));
        }
        let max_events = if request.max_events == 0 {
            DEFAULT_FETCH_EVENTS
        } else {
            request.max_events
        };
        let max_bytes = if request.max_bytes == 0 {
            DEFAULT_FETCH_BYTES
        } else {
            request.max_bytes
        };
        let wait_ms = if request.wait_ms == 0 {
            DEFAULT_FETCH_WAIT_MS
        } else {
            request.wait_ms
        };
        if max_events > MAX_FETCH_EVENTS
            || max_bytes > MAX_FETCH_BYTES
            || wait_ms > MAX_FETCH_WAIT_MS
        {
            return Err(Status::invalid_argument(
                "Fetch 超过 max_events/max_bytes/wait_ms 上限",
            ));
        }
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let response = self
                .fetch_once(&request.name, &request.consumer_id, max_events, max_bytes)
                .await?;
            if !response.deliveries.is_empty() || response.throttled {
                return Ok(Response::new(response));
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(Response::new(response));
            }
            let sleep = if response.retry_after_ms == 0 {
                POLL_INTERVAL
            } else {
                POLL_INTERVAL.min(Duration::from_millis(response.retry_after_ms))
            };
            tokio::time::sleep(sleep).await;
        }
    }

    async fn settle_persistent_subscription(
        &self,
        request: Request<SettlePersistentSubscriptionRequest>,
    ) -> Result<Response<SettlePersistentSubscriptionResponse>, Status> {
        let request = request.into_inner();
        if request.consumer_id.is_empty() || request.settlements.is_empty() {
            return Err(Status::invalid_argument(
                "consumer_id 与 settlements 不能为空",
            ));
        }
        let mut ids = Vec::with_capacity(request.settlements.len());
        let mut settlements = Vec::with_capacity(request.settlements.len());
        for settlement in request.settlements {
            let id = parse_uuid(&settlement.delivery_id, "delivery_id")?;
            let action = match PersistentSettlementAction::try_from(settlement.action) {
                Ok(PersistentSettlementAction::PersistentSettlementAck) => SettlementAction::Ack,
                Ok(PersistentSettlementAction::PersistentSettlementRetry) => {
                    SettlementAction::Retry
                }
                Ok(PersistentSettlementAction::PersistentSettlementPark) => SettlementAction::Park,
                Ok(PersistentSettlementAction::PersistentSettlementSkip) => SettlementAction::Skip,
                Err(_) => return Err(Status::invalid_argument("unknown settlement action")),
            };
            ids.push(id);
            settlements.push(CoreSettlement {
                delivery_id: id,
                action,
                reason: settlement.reason,
            });
        }
        let settled = match self
            .submit_persistent(PersistentSubscriptionCommand::Settle {
                name: request.name,
                consumer_id: request.consumer_id,
                group_epoch: request.group_epoch,
                now_ms: now_ms(),
                settlements,
            })
            .await?
        {
            PersistentSubscriptionResponse::Settled(results) => results,
            PersistentSubscriptionResponse::NotFound => {
                return Err(Status::not_found("group not found"));
            }
            other => return Err(Status::internal(format!("Settle 返回意外结果: {other:?}"))),
        };
        let results = ids
            .into_iter()
            .zip(settled)
            .map(|(id, status)| PersistentSettlementResult {
                delivery_id: id.as_bytes().to_vec(),
                status: match status {
                    CoreSettlementResult::Applied => {
                        PersistentSettlementStatus::PersistentSettlementApplied as i32
                    }
                    CoreSettlementResult::AlreadySettled => {
                        PersistentSettlementStatus::PersistentSettlementAlreadySettled as i32
                    }
                    CoreSettlementResult::StaleLease => {
                        PersistentSettlementStatus::PersistentSettlementStaleLease as i32
                    }
                    CoreSettlementResult::WrongConsumer => {
                        PersistentSettlementStatus::PersistentSettlementWrongConsumer as i32
                    }
                },
            })
            .collect();
        Ok(Response::new(SettlePersistentSubscriptionResponse {
            results,
        }))
    }

    async fn list_parked_persistent_subscription(
        &self,
        request: Request<ListParkedPersistentSubscriptionRequest>,
    ) -> Result<Response<ListParkedPersistentSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let group = self.read_persistent_group(&request.name).await?;
        let (group, table) = self.reconcile_ownership(group).await?;
        let limit = if request.limit == 0 {
            100
        } else {
            request.limit.min(1000)
        } as usize;
        let selected: Vec<_> = group
            .parked
            .iter()
            .skip(request.offset as usize)
            .take(limit)
            .collect();
        let mut events = Vec::with_capacity(selected.len());
        for (id, parked) in selected {
            let event = match table.lookup(&parked.stream_id) {
                Some(shard) => self
                    .read_source(
                        shard,
                        vec![InternalPersistentCursor {
                            stream_id: parked.stream_id.clone(),
                            from_version: parked.version,
                            max_count: 1,
                        }],
                        1,
                        MAX_FETCH_BYTES,
                    )
                    .await?
                    .events
                    .into_iter()
                    .next()
                    .map(public_subscription_event),
                None => None,
            };
            events.push(ParkedPersistentEvent {
                parked_id: id.as_bytes().to_vec(),
                event,
                attempts: parked.attempts,
                reason: parked.reason.clone(),
                parked_at_ms: parked.parked_at_ms,
            });
        }
        let consumed = request.offset as usize + events.len();
        let next_offset = if consumed < group.parked.len() {
            consumed as u32
        } else {
            0
        };
        Ok(Response::new(ListParkedPersistentSubscriptionResponse {
            events,
            next_offset,
        }))
    }

    async fn replay_parked_persistent_subscription(
        &self,
        request: Request<ReplayParkedPersistentSubscriptionRequest>,
    ) -> Result<Response<ReplayParkedPersistentSubscriptionResponse>, Status> {
        let name = request.into_inner().name;
        let group = self.read_persistent_group(&name).await?;
        let _ = self.reconcile_ownership(group).await?;
        match self
            .submit_persistent(PersistentSubscriptionCommand::ReplayParked {
                name,
                now_ms: now_ms(),
            })
            .await?
        {
            PersistentSubscriptionResponse::Count(replayed_count) => {
                Ok(Response::new(ReplayParkedPersistentSubscriptionResponse {
                    replayed_count,
                }))
            }
            PersistentSubscriptionResponse::NotFound => Err(Status::not_found("group not found")),
            other => Err(Status::internal(format!(
                "ReplayParked 返回意外结果: {other:?}"
            ))),
        }
    }
}
