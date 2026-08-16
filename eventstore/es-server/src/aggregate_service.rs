//! AggregateStore gRPC module：隐藏 catalog、虚拟分区与跨节点 fan-out。

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use es_core::{AggregateCatalogOutcome, AggregateTypeId, AggregateTypeStatus};
use es_proto::eventstore::aggregate_store_internal_server::AggregateStoreInternal;
use es_proto::eventstore::aggregate_store_server::AggregateStore;
use es_proto::eventstore::*;
use es_raft::ShardManager;

use crate::config::Config;
use crate::rpc_support::{RuntimeTopology, client_write_to_status};

const CURSOR_VERSION: u8 = 1;
const STATE_PAGE_TOKEN_VERSION: u8 = 1;
const DEFAULT_STATE_PAGE_SIZE: u32 = 100;
const MAX_STATE_PAGE_SIZE: u32 = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AggregateCursor {
    version: u8,
    aggregate_type: AggregateTypeId,
    next_positions: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatePageToken {
    version: u8,
    aggregate_type: AggregateTypeId,
    after_aggregate_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupPartitionFetchInput {
    aggregate_type: AggregateTypeId,
    partition_id: u16,
    partition_generation: u64,
    group_name: String,
    group_epoch: u64,
    start_position: u64,
    settings: es_core::AggregateGroupSettings,
    consumer_id: String,
    now_ms: u64,
    max_events: u32,
    max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupPartitionFetchOutput {
    deliveries: Vec<(es_core::AggregateGroupDelivery, es_core::AggregateEvent)>,
    caught_up: bool,
    throttled: bool,
}

#[derive(Debug)]
enum AggregateSourceMessage {
    Event(u64, InternalAggregateEvent),
    CaughtUp(u64, Vec<AggregatePartitionCursor>),
    Degraded(u64),
}

struct AggregateReadMerger {
    cursor: AggregateCursor,
    pending: BTreeSet<u64>,
    unavailable: BTreeSet<u64>,
    caught_up_sent: bool,
}

impl AggregateReadMerger {
    fn new(cursor: AggregateCursor, source_ids: BTreeSet<u64>) -> Self {
        Self {
            cursor,
            pending: source_ids,
            unavailable: BTreeSet::new(),
            caught_up_sent: false,
        }
    }

    fn apply(&mut self, message: AggregateSourceMessage) -> Vec<FollowAggregateTypeEventsResponse> {
        match message {
            AggregateSourceMessage::Event(_source, internal) => {
                let Ok(partition_id) = u16::try_from(internal.partition_id) else {
                    return Vec::new();
                };
                let Some(next_position) = self
                    .cursor
                    .next_positions
                    .get_mut(usize::from(partition_id))
                else {
                    return Vec::new();
                };
                *next_position = internal.partition_position.saturating_add(1);
                let Some(event) = internal.event else {
                    return Vec::new();
                };
                vec![FollowAggregateTypeEventsResponse {
                    payload: Some(follow_aggregate_type_events_response::Payload::Event(event)),
                    cursor: encode_cursor(&self.cursor).unwrap_or_default(),
                }]
            }
            AggregateSourceMessage::CaughtUp(source, heads) => {
                for head in heads {
                    let Ok(partition_id) = u16::try_from(head.partition_id) else {
                        continue;
                    };
                    if let Some(next_position) = self
                        .cursor
                        .next_positions
                        .get_mut(usize::from(partition_id))
                    {
                        *next_position = (*next_position).max(head.next_position);
                    }
                }
                self.pending.remove(&source);
                let recovered = self.unavailable.remove(&source) && self.unavailable.is_empty();
                let mut responses = Vec::with_capacity(2);
                if recovered && self.caught_up_sent {
                    responses.push(FollowAggregateTypeEventsResponse {
                        payload: Some(follow_aggregate_type_events_response::Payload::Recovered(
                            Empty {},
                        )),
                        cursor: encode_cursor(&self.cursor).unwrap_or_default(),
                    });
                }
                if self.pending.is_empty() && !self.caught_up_sent {
                    self.caught_up_sent = true;
                    responses.push(FollowAggregateTypeEventsResponse {
                        payload: Some(follow_aggregate_type_events_response::Payload::CaughtUp(
                            Empty {},
                        )),
                        cursor: encode_cursor(&self.cursor).unwrap_or_default(),
                    });
                }
                responses
            }
            AggregateSourceMessage::Degraded(source) => {
                self.pending.insert(source);
                if self.unavailable.insert(source) {
                    vec![FollowAggregateTypeEventsResponse {
                        payload: Some(follow_aggregate_type_events_response::Payload::Degraded(
                            AggregateReadDegraded {
                                unavailable_source_count: self.unavailable.len() as u32,
                                retrying: true,
                            },
                        )),
                        cursor: encode_cursor(&self.cursor).unwrap_or_default(),
                    }]
                } else {
                    Vec::new()
                }
            }
        }
    }
}

/// AggregateStore 的公共与内部 gRPC 实现。
#[derive(Clone)]
pub struct AggregateStoreService {
    shard_manager: Arc<ShardManager>,
    topology: RuntimeTopology,
    max_event_bytes: u64,
}

impl AggregateStoreService {
    /// 使用服务器配置构造 AggregateStore module。
    ///
    /// - `shard_manager`：本节点承载的 Raft Shard 集合。
    /// - `config`：节点地址、放置和大小限制。
    /// - 返回：可同时注册到公共与内部端口的服务。
    /// - 错误：放置为空、peer/TLS 地址非法时返回配置错误。
    pub fn new(shard_manager: Arc<ShardManager>, config: &Config) -> Result<Self, String> {
        let topology = RuntimeTopology::new(config)?;
        Ok(Self {
            shard_manager,
            topology,
            max_event_bytes: config.limits.max_event_bytes,
        })
    }

    pub(crate) fn with_topology(
        shard_manager: Arc<ShardManager>,
        topology: RuntimeTopology,
        max_event_bytes: u64,
    ) -> Self {
        Self {
            shard_manager,
            topology,
            max_event_bytes,
        }
    }

    async fn local_leader(&self, shard_id: u64) -> Result<Arc<es_raft::Shard>, Status> {
        let shard = match self.shard_manager.get_shard(shard_id).await {
            Ok(shard) => shard,
            Err(_) => {
                return Err(self
                    .topology
                    .snapshot()
                    .await
                    .remote
                    .leader_hint_status(shard_id)
                    .await);
            }
        };
        if !shard.raft.metrics().borrow().state.is_leader() {
            return Err(self
                .topology
                .snapshot()
                .await
                .remote
                .leader_hint_status(shard_id)
                .await);
        }
        Ok(shard)
    }

    async fn require_control_shard(&self, shard_id: u64) -> Result<(), Status> {
        if shard_id != self.topology.snapshot().await.control_shard_id {
            return Err(Status::invalid_argument("control_shard_id 不匹配"));
        }
        Ok(())
    }

    async fn fetch_catalog(&self) -> Result<es_core::AggregateCatalog, Status> {
        let topology = self.topology.snapshot().await;
        if let Ok(shard) = self.local_leader(topology.control_shard_id).await {
            return shard
                .storage
                .read_aggregate_catalog()
                .map_err(|error| Status::internal(format!("读取聚合 catalog 失败: {error}")));
        }
        let mut client = topology
            .remote
            .aggregate_internal_client(topology.control_shard_id)
            .await?;
        let response = client
            .get_aggregate_catalog_internal(GetAggregateCatalogInternalRequest {
                control_shard_id: topology.control_shard_id,
            })
            .await?
            .into_inner();
        decode_bincode(&response.payload, "AggregateCatalog")
    }

    async fn commit_catalog(
        &self,
        command: es_core::AggregateCatalogCommand,
    ) -> Result<es_core::AggregateCatalogApply, Status> {
        let topology = self.topology.snapshot().await;
        if let Ok(shard) = self.local_leader(topology.control_shard_id).await {
            let response = shard
                .raft
                .client_write(es_storage::EsRequest::CommitAggregateCatalog { command })
                .await
                .map_err(client_write_to_status)?;
            return match response.data {
                es_storage::EsResponse::AggregateCatalogApplied(applied) => Ok(applied),
                other => Err(Status::internal(format!(
                    "聚合 catalog 返回意外结果: {other:?}"
                ))),
            };
        }
        let payload = encode_bincode(&command, "AggregateCatalogCommand")?;
        let mut client = topology
            .remote
            .aggregate_internal_client(topology.control_shard_id)
            .await?;
        let response = client
            .commit_aggregate_catalog_internal(CommitAggregateCatalogInternalRequest {
                control_shard_id: topology.control_shard_id,
                payload,
            })
            .await?
            .into_inner();
        decode_bincode(&response.payload, "AggregateCatalogApply")
    }

    async fn fetch_group_catalog(&self) -> Result<es_core::AggregateGroupCatalog, Status> {
        let topology = self.topology.snapshot().await;
        if let Ok(shard) = self.local_leader(topology.control_shard_id).await {
            return shard
                .storage
                .read_aggregate_group_catalog()
                .map_err(|error| Status::internal(format!("读取消费者组 catalog 失败: {error}")));
        }
        let mut client = topology
            .remote
            .aggregate_internal_client(topology.control_shard_id)
            .await?;
        let response = client
            .get_aggregate_group_catalog_internal(GetAggregateGroupCatalogInternalRequest {
                control_shard_id: topology.control_shard_id,
            })
            .await?
            .into_inner();
        decode_bincode(&response.payload, "AggregateGroupCatalog")
    }

    async fn commit_group_catalog(
        &self,
        command: es_core::AggregateGroupCatalogCommand,
    ) -> Result<es_core::AggregateGroupCatalogApply, Status> {
        let topology = self.topology.snapshot().await;
        if let Ok(shard) = self.local_leader(topology.control_shard_id).await {
            let response = shard
                .raft
                .client_write(es_storage::EsRequest::CommitAggregateGroupCatalog { command })
                .await
                .map_err(client_write_to_status)?;
            return match response.data {
                es_storage::EsResponse::AggregateGroupCatalogApplied(applied) => Ok(applied),
                other => Err(Status::internal(format!(
                    "消费者组 catalog 返回意外结果: {other:?}"
                ))),
            };
        }
        let payload = encode_bincode(&command, "AggregateGroupCatalogCommand")?;
        let mut client = topology
            .remote
            .aggregate_internal_client(topology.control_shard_id)
            .await?;
        let response = client
            .commit_aggregate_group_catalog_internal(CommitAggregateGroupCatalogInternalRequest {
                control_shard_id: topology.control_shard_id,
                payload,
            })
            .await?
            .into_inner();
        decode_bincode(&response.payload, "AggregateGroupCatalogApply")
    }

    async fn require_group(
        &self,
        aggregate_type: &AggregateTypeId,
        name: &str,
    ) -> Result<es_core::AggregateGroupDefinition, Status> {
        es_core::validate_aggregate_identifier("group_name", name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.fetch_group_catalog()
            .await?
            .groups
            .get(&(aggregate_type.clone(), name.to_string()))
            .cloned()
            .ok_or_else(|| Status::not_found("消费者组不存在"))
    }

    async fn capture_group_starts(
        &self,
        definition: &es_core::AggregateTypeDefinition,
        start: es_core::AggregateGroupStart,
    ) -> Result<BTreeMap<u16, u64>, Status> {
        if start == es_core::AggregateGroupStart::Beginning {
            return Ok((0..definition.partition_count)
                .map(|partition| (partition, 0))
                .collect());
        }
        let mut grouped: BTreeMap<u64, Vec<AggregatePartitionCursor>> = BTreeMap::new();
        for (partition_id, placement) in &definition.placements {
            grouped
                .entry(placement.shard_id)
                .or_default()
                .push(AggregatePartitionCursor {
                    partition_id: u32::from(*partition_id),
                    next_position: 0,
                });
        }
        let mut starts = BTreeMap::new();
        for (shard_id, cursors) in grouped {
            let mut stream = self
                .open_source_stream(
                    shard_id,
                    SubscribeAggregatePartitionsInternalRequest {
                        shard_id,
                        aggregate_type: Some(aggregate_type_to_proto(&definition.id)),
                        cursors,
                        from_now: true,
                    },
                )
                .await?;
            loop {
                let frame = stream
                    .next()
                    .await
                    .ok_or_else(|| Status::unavailable("捕获消费者组 Now 起点时来源关闭"))??;
                if let Some(subscribe_aggregate_partitions_internal_response::Payload::CaughtUp(
                    caught_up,
                )) = frame.payload
                {
                    for cursor in caught_up.cursors {
                        let partition_id = u16::try_from(cursor.partition_id)
                            .map_err(|_| Status::internal("内部 partition_id 超出范围"))?;
                        starts.insert(partition_id, cursor.next_position);
                    }
                    break;
                }
            }
        }
        if starts.len() != usize::from(definition.partition_count) {
            return Err(Status::unavailable("未能捕获全部消费者组分区起点"));
        }
        Ok(starts)
    }

    async fn apply_group_partition_request(
        &self,
        shard_id: u64,
        request: es_storage::EsRequest,
    ) -> Result<es_storage::EsResponse, Status> {
        if let Ok(shard) = self.local_leader(shard_id).await {
            return shard
                .raft
                .client_write(request)
                .await
                .map(|response| response.data)
                .map_err(client_write_to_status);
        }
        let payload = encode_bincode(&request, "AggregateGroupPartition request")?;
        let mut client = self
            .topology
            .snapshot()
            .await
            .remote
            .aggregate_internal_client(shard_id)
            .await?;
        let response = client
            .apply_aggregate_group_partition_internal(ApplyAggregateGroupPartitionInternalRequest {
                shard_id,
                payload,
            })
            .await?
            .into_inner();
        decode_bincode(&response.payload, "AggregateGroupPartition response")
    }

    async fn fetch_group_partition(
        &self,
        shard_id: u64,
        input: GroupPartitionFetchInput,
    ) -> Result<GroupPartitionFetchOutput, Status> {
        if let Ok(shard) = self.local_leader(shard_id).await {
            return fetch_group_partition_local(shard, input).await;
        }
        let payload = encode_bincode(&input, "GroupPartitionFetchInput")?;
        let mut client = self
            .topology
            .snapshot()
            .await
            .remote
            .aggregate_internal_client(shard_id)
            .await?;
        let response = client
            .fetch_aggregate_group_partition_internal(FetchAggregateGroupPartitionInternalRequest {
                shard_id,
                payload,
            })
            .await?
            .into_inner();
        decode_bincode(&response.payload, "GroupPartitionFetchOutput")
    }

    async fn install_partition_fence(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        shard_id: u64,
        generation: u64,
    ) -> Result<u64, Status> {
        if let Ok(shard) = self.local_leader(shard_id).await {
            let response = shard
                .raft
                .client_write(es_storage::EsRequest::InstallAggregatePartitionFence {
                    aggregate_type: aggregate_type.clone(),
                    partition_id,
                    generation,
                })
                .await
                .map_err(client_write_to_status)?;
            return map_fence_response(response.data);
        }
        let mut client = self
            .topology
            .snapshot()
            .await
            .remote
            .aggregate_internal_client(shard_id)
            .await?;
        let response = client
            .install_aggregate_partition_fence_internal(
                InstallAggregatePartitionFenceInternalRequest {
                    shard_id,
                    aggregate_type: Some(aggregate_type_to_proto(aggregate_type)),
                    partition_id: u32::from(partition_id),
                    generation,
                },
            )
            .await?
            .into_inner();
        Ok(response.generation)
    }

    async fn require_active_aggregate_type(
        &self,
        aggregate_type: &AggregateTypeId,
    ) -> Result<es_core::AggregateTypeDefinition, Status> {
        let catalog = self.fetch_catalog().await?;
        let definition = catalog
            .aggregate_types
            .get(aggregate_type)
            .cloned()
            .ok_or_else(|| Status::not_found(format!("聚合类型 {aggregate_type} 不存在")))?;
        if definition.status != AggregateTypeStatus::Active {
            return Err(Status::unavailable(format!(
                "聚合类型 {aggregate_type} 尚未激活，请重试"
            )));
        }
        Ok(definition)
    }

    async fn append_once(
        &self,
        definition: &es_core::AggregateTypeDefinition,
        aggregate_id: &str,
        expected_version: es_core::ExpectedAggregateVersion,
        event: es_core::NewAggregateEvent,
    ) -> Result<AppendAggregateEventResponse, Status> {
        let partition_id = definition
            .partition_for(aggregate_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let placement = definition
            .placements
            .get(&partition_id)
            .ok_or_else(|| Status::internal("聚合类型缺少分区放置"))?;
        let shard = self.local_leader(placement.shard_id).await?;
        let response = shard
            .raft
            .client_write(es_storage::EsRequest::AggregateAppend {
                aggregate_type: definition.id.clone(),
                partition_id,
                partition_generation: placement.generation,
                aggregate_id: aggregate_id.to_string(),
                expected_version,
                event,
                hlc: es_core::Hlc::now(),
            })
            .await
            .map_err(client_write_to_status)?;
        map_append_response(response.data)
    }

    async fn open_source_stream(
        &self,
        shard_id: u64,
        request: SubscribeAggregatePartitionsInternalRequest,
    ) -> Result<InternalAggregateStream, Status> {
        if let Ok(shard) = self.local_leader(shard_id).await {
            let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
            let (tx, rx) = tokio::sync::mpsc::channel(128);
            tokio::spawn(run_local_aggregate_subscription(
                shard.storage.clone(),
                aggregate_type,
                request.cursors,
                request.from_now,
                tx,
            ));
            return Ok(Box::pin(ReceiverStream::new(rx)));
        }
        let mut client = self
            .topology
            .snapshot()
            .await
            .remote
            .aggregate_internal_client(shard_id)
            .await?;
        let stream = client
            .subscribe_aggregate_partitions_internal(request)
            .await?
            .into_inner();
        Ok(Box::pin(stream))
    }

    async fn list_states_from_shard(
        &self,
        shard_id: u64,
        aggregate_type: &AggregateTypeId,
        cursors: Vec<AggregateStatePartitionCursor>,
        limit_per_partition: u32,
    ) -> Result<Vec<InternalAggregateStateInfo>, Status> {
        let request = ListAggregatePartitionStatesInternalRequest {
            shard_id,
            aggregate_type: Some(aggregate_type_to_proto(aggregate_type)),
            cursors,
            limit_per_partition,
        };
        if let Ok(shard) = self.local_leader(shard_id).await {
            return list_local_partition_states(&shard.storage, request)
                .map(|response| response.states);
        }
        let mut client = self
            .topology
            .snapshot()
            .await
            .remote
            .aggregate_internal_client(shard_id)
            .await?;
        Ok(client
            .list_aggregate_partition_states_internal(request)
            .await?
            .into_inner()
            .states)
    }
}

type InternalAggregateStream = Pin<
    Box<dyn Stream<Item = Result<SubscribeAggregatePartitionsInternalResponse, Status>> + Send>,
>;

fn encode_bincode<T: Serialize>(value: &T, name: &str) -> Result<Vec<u8>, Status> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| Status::internal(format!("{name} 编码失败: {error}")))
}

fn decode_bincode<T: for<'de> Deserialize<'de>>(bytes: &[u8], name: &str) -> Result<T, Status> {
    let (value, consumed) =
        bincode::serde::decode_from_slice(bytes, bincode::config::standard())
            .map_err(|error| Status::internal(format!("{name} 解码失败: {error}")))?;
    if consumed != bytes.len() {
        return Err(Status::invalid_argument(format!("{name} 包含多余字节")));
    }
    Ok(value)
}

fn proto_aggregate_type(value: Option<AggregateTypeRef>) -> Result<AggregateTypeId, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("aggregate_type 必填"))?;
    AggregateTypeId::new(value.business_space, value.aggregate_type)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}

fn aggregate_type_to_proto(aggregate_type: &AggregateTypeId) -> AggregateTypeRef {
    AggregateTypeRef {
        business_space: aggregate_type.business_space().to_string(),
        aggregate_type: aggregate_type.aggregate_type().to_string(),
    }
}

fn aggregate_type_info(
    aggregate_type: &es_core::AggregateTypeDefinition,
    catalog_revision: u64,
) -> AggregateTypeInfo {
    AggregateTypeInfo {
        aggregate_type: Some(aggregate_type_to_proto(&aggregate_type.id)),
        partition_count: u32::from(aggregate_type.partition_count),
        hash_algorithm: "xxh3-v1".into(),
        status: match aggregate_type.status {
            AggregateTypeStatus::Registering => {
                es_proto::eventstore::AggregateTypeStatus::AggregateTypeRegistering as i32
            }
            AggregateTypeStatus::Active => {
                es_proto::eventstore::AggregateTypeStatus::AggregateTypeActive as i32
            }
        },
        catalog_revision,
    }
}

fn proto_expected_version(
    value: Option<ExpectedAggregateVersion>,
) -> Result<es_core::ExpectedAggregateVersion, Status> {
    use expected_aggregate_version::Kind;
    Ok(match value.and_then(|value| value.kind) {
        None | Some(Kind::Any(_)) => es_core::ExpectedAggregateVersion::Any,
        Some(Kind::NoAggregate(_)) => es_core::ExpectedAggregateVersion::NoAggregate,
        Some(Kind::AggregateExists(_)) => es_core::ExpectedAggregateVersion::AggregateExists,
        Some(Kind::Exact(version)) => es_core::ExpectedAggregateVersion::Exact(version),
    })
}

fn proto_expected_state_revision(
    value: Option<ExpectedStateRevision>,
) -> Result<es_core::ExpectedStateRevision, Status> {
    use expected_state_revision::Kind;
    match value.and_then(|value| value.kind) {
        Some(Kind::Absent(_)) => Ok(es_core::ExpectedStateRevision::Absent),
        Some(Kind::Exact(revision)) => Ok(es_core::ExpectedStateRevision::Exact(revision)),
        None => Err(Status::invalid_argument("expected_revision 必填")),
    }
}

fn stored_event_to_proto(event: es_core::AggregateEvent) -> AggregateEvent {
    AggregateEvent {
        aggregate_id: event.aggregate_id,
        aggregate_version: event.aggregate_version,
        event_id: event.event_id.as_bytes().to_vec(),
        event_type: event.event_type,
        data: event.data,
        metadata: event.metadata,
        hlc: Some(Hlc {
            wall: event.hlc.wall,
            logical: event.hlc.logical,
        }),
    }
}

fn map_append_response(
    response: es_storage::EsResponse,
) -> Result<AppendAggregateEventResponse, Status> {
    match response {
        es_storage::EsResponse::AggregateAppendOk {
            aggregate_version, ..
        } => Ok(AppendAggregateEventResponse { aggregate_version }),
        es_storage::EsResponse::AggregateOptimisticConflict { actual_version } => {
            Err(Status::failed_precondition(format!(
                "aggregate version conflict: actual={actual_version:?}"
            )))
        }
        es_storage::EsResponse::AggregateIdempotencyConflict => {
            Err(Status::already_exists("event_id 已绑定到不同内容"))
        }
        es_storage::EsResponse::AggregatePartitionFenced { current_generation } => {
            Err(Status::unavailable(format!(
                "partition generation advanced to {current_generation}"
            )))
        }
        es_storage::EsResponse::AggregateInvalid { reason } => {
            Err(Status::invalid_argument(reason))
        }
        other => Err(Status::internal(format!("聚合追加返回意外结果: {other:?}"))),
    }
}

fn map_fence_response(response: es_storage::EsResponse) -> Result<u64, Status> {
    match response {
        es_storage::EsResponse::AggregatePartitionFenceInstalled { generation } => Ok(generation),
        es_storage::EsResponse::AggregateInvalid { reason } => {
            Err(Status::invalid_argument(reason))
        }
        other => Err(Status::internal(format!(
            "聚合分区 fence 返回意外结果: {other:?}"
        ))),
    }
}

async fn fetch_group_partition_local(
    shard: Arc<es_raft::Shard>,
    input: GroupPartitionFetchInput,
) -> Result<GroupPartitionFetchOutput, Status> {
    let mut state = shard
        .storage
        .read_aggregate_group_partition(
            &input.aggregate_type,
            input.partition_id,
            &input.group_name,
        )
        .map_err(|error| Status::internal(error.to_string()))?
        .unwrap_or_else(|| {
            es_core::AggregateGroupPartition::new(input.group_epoch, input.start_position)
        });
    if state.epoch > input.group_epoch {
        return Err(Status::failed_precondition(format!(
            "stale group epoch: actual={}",
            state.epoch
        )));
    }
    if state.epoch < input.group_epoch {
        state.reset(input.group_epoch, input.start_position);
    }
    let head = shard
        .storage
        .read_aggregate_partition_head(&input.aggregate_type, input.partition_id)
        .map_err(|error| Status::internal(error.to_string()))?;
    if state.deliveries.is_empty()
        && state.pending_retries.is_empty()
        && state.next_position >= head
    {
        return Ok(GroupPartitionFetchOutput {
            deliveries: Vec::new(),
            caught_up: true,
            throttled: false,
        });
    }
    if state.available_credit(&input.consumer_id, &input.settings) == 0 {
        return Ok(GroupPartitionFetchOutput {
            deliveries: Vec::new(),
            caught_up: state.next_position >= head,
            throttled: true,
        });
    }
    let scan_limit = u64::from(input.max_events.max(1))
        .saturating_mul(16)
        .clamp(32, 4096);
    let events = shard
        .storage
        .read_aggregate_partition_events(
            &input.aggregate_type,
            input.partition_id,
            state.next_position,
            scan_limit,
        )
        .map_err(|error| Status::internal(error.to_string()))?;
    let mut candidates = Vec::new();
    let mut by_position = BTreeMap::new();
    for event in events {
        let event_bytes = (event.data.len() + event.metadata.len()) as u64;
        candidates.push(es_core::AggregateDeliveryCandidate {
            delivery_id: uuid::Uuid::new_v4(),
            partition_position: event.partition_position,
            aggregate_id: event.aggregate_id.clone(),
            aggregate_version: event.aggregate_version,
            event_id: event.event_id,
            payload_bytes: event_bytes,
            replayed: false,
        });
        by_position.insert(event.partition_position, event);
    }
    let deadline_ms = input.now_ms.saturating_add(input.settings.ack_timeout_ms);
    let request = es_storage::EsRequest::AggregateGroupPartition {
        aggregate_type: input.aggregate_type,
        partition_id: input.partition_id,
        partition_generation: input.partition_generation,
        group_name: input.group_name,
        group_epoch: input.group_epoch,
        start_position: input.start_position,
        settings: input.settings,
        command: es_storage::AggregateGroupPartitionCommand::Claim {
            consumer_id: input.consumer_id,
            now_ms: input.now_ms,
            deadline_ms,
            max_claim: input.max_events,
            max_bytes: input.max_bytes,
            candidates,
        },
    };
    let response = shard
        .raft
        .client_write(request)
        .await
        .map_err(client_write_to_status)?;
    let claimed = match response.data {
        es_storage::EsResponse::AggregateGroupClaimed(deliveries) => deliveries,
        es_storage::EsResponse::AggregateGroupStaleEpoch { current_epoch } => {
            return Err(Status::failed_precondition(format!(
                "stale group epoch: actual={current_epoch}"
            )));
        }
        es_storage::EsResponse::AggregatePartitionFenced { current_generation } => {
            return Err(Status::unavailable(format!(
                "partition generation advanced to {current_generation}"
            )));
        }
        es_storage::EsResponse::AggregateInvalid { reason } => {
            return Err(Status::invalid_argument(reason));
        }
        other => {
            return Err(Status::internal(format!(
                "消费者组 claim 返回意外结果: {other:?}"
            )));
        }
    };
    let mut deliveries = Vec::with_capacity(claimed.len());
    for delivery in claimed {
        let event = by_position
            .remove(&delivery.partition_position)
            .ok_or_else(|| Status::internal("claim 返回了扫描窗口外的事件"))?;
        deliveries.push((delivery, event));
    }
    let scanned_to = by_position
        .keys()
        .next_back()
        .copied()
        .or_else(|| deliveries.last().map(|(_, event)| event.partition_position))
        .map(|position| position.saturating_add(1))
        .unwrap_or(state.next_position);
    Ok(GroupPartitionFetchOutput {
        deliveries,
        caught_up: scanned_to >= head,
        throttled: false,
    })
}

fn proto_group_start(
    value: Option<AggregateGroupStart>,
) -> Result<es_core::AggregateGroupStart, Status> {
    use aggregate_group_start::Kind;
    Ok(match value.and_then(|value| value.kind) {
        None | Some(Kind::Beginning(_)) => es_core::AggregateGroupStart::Beginning,
        Some(Kind::Now(_)) => es_core::AggregateGroupStart::Now,
    })
}

fn group_start_to_proto(value: es_core::AggregateGroupStart) -> AggregateGroupStart {
    let kind = match value {
        es_core::AggregateGroupStart::Beginning => aggregate_group_start::Kind::Beginning(Empty {}),
        es_core::AggregateGroupStart::Now => aggregate_group_start::Kind::Now(Empty {}),
    };
    AggregateGroupStart { kind: Some(kind) }
}

fn proto_group_settings(
    value: Option<AggregateGroupSettings>,
) -> Result<es_core::AggregateGroupSettings, Status> {
    let value = value
        .map(|value| es_core::AggregateGroupSettings {
            max_unacked_per_consumer: value.max_unacked_per_consumer,
            max_unacked_per_group: value.max_unacked_per_group,
            ack_timeout_ms: value.ack_timeout_ms,
            max_retries: value.max_retries,
            retry_min_ms: value.retry_min_ms,
            retry_max_ms: value.retry_max_ms,
        })
        .unwrap_or_default();
    value
        .validate()
        .map_err(Status::invalid_argument)
        .map(|()| value)
}

fn group_settings_to_proto(value: &es_core::AggregateGroupSettings) -> AggregateGroupSettings {
    AggregateGroupSettings {
        max_unacked_per_consumer: value.max_unacked_per_consumer,
        max_unacked_per_group: value.max_unacked_per_group,
        ack_timeout_ms: value.ack_timeout_ms,
        max_retries: value.max_retries,
        retry_min_ms: value.retry_min_ms,
        retry_max_ms: value.retry_max_ms,
    }
}

fn group_info(value: &es_core::AggregateGroupDefinition) -> AggregateGroupInfo {
    AggregateGroupInfo {
        aggregate_type: Some(aggregate_type_to_proto(&value.aggregate_type)),
        name: value.name.clone(),
        revision: value.revision,
        epoch: value.epoch,
        start: Some(group_start_to_proto(value.start)),
        settings: Some(group_settings_to_proto(&value.settings)),
    }
}

fn encode_delivery_token(
    definition: &es_core::AggregateGroupDefinition,
    partition_id: u16,
    delivery_id: uuid::Uuid,
) -> Result<Vec<u8>, Status> {
    encode_bincode(
        &es_core::AggregateDeliveryToken {
            version: 1,
            aggregate_type: definition.aggregate_type.clone(),
            group_name: definition.name.clone(),
            partition_id,
            group_epoch: definition.epoch,
            delivery_id,
        },
        "delivery token",
    )
}

fn decode_delivery_token(
    bytes: &[u8],
    aggregate_type: &AggregateTypeId,
    group_name: &str,
) -> Result<es_core::AggregateDeliveryToken, Status> {
    let token: es_core::AggregateDeliveryToken = decode_bincode(bytes, "delivery token")?;
    if token.version != 1
        || token.aggregate_type != *aggregate_type
        || token.group_name != group_name
    {
        return Err(Status::invalid_argument(
            "delivery token 版本、聚合类型或消费者组不匹配",
        ));
    }
    Ok(token)
}

fn group_settlement_status(value: es_core::AggregateSettlementResult) -> i32 {
    match value {
        es_core::AggregateSettlementResult::Applied => {
            AggregateGroupSettlementStatus::AggregateGroupSettlementApplied as i32
        }
        es_core::AggregateSettlementResult::AlreadySettled => {
            AggregateGroupSettlementStatus::AggregateGroupSettlementAlreadySettled as i32
        }
        es_core::AggregateSettlementResult::StaleLease => {
            AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease as i32
        }
        es_core::AggregateSettlementResult::WrongConsumer => {
            AggregateGroupSettlementStatus::AggregateGroupSettlementWrongConsumer as i32
        }
    }
}

fn map_group_settlement_response(
    response: es_storage::EsResponse,
    renew: bool,
) -> Result<Vec<i32>, Status> {
    let values = match response {
        es_storage::EsResponse::AggregateGroupSettled(values) if !renew => values,
        es_storage::EsResponse::AggregateGroupRenewed(values) if renew => values,
        es_storage::EsResponse::AggregateGroupStaleEpoch { .. } => {
            return Err(Status::failed_precondition("消费者组 epoch 已过期"));
        }
        es_storage::EsResponse::AggregatePartitionFenced { current_generation } => {
            return Err(Status::unavailable(format!(
                "partition generation advanced to {current_generation}"
            )));
        }
        es_storage::EsResponse::AggregateInvalid { reason } => {
            return Err(Status::invalid_argument(reason));
        }
        other => {
            return Err(Status::internal(format!(
                "消费者组结算返回意外结果: {other:?}"
            )));
        }
    };
    Ok(values.into_iter().map(group_settlement_status).collect())
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn encode_cursor(cursor: &AggregateCursor) -> Result<Vec<u8>, Status> {
    encode_bincode(cursor, "aggregate cursor")
}

fn decode_cursor(
    bytes: &[u8],
    aggregate_type: &AggregateTypeId,
    partition_count: u16,
) -> Result<AggregateCursor, Status> {
    let cursor: AggregateCursor = decode_bincode(bytes, "aggregate cursor")?;
    if cursor.version != CURSOR_VERSION
        || cursor.aggregate_type != *aggregate_type
        || cursor.next_positions.len() != usize::from(partition_count)
    {
        return Err(Status::invalid_argument(
            "cursor 版本、聚合类型或分区数量不匹配",
        ));
    }
    Ok(cursor)
}

fn decode_state_page_token(
    bytes: &[u8],
    aggregate_type: &AggregateTypeId,
    partition_count: u16,
) -> Result<StatePageToken, Status> {
    if bytes.is_empty() {
        return Ok(StatePageToken {
            version: STATE_PAGE_TOKEN_VERSION,
            aggregate_type: aggregate_type.clone(),
            after_aggregate_ids: vec![String::new(); usize::from(partition_count)],
        });
    }
    let token: StatePageToken = decode_bincode(bytes, "state page token")?;
    if token.version != STATE_PAGE_TOKEN_VERSION
        || token.aggregate_type != *aggregate_type
        || token.after_aggregate_ids.len() != usize::from(partition_count)
    {
        return Err(Status::invalid_argument(
            "page_token 版本、聚合类型或分区数量不匹配",
        ));
    }
    Ok(token)
}

async fn run_local_aggregate_subscription(
    storage: Arc<es_storage::EsStorage>,
    aggregate_type: AggregateTypeId,
    cursors: Vec<AggregatePartitionCursor>,
    from_now: bool,
    tx: tokio::sync::mpsc::Sender<Result<SubscribeAggregatePartitionsInternalResponse, Status>>,
) {
    let mut receiver = storage.subscribe_aggregate_events();
    let mut next_positions = BTreeMap::new();
    for cursor in cursors {
        let Ok(partition_id) = u16::try_from(cursor.partition_id) else {
            let _ = tx
                .send(Err(Status::invalid_argument("partition_id 超出范围")))
                .await;
            return;
        };
        let start = if from_now {
            match storage.read_aggregate_partition_head(&aggregate_type, partition_id) {
                Ok(head) => head,
                Err(error) => {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                    return;
                }
            }
        } else {
            cursor.next_position
        };
        next_positions.insert(partition_id, start);
        if from_now {
            continue;
        }
        let historical = match storage.read_aggregate_partition_events(
            &aggregate_type,
            partition_id,
            start,
            0,
        ) {
            Ok(events) => events,
            Err(error) => {
                let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                return;
            }
        };
        for event in historical {
            next_positions.insert(partition_id, event.partition_position.saturating_add(1));
            if tx
                .send(Ok(SubscribeAggregatePartitionsInternalResponse {
                    payload: Some(
                        subscribe_aggregate_partitions_internal_response::Payload::Event(
                            InternalAggregateEvent {
                                partition_id: u32::from(partition_id),
                                partition_position: event.partition_position,
                                event: Some(stored_event_to_proto(event)),
                            },
                        ),
                    ),
                }))
                .await
                .is_err()
            {
                return;
            }
        }
    }
    let heads = next_positions
        .iter()
        .map(|(partition_id, next_position)| AggregatePartitionCursor {
            partition_id: u32::from(*partition_id),
            next_position: *next_position,
        })
        .collect();
    if tx
        .send(Ok(SubscribeAggregatePartitionsInternalResponse {
            payload: Some(
                subscribe_aggregate_partitions_internal_response::Payload::CaughtUp(
                    InternalAggregateCaughtUp { cursors: heads },
                ),
            ),
        }))
        .await
        .is_err()
    {
        return;
    }

    loop {
        match receiver.recv().await {
            Ok(event) => {
                if event.aggregate_type != aggregate_type {
                    continue;
                }
                let Some(next) = next_positions.get_mut(&event.partition_id) else {
                    continue;
                };
                if event.partition_position < *next {
                    continue;
                }
                *next = event.partition_position.saturating_add(1);
                let response = SubscribeAggregatePartitionsInternalResponse {
                    payload: Some(
                        subscribe_aggregate_partitions_internal_response::Payload::Event(
                            InternalAggregateEvent {
                                partition_id: u32::from(event.partition_id),
                                partition_position: event.partition_position,
                                event: Some(stored_event_to_proto(event)),
                            },
                        ),
                    ),
                };
                if tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                let _ = tx
                    .send(Err(Status::unavailable(
                        "aggregate subscription lagged; resume from cursor",
                    )))
                    .await;
                return;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

fn list_local_partition_states(
    storage: &es_storage::EsStorage,
    request: ListAggregatePartitionStatesInternalRequest,
) -> Result<ListAggregatePartitionStatesInternalResponse, Status> {
    let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
    let mut states = Vec::new();
    for cursor in request.cursors {
        let partition_id = u16::try_from(cursor.partition_id)
            .map_err(|_| Status::invalid_argument("partition_id 超出范围"))?;
        let after =
            (!cursor.after_aggregate_id.is_empty()).then_some(cursor.after_aggregate_id.as_str());
        for (aggregate_id, state) in storage
            .list_aggregate_partition_states(
                &aggregate_type,
                partition_id,
                after,
                u64::from(request.limit_per_partition),
            )
            .map_err(|error| Status::internal(error.to_string()))?
        {
            states.push(InternalAggregateStateInfo {
                partition_id: u32::from(partition_id),
                aggregate_id,
                revision: state.revision,
                modified_unix_millis: state.modified_hlc.wall,
            });
        }
    }
    Ok(ListAggregatePartitionStatesInternalResponse { states })
}

#[tonic::async_trait]
impl AggregateStore for AggregateStoreService {
    type FollowAggregateTypeEventsStream =
        ReceiverStream<Result<FollowAggregateTypeEventsResponse, Status>>;

    async fn get_aggregate_store_capabilities(
        &self,
        _request: Request<GetAggregateStoreCapabilitiesRequest>,
    ) -> Result<Response<AggregateStoreCapabilities>, Status> {
        Ok(Response::new(AggregateStoreCapabilities {
            api_version: "1.0".into(),
            partition_count: u32::from(es_core::EVENT_PARTITION_COUNT),
            max_event_bytes: self.max_event_bytes,
            max_state_bytes: self.max_event_bytes,
            state_revision_cas: true,
            explicit_group_settlement: true,
            state_modified_time: true,
        }))
    }

    async fn register_aggregate_type(
        &self,
        request: Request<RegisterAggregateTypeRequest>,
    ) -> Result<Response<AggregateTypeInfo>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let operation_id = uuid::Uuid::from_slice(&request.operation_id)
            .map_err(|_| Status::invalid_argument("operation_id 必须是 16 字节 UUID"))?;
        let all_shards = self.topology.snapshot().await.all_shards;
        let placements = (0..es_core::EVENT_PARTITION_COUNT)
            .map(|partition_id| {
                let shard = all_shards[usize::from(partition_id) % all_shards.len()];
                (partition_id, shard)
            })
            .collect();
        let created = self
            .commit_catalog(es_core::AggregateCatalogCommand::Create {
                aggregate_type: aggregate_type.clone(),
                operation_id,
                seed: *operation_id.as_bytes(),
                placements,
            })
            .await?;
        let definition = match created.outcome {
            AggregateCatalogOutcome::AggregateType { aggregate_type, .. } => aggregate_type,
            AggregateCatalogOutcome::Conflict { reason } => {
                return Err(Status::already_exists(reason));
            }
            AggregateCatalogOutcome::Invalid { reason } => {
                return Err(Status::invalid_argument(reason));
            }
            AggregateCatalogOutcome::NotFound => {
                return Err(Status::internal("注册聚合类型返回 NotFound"));
            }
        };
        for (partition_id, placement) in &definition.placements {
            self.install_partition_fence(
                &definition.id,
                *partition_id,
                placement.shard_id,
                placement.generation,
            )
            .await?;
        }
        let activated = self
            .commit_catalog(es_core::AggregateCatalogCommand::Activate {
                aggregate_type,
                operation_id,
            })
            .await?;
        match activated.outcome {
            AggregateCatalogOutcome::AggregateType { aggregate_type, .. } => Ok(Response::new(
                aggregate_type_info(&aggregate_type, activated.revision),
            )),
            AggregateCatalogOutcome::Conflict { reason } => {
                Err(Status::failed_precondition(reason))
            }
            AggregateCatalogOutcome::Invalid { reason } => Err(Status::invalid_argument(reason)),
            AggregateCatalogOutcome::NotFound => Err(Status::not_found("聚合类型不存在")),
        }
    }

    async fn list_aggregate_types(
        &self,
        _request: Request<ListAggregateTypesRequest>,
    ) -> Result<Response<ListAggregateTypesResponse>, Status> {
        let catalog = self.fetch_catalog().await?;
        Ok(Response::new(ListAggregateTypesResponse {
            aggregate_types: catalog
                .aggregate_types
                .values()
                .map(|aggregate_type| aggregate_type_info(aggregate_type, catalog.revision))
                .collect(),
        }))
    }

    async fn get_aggregate_type(
        &self,
        request: Request<GetAggregateTypeRequest>,
    ) -> Result<Response<AggregateTypeInfo>, Status> {
        let aggregate_type = proto_aggregate_type(request.into_inner().aggregate_type)?;
        let catalog = self.fetch_catalog().await?;
        let definition = catalog
            .aggregate_types
            .get(&aggregate_type)
            .ok_or_else(|| Status::not_found("聚合类型不存在"))?;
        Ok(Response::new(aggregate_type_info(
            definition,
            catalog.revision,
        )))
    }

    async fn append_aggregate_event(
        &self,
        request: Request<AppendAggregateEventRequest>,
    ) -> Result<Response<AppendAggregateEventResponse>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        es_core::validate_aggregate_identifier("aggregate_id", &request.aggregate_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let expected = proto_expected_version(request.expected_version)?;
        let event = request
            .event
            .ok_or_else(|| Status::invalid_argument("event 必填"))?;
        if event.data.len().saturating_add(event.metadata.len()) as u64 > self.max_event_bytes {
            return Err(Status::failed_precondition("聚合事件 payload 超出限制"));
        }
        let event_id = uuid::Uuid::from_slice(&event.event_id)
            .map_err(|_| Status::invalid_argument("event_id 必须是 16 字节 UUID"))?;
        if event.event_type.is_empty() {
            return Err(Status::invalid_argument("event_type 不能为空"));
        }
        let event = es_core::NewAggregateEvent {
            event_id,
            event_type: event.event_type,
            data: event.data,
            metadata: event.metadata,
        };
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        match self
            .append_once(&definition, &request.aggregate_id, expected, event.clone())
            .await
        {
            Ok(response) => Ok(Response::new(response)),
            Err(status)
                if status.code() == tonic::Code::Unavailable
                    && status.message().contains("partition generation advanced") =>
            {
                let refreshed = self.require_active_aggregate_type(&aggregate_type).await?;
                self.append_once(&refreshed, &request.aggregate_id, expected, event)
                    .await
                    .map(Response::new)
            }
            Err(status) => Err(status),
        }
    }

    async fn follow_aggregate_type_events(
        &self,
        request: Request<FollowAggregateTypeEventsRequest>,
    ) -> Result<Response<Self::FollowAggregateTypeEventsStream>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let start = request.start.and_then(|start| start.kind);
        let (cursor, from_now) = match start {
            None | Some(aggregate_follow_start::Kind::Beginning(_)) => (
                AggregateCursor {
                    version: CURSOR_VERSION,
                    aggregate_type: aggregate_type.clone(),
                    next_positions: vec![0; usize::from(definition.partition_count)],
                },
                false,
            ),
            Some(aggregate_follow_start::Kind::Now(_)) => (
                AggregateCursor {
                    version: CURSOR_VERSION,
                    aggregate_type: aggregate_type.clone(),
                    next_positions: vec![0; usize::from(definition.partition_count)],
                },
                true,
            ),
            Some(aggregate_follow_start::Kind::Cursor(bytes)) => (
                decode_cursor(&bytes, &aggregate_type, definition.partition_count)?,
                false,
            ),
        };

        let mut by_shard: BTreeMap<u64, Vec<AggregatePartitionCursor>> = BTreeMap::new();
        for (partition_id, placement) in &definition.placements {
            by_shard
                .entry(placement.shard_id)
                .or_default()
                .push(AggregatePartitionCursor {
                    partition_id: u32::from(*partition_id),
                    next_position: cursor.next_positions[usize::from(*partition_id)],
                });
        }
        let source_ids: BTreeSet<u64> = by_shard.keys().copied().collect();
        let (source_tx, mut source_rx) = tokio::sync::mpsc::channel(256);
        for (shard_id, cursors) in by_shard {
            let service = self.clone();
            let tx = source_tx.clone();
            let aggregate_type = aggregate_type.clone();
            tokio::spawn(async move {
                run_aggregate_source(service, shard_id, aggregate_type, cursors, from_now, tx)
                    .await;
            });
        }
        drop(source_tx);
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            let mut merger = AggregateReadMerger::new(cursor, source_ids);
            while let Some(message) = source_rx.recv().await {
                for response in merger.apply(message) {
                    if tx.send(Ok(response)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_aggregate_states(
        &self,
        request: Request<ListAggregateStatesRequest>,
    ) -> Result<Response<ListAggregateStatesResponse>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let page_size = if request.page_size == 0 {
            DEFAULT_STATE_PAGE_SIZE
        } else {
            request.page_size.min(MAX_STATE_PAGE_SIZE)
        };
        let mut token = decode_state_page_token(
            &request.page_token,
            &aggregate_type,
            definition.partition_count,
        )?;
        let mut grouped: BTreeMap<u64, Vec<AggregateStatePartitionCursor>> = BTreeMap::new();
        for (partition_id, placement) in &definition.placements {
            grouped
                .entry(placement.shard_id)
                .or_default()
                .push(AggregateStatePartitionCursor {
                    partition_id: u32::from(*partition_id),
                    after_aggregate_id: token.after_aggregate_ids[usize::from(*partition_id)]
                        .clone(),
                });
        }
        let mut all = Vec::new();
        for (shard_id, cursors) in grouped {
            all.extend(
                self.list_states_from_shard(
                    shard_id,
                    &aggregate_type,
                    cursors,
                    page_size.saturating_add(1),
                )
                .await?,
            );
        }
        all.sort_by(|left, right| left.aggregate_id.cmp(&right.aggregate_id));
        let has_more = all.len() > page_size as usize;
        all.truncate(page_size as usize);
        let mut states = Vec::with_capacity(all.len());
        for item in all {
            let partition_id = u16::try_from(item.partition_id)
                .map_err(|_| Status::internal("内部 partition_id 超出范围"))?;
            token.after_aggregate_ids[usize::from(partition_id)] = item.aggregate_id.clone();
            states.push(AggregateStateInfo {
                aggregate_id: item.aggregate_id,
                revision: item.revision,
                modified_unix_millis: item.modified_unix_millis,
            });
        }
        let next_page_token = if has_more {
            encode_bincode(&token, "state page token")?
        } else {
            Vec::new()
        };
        Ok(Response::new(ListAggregateStatesResponse {
            states,
            next_page_token,
        }))
    }

    async fn get_aggregate_state(
        &self,
        request: Request<GetAggregateStateRequest>,
    ) -> Result<Response<GetAggregateStateResponse>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let partition_id = definition
            .partition_for(&request.aggregate_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let placement = &definition.placements[&partition_id];
        let shard = self.local_leader(placement.shard_id).await?;
        let state = shard
            .storage
            .read_aggregate_state_document(&aggregate_type, partition_id, &request.aggregate_id)
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::not_found("聚合状态不存在"))?;
        Ok(Response::new(GetAggregateStateResponse {
            revision: state.revision,
            data: state.data,
            modified_unix_millis: state.modified_hlc.wall,
        }))
    }

    async fn put_aggregate_state(
        &self,
        request: Request<PutAggregateStateRequest>,
    ) -> Result<Response<PutAggregateStateResponse>, Status> {
        let request = request.into_inner();
        if request.data.len() as u64 > self.max_event_bytes {
            return Err(Status::failed_precondition("聚合状态 payload 超出限制"));
        }
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let expected_revision = proto_expected_state_revision(request.expected_revision)?;
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let partition_id = definition
            .partition_for(&request.aggregate_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let placement = &definition.placements[&partition_id];
        let shard = self.local_leader(placement.shard_id).await?;
        let response = shard
            .raft
            .client_write(es_storage::EsRequest::PutAggregateState {
                aggregate_type,
                partition_id,
                partition_generation: placement.generation,
                aggregate_id: request.aggregate_id,
                expected_revision,
                data: request.data,
                hlc: es_core::Hlc::now(),
            })
            .await
            .map_err(client_write_to_status)?;
        match response.data {
            es_storage::EsResponse::AggregateStateStored { state } => {
                Ok(Response::new(PutAggregateStateResponse {
                    revision: state.revision,
                    modified_unix_millis: state.modified_hlc.wall,
                }))
            }
            es_storage::EsResponse::AggregateNotFound => Err(Status::not_found("聚合实例不存在")),
            es_storage::EsResponse::AggregateStateConflict { actual_revision } => {
                Err(Status::failed_precondition(format!(
                    "state revision conflict: actual={actual_revision:?}"
                )))
            }
            es_storage::EsResponse::AggregatePartitionFenced { current_generation } => {
                Err(Status::unavailable(format!(
                    "partition generation advanced to {current_generation}"
                )))
            }
            es_storage::EsResponse::AggregateInvalid { reason } => {
                Err(Status::invalid_argument(reason))
            }
            other => Err(Status::internal(format!(
                "写聚合状态返回意外结果: {other:?}"
            ))),
        }
    }

    async fn get_aggregate_store_status(
        &self,
        _request: Request<GetAggregateStoreStatusRequest>,
    ) -> Result<Response<AggregateStoreStatus>, Status> {
        let catalog = self.fetch_catalog().await?;
        let creating = catalog
            .aggregate_types
            .values()
            .filter(|aggregate_type| aggregate_type.status == AggregateTypeStatus::Registering)
            .count();
        Ok(Response::new(AggregateStoreStatus {
            catalog_revision: catalog.revision,
            aggregate_type_count: catalog.aggregate_types.len() as u32,
            registering_aggregate_type_count: creating as u32,
            active_aggregate_type_count: (catalog.aggregate_types.len() - creating) as u32,
        }))
    }

    async fn list_aggregate_partitions(
        &self,
        request: Request<ListAggregatePartitionsRequest>,
    ) -> Result<Response<ListAggregatePartitionsResponse>, Status> {
        let aggregate_type = proto_aggregate_type(request.into_inner().aggregate_type)?;
        let catalog = self.fetch_catalog().await?;
        let definition = catalog
            .aggregate_types
            .get(&aggregate_type)
            .ok_or_else(|| Status::not_found("聚合类型不存在"))?;
        Ok(Response::new(ListAggregatePartitionsResponse {
            partitions: definition
                .placements
                .iter()
                .map(|(partition_id, placement)| AggregatePartitionInfo {
                    partition_id: u32::from(*partition_id),
                    shard_id: placement.shard_id,
                    generation: placement.generation,
                    moving: placement.pending_move.is_some(),
                    target_shard_id: placement
                        .pending_move
                        .as_ref()
                        .map(|pending| pending.target_shard)
                        .unwrap_or(0),
                })
                .collect(),
        }))
    }

    async fn create_aggregate_group(
        &self,
        request: Request<CreateAggregateGroupRequest>,
    ) -> Result<Response<AggregateGroupInfo>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        es_core::validate_aggregate_identifier("group_name", &request.name)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let operation_id = uuid::Uuid::from_slice(&request.operation_id)
            .map_err(|_| Status::invalid_argument("operation_id 必须是 16 字节 UUID"))?;
        let start = proto_group_start(request.start)?;
        let settings = proto_group_settings(request.settings)?;
        let aggregate_type_definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let partition_starts = self
            .capture_group_starts(&aggregate_type_definition, start)
            .await?;
        let applied = self
            .commit_group_catalog(es_core::AggregateGroupCatalogCommand::Create {
                definition: es_core::AggregateGroupDefinition {
                    aggregate_type,
                    name: request.name,
                    revision: 0,
                    epoch: 0,
                    start,
                    partition_starts,
                    settings,
                    create_operation_id: operation_id,
                    last_operation_id: operation_id,
                },
                partition_count: aggregate_type_definition.partition_count,
            })
            .await?;
        match applied.outcome {
            es_core::AggregateGroupCatalogOutcome::Group(group) => {
                Ok(Response::new(group_info(&group)))
            }
            es_core::AggregateGroupCatalogOutcome::Conflict { actual_revision } => Err(
                Status::already_exists(format!("消费者组已存在: revision={actual_revision:?}")),
            ),
            es_core::AggregateGroupCatalogOutcome::Invalid { reason } => {
                Err(Status::invalid_argument(reason))
            }
            es_core::AggregateGroupCatalogOutcome::NotFound => {
                Err(Status::not_found("聚合类型不存在"))
            }
            es_core::AggregateGroupCatalogOutcome::Deleted => {
                Err(Status::internal("创建消费者组返回 Deleted"))
            }
        }
    }

    async fn update_aggregate_group(
        &self,
        request: Request<UpdateAggregateGroupRequest>,
    ) -> Result<Response<AggregateGroupInfo>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let existing = self.require_group(&aggregate_type, &request.name).await?;
        let operation_id = uuid::Uuid::from_slice(&request.operation_id)
            .map_err(|_| Status::invalid_argument("operation_id 必须是 16 字节 UUID"))?;
        let aggregate_type_definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let reset = request.start.is_some();
        let (start, partition_starts) = match request.start {
            Some(start) => {
                let start = proto_group_start(Some(start))?;
                let positions = self
                    .capture_group_starts(&aggregate_type_definition, start)
                    .await?;
                (start, positions)
            }
            None => (existing.start, existing.partition_starts.clone()),
        };
        let settings = match request.settings {
            Some(settings) => proto_group_settings(Some(settings))?,
            None => existing.settings.clone(),
        };
        let applied = self
            .commit_group_catalog(es_core::AggregateGroupCatalogCommand::Replace {
                definition: es_core::AggregateGroupDefinition {
                    aggregate_type,
                    name: request.name,
                    revision: existing.revision,
                    epoch: existing.epoch,
                    start,
                    partition_starts,
                    settings,
                    create_operation_id: existing.create_operation_id,
                    last_operation_id: operation_id,
                },
                expected_revision: request.expected_revision,
                partition_count: aggregate_type_definition.partition_count,
                reset,
            })
            .await?;
        match applied.outcome {
            es_core::AggregateGroupCatalogOutcome::Group(group) => {
                Ok(Response::new(group_info(&group)))
            }
            es_core::AggregateGroupCatalogOutcome::Conflict { actual_revision } => {
                Err(Status::failed_precondition(format!(
                    "group revision conflict: actual={actual_revision:?}"
                )))
            }
            es_core::AggregateGroupCatalogOutcome::Invalid { reason } => {
                Err(Status::invalid_argument(reason))
            }
            es_core::AggregateGroupCatalogOutcome::NotFound => {
                Err(Status::not_found("消费者组不存在"))
            }
            es_core::AggregateGroupCatalogOutcome::Deleted => {
                Err(Status::internal("更新消费者组返回 Deleted"))
            }
        }
    }

    async fn delete_aggregate_group(
        &self,
        request: Request<DeleteAggregateGroupRequest>,
    ) -> Result<Response<Empty>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let operation_id = uuid::Uuid::from_slice(&request.operation_id)
            .map_err(|_| Status::invalid_argument("operation_id 必须是 16 字节 UUID"))?;
        let applied = self
            .commit_group_catalog(es_core::AggregateGroupCatalogCommand::Delete {
                aggregate_type,
                name: request.name,
                expected_revision: request.expected_revision,
                operation_id,
            })
            .await?;
        match applied.outcome {
            es_core::AggregateGroupCatalogOutcome::Deleted => Ok(Response::new(Empty {})),
            es_core::AggregateGroupCatalogOutcome::Conflict { actual_revision } => {
                Err(Status::failed_precondition(format!(
                    "group revision conflict: actual={actual_revision:?}"
                )))
            }
            es_core::AggregateGroupCatalogOutcome::NotFound => {
                Err(Status::not_found("消费者组不存在"))
            }
            es_core::AggregateGroupCatalogOutcome::Invalid { reason } => {
                Err(Status::invalid_argument(reason))
            }
            es_core::AggregateGroupCatalogOutcome::Group(_) => {
                Err(Status::internal("删除消费者组返回 Group"))
            }
        }
    }

    async fn get_aggregate_group(
        &self,
        request: Request<GetAggregateGroupRequest>,
    ) -> Result<Response<AggregateGroupInfo>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let group = self.require_group(&aggregate_type, &request.name).await?;
        Ok(Response::new(group_info(&group)))
    }

    async fn list_aggregate_groups(
        &self,
        request: Request<ListAggregateGroupsRequest>,
    ) -> Result<Response<ListAggregateGroupsResponse>, Status> {
        let aggregate_type = proto_aggregate_type(request.into_inner().aggregate_type)?;
        let catalog = self.fetch_group_catalog().await?;
        Ok(Response::new(ListAggregateGroupsResponse {
            groups: catalog
                .groups
                .iter()
                .filter(|((identity, _), _)| *identity == aggregate_type)
                .map(|(_, group)| group_info(group))
                .collect(),
        }))
    }

    async fn fetch_aggregate_group(
        &self,
        request: Request<FetchAggregateGroupRequest>,
    ) -> Result<Response<FetchAggregateGroupResponse>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        es_core::validate_aggregate_identifier("consumer_id", &request.consumer_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let group = self.require_group(&aggregate_type, &request.name).await?;
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let max_events = if request.max_events == 0 {
            es_core::DEFAULT_AGGREGATE_GROUP_FETCH_EVENTS
        } else {
            request
                .max_events
                .min(es_core::MAX_AGGREGATE_GROUP_FETCH_EVENTS)
        };
        let max_bytes = if request.max_bytes == 0 {
            es_core::DEFAULT_AGGREGATE_GROUP_FETCH_BYTES
        } else {
            request
                .max_bytes
                .min(es_core::MAX_AGGREGATE_GROUP_FETCH_BYTES)
        };
        let wait_ms = request
            .wait_ms
            .min(es_core::MAX_AGGREGATE_GROUP_FETCH_WAIT_MS);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let now_ms = unix_millis();
            let mut deliveries = Vec::new();
            let mut used_bytes = 0u64;
            let mut caught_up = true;
            let mut throttled = false;
            for (partition_id, placement) in &definition.placements {
                if deliveries.len() >= max_events as usize || used_bytes >= max_bytes {
                    caught_up = false;
                    break;
                }
                let output = self
                    .fetch_group_partition(
                        placement.shard_id,
                        GroupPartitionFetchInput {
                            aggregate_type: aggregate_type.clone(),
                            partition_id: *partition_id,
                            partition_generation: placement.generation,
                            group_name: group.name.clone(),
                            group_epoch: group.epoch,
                            start_position: group.partition_starts[partition_id],
                            settings: group.settings.clone(),
                            consumer_id: request.consumer_id.clone(),
                            now_ms,
                            max_events: max_events.saturating_sub(deliveries.len() as u32),
                            max_bytes: max_bytes.saturating_sub(used_bytes),
                        },
                    )
                    .await?;
                caught_up &= output.caught_up;
                throttled |= output.throttled;
                for (delivery, event) in output.deliveries {
                    used_bytes =
                        used_bytes.saturating_add((event.data.len() + event.metadata.len()) as u64);
                    deliveries.push(AggregateGroupDelivery {
                        delivery_id: encode_delivery_token(
                            &group,
                            *partition_id,
                            delivery.delivery_id,
                        )?,
                        event: Some(stored_event_to_proto(event)),
                        attempt: delivery.attempt,
                        deadline_ms: delivery.deadline_ms,
                        replayed: delivery.replayed,
                    });
                }
            }
            if !deliveries.is_empty()
                || throttled
                || caught_up
                || wait_ms == 0
                || tokio::time::Instant::now() >= deadline
            {
                return Ok(Response::new(FetchAggregateGroupResponse {
                    deliveries,
                    caught_up,
                    throttled,
                }));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn settle_aggregate_group(
        &self,
        request: Request<SettleAggregateGroupRequest>,
    ) -> Result<Response<SettleAggregateGroupResponse>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        es_core::validate_aggregate_identifier("consumer_id", &request.consumer_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let group = self.require_group(&aggregate_type, &request.name).await?;
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let mut results = vec![None; request.settlements.len()];
        let mut grouped: BTreeMap<u16, Vec<(usize, es_core::AggregateSettlement)>> =
            BTreeMap::new();
        for (index, settlement) in request.settlements.iter().enumerate() {
            let token =
                decode_delivery_token(&settlement.delivery_id, &aggregate_type, &request.name)?;
            if token.group_epoch != group.epoch {
                results[index] = Some(AggregateGroupSettlementResult {
                    delivery_id: settlement.delivery_id.clone(),
                    status: AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease
                        as i32,
                    deadline_ms: 0,
                });
                continue;
            }
            let action = match AggregateGroupSettlementAction::try_from(settlement.action)
                .map_err(|_| Status::invalid_argument("settlement action 非法"))?
            {
                AggregateGroupSettlementAction::AggregateGroupSettlementAck => {
                    es_core::AggregateSettlementAction::Ack
                }
                AggregateGroupSettlementAction::AggregateGroupSettlementRetry => {
                    es_core::AggregateSettlementAction::Retry
                }
                AggregateGroupSettlementAction::AggregateGroupSettlementPark => {
                    es_core::AggregateSettlementAction::Park
                }
                AggregateGroupSettlementAction::AggregateGroupSettlementSkip => {
                    es_core::AggregateSettlementAction::Skip
                }
            };
            grouped.entry(token.partition_id).or_default().push((
                index,
                es_core::AggregateSettlement {
                    delivery_id: token.delivery_id,
                    action,
                    reason: settlement.reason.clone(),
                },
            ));
        }
        for (partition_id, items) in grouped {
            let placement = definition
                .placements
                .get(&partition_id)
                .ok_or_else(|| Status::invalid_argument("delivery token partition 非法"))?;
            let response = self
                .apply_group_partition_request(
                    placement.shard_id,
                    es_storage::EsRequest::AggregateGroupPartition {
                        aggregate_type: aggregate_type.clone(),
                        partition_id,
                        partition_generation: placement.generation,
                        group_name: group.name.clone(),
                        group_epoch: group.epoch,
                        start_position: group.partition_starts[&partition_id],
                        settings: group.settings.clone(),
                        command: es_storage::AggregateGroupPartitionCommand::Settle {
                            consumer_id: request.consumer_id.clone(),
                            now_ms: unix_millis(),
                            settlements: items.iter().map(|(_, value)| value.clone()).collect(),
                        },
                    },
                )
                .await?;
            let statuses = map_group_settlement_response(response, false)?;
            for ((index, _), status) in items.into_iter().zip(statuses) {
                results[index] = Some(AggregateGroupSettlementResult {
                    delivery_id: request.settlements[index].delivery_id.clone(),
                    status,
                    deadline_ms: 0,
                });
            }
        }
        Ok(Response::new(SettleAggregateGroupResponse {
            results: results
                .into_iter()
                .map(|value| value.expect("每个 settlement 均已分组或标记 stale"))
                .collect(),
        }))
    }

    async fn renew_aggregate_group(
        &self,
        request: Request<RenewAggregateGroupRequest>,
    ) -> Result<Response<RenewAggregateGroupResponse>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        es_core::validate_aggregate_identifier("consumer_id", &request.consumer_id)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let group = self.require_group(&aggregate_type, &request.name).await?;
        let definition = self.require_active_aggregate_type(&aggregate_type).await?;
        let mut results = vec![None; request.delivery_ids.len()];
        let mut grouped: BTreeMap<u16, Vec<(usize, uuid::Uuid)>> = BTreeMap::new();
        for (index, delivery_id) in request.delivery_ids.iter().enumerate() {
            let token = decode_delivery_token(delivery_id, &aggregate_type, &request.name)?;
            if token.group_epoch != group.epoch {
                results[index] = Some(AggregateGroupSettlementResult {
                    delivery_id: delivery_id.clone(),
                    status: AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease
                        as i32,
                    deadline_ms: 0,
                });
            } else {
                grouped
                    .entry(token.partition_id)
                    .or_default()
                    .push((index, token.delivery_id));
            }
        }
        for (partition_id, items) in grouped {
            let placement = definition
                .placements
                .get(&partition_id)
                .ok_or_else(|| Status::invalid_argument("delivery token partition 非法"))?;
            let renewed_deadline_ms = unix_millis().saturating_add(group.settings.ack_timeout_ms);
            let response = self
                .apply_group_partition_request(
                    placement.shard_id,
                    es_storage::EsRequest::AggregateGroupPartition {
                        aggregate_type: aggregate_type.clone(),
                        partition_id,
                        partition_generation: placement.generation,
                        group_name: group.name.clone(),
                        group_epoch: group.epoch,
                        start_position: group.partition_starts[&partition_id],
                        settings: group.settings.clone(),
                        command: es_storage::AggregateGroupPartitionCommand::Renew {
                            consumer_id: request.consumer_id.clone(),
                            deadline_ms: renewed_deadline_ms,
                            delivery_ids: items.iter().map(|(_, value)| *value).collect(),
                        },
                    },
                )
                .await?;
            let statuses = map_group_settlement_response(response, true)?;
            for ((index, _), status) in items.into_iter().zip(statuses) {
                results[index] = Some(AggregateGroupSettlementResult {
                    delivery_id: request.delivery_ids[index].clone(),
                    status,
                    deadline_ms: if status
                        == AggregateGroupSettlementStatus::AggregateGroupSettlementApplied as i32
                    {
                        renewed_deadline_ms
                    } else {
                        0
                    },
                });
            }
        }
        Ok(Response::new(RenewAggregateGroupResponse {
            results: results
                .into_iter()
                .map(|value| value.expect("每个 delivery 均已分组或标记 stale"))
                .collect(),
        }))
    }
}

async fn run_aggregate_source(
    service: AggregateStoreService,
    shard_id: u64,
    aggregate_type: AggregateTypeId,
    mut cursors: Vec<AggregatePartitionCursor>,
    mut from_now: bool,
    tx: tokio::sync::mpsc::Sender<AggregateSourceMessage>,
) {
    loop {
        let request = SubscribeAggregatePartitionsInternalRequest {
            shard_id,
            aggregate_type: Some(aggregate_type_to_proto(&aggregate_type)),
            cursors: cursors.clone(),
            from_now,
        };
        let mut stream = match service.open_source_stream(shard_id, request).await {
            Ok(stream) => stream,
            Err(_) => {
                if tx
                    .send(AggregateSourceMessage::Degraded(shard_id))
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let mut failed = false;
        while let Some(item) = stream.next().await {
            let response = match item {
                Ok(response) => response,
                Err(_) => {
                    failed = true;
                    break;
                }
            };
            match response.payload {
                Some(subscribe_aggregate_partitions_internal_response::Payload::Event(event)) => {
                    if let Some(cursor) = cursors
                        .iter_mut()
                        .find(|cursor| cursor.partition_id == event.partition_id)
                    {
                        cursor.next_position = event.partition_position.saturating_add(1);
                    }
                    if tx
                        .send(AggregateSourceMessage::Event(shard_id, event))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Some(subscribe_aggregate_partitions_internal_response::Payload::CaughtUp(
                    heads,
                )) => {
                    for head in &heads.cursors {
                        if let Some(cursor) = cursors
                            .iter_mut()
                            .find(|cursor| cursor.partition_id == head.partition_id)
                        {
                            cursor.next_position = cursor.next_position.max(head.next_position);
                        }
                    }
                    from_now = false;
                    if tx
                        .send(AggregateSourceMessage::CaughtUp(shard_id, heads.cursors))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                None => {}
            }
        }
        if (failed || !from_now)
            && tx
                .send(AggregateSourceMessage::Degraded(shard_id))
                .await
                .is_err()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tonic::async_trait]
impl AggregateStoreInternal for AggregateStoreService {
    type SubscribeAggregatePartitionsInternalStream =
        ReceiverStream<Result<SubscribeAggregatePartitionsInternalResponse, Status>>;

    async fn get_aggregate_catalog_internal(
        &self,
        request: Request<GetAggregateCatalogInternalRequest>,
    ) -> Result<Response<GetAggregateCatalogInternalResponse>, Status> {
        let request = request.into_inner();
        self.require_control_shard(request.control_shard_id).await?;
        let shard = self.local_leader(request.control_shard_id).await?;
        let catalog = shard
            .storage
            .read_aggregate_catalog()
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(GetAggregateCatalogInternalResponse {
            payload: encode_bincode(&catalog, "AggregateCatalog")?,
        }))
    }

    async fn commit_aggregate_catalog_internal(
        &self,
        request: Request<CommitAggregateCatalogInternalRequest>,
    ) -> Result<Response<CommitAggregateCatalogInternalResponse>, Status> {
        let request = request.into_inner();
        self.require_control_shard(request.control_shard_id).await?;
        let command = decode_bincode(&request.payload, "AggregateCatalogCommand")?;
        let shard = self.local_leader(request.control_shard_id).await?;
        let response = shard
            .raft
            .client_write(es_storage::EsRequest::CommitAggregateCatalog { command })
            .await
            .map_err(client_write_to_status)?;
        let applied = match response.data {
            es_storage::EsResponse::AggregateCatalogApplied(applied) => applied,
            other => return Err(Status::internal(format!("catalog 返回意外结果: {other:?}"))),
        };
        Ok(Response::new(CommitAggregateCatalogInternalResponse {
            payload: encode_bincode(&applied, "AggregateCatalogApply")?,
        }))
    }

    async fn install_aggregate_partition_fence_internal(
        &self,
        request: Request<InstallAggregatePartitionFenceInternalRequest>,
    ) -> Result<Response<InstallAggregatePartitionFenceInternalResponse>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type)?;
        let partition_id = u16::try_from(request.partition_id)
            .map_err(|_| Status::invalid_argument("partition_id 超出范围"))?;
        let shard = self.local_leader(request.shard_id).await?;
        let response = shard
            .raft
            .client_write(es_storage::EsRequest::InstallAggregatePartitionFence {
                aggregate_type,
                partition_id,
                generation: request.generation,
            })
            .await
            .map_err(client_write_to_status)?;
        Ok(Response::new(
            InstallAggregatePartitionFenceInternalResponse {
                generation: map_fence_response(response.data)?,
            },
        ))
    }

    async fn subscribe_aggregate_partitions_internal(
        &self,
        request: Request<SubscribeAggregatePartitionsInternalRequest>,
    ) -> Result<Response<Self::SubscribeAggregatePartitionsInternalStream>, Status> {
        let request = request.into_inner();
        let aggregate_type = proto_aggregate_type(request.aggregate_type.clone())?;
        let shard = self.local_leader(request.shard_id).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(run_local_aggregate_subscription(
            shard.storage.clone(),
            aggregate_type,
            request.cursors,
            request.from_now,
            tx,
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_aggregate_partition_states_internal(
        &self,
        request: Request<ListAggregatePartitionStatesInternalRequest>,
    ) -> Result<Response<ListAggregatePartitionStatesInternalResponse>, Status> {
        let request = request.into_inner();
        let shard = self.local_leader(request.shard_id).await?;
        list_local_partition_states(&shard.storage, request).map(Response::new)
    }

    async fn get_aggregate_group_catalog_internal(
        &self,
        request: Request<GetAggregateGroupCatalogInternalRequest>,
    ) -> Result<Response<GetAggregateGroupCatalogInternalResponse>, Status> {
        let request = request.into_inner();
        self.require_control_shard(request.control_shard_id).await?;
        let shard = self.local_leader(request.control_shard_id).await?;
        let catalog = shard
            .storage
            .read_aggregate_group_catalog()
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(GetAggregateGroupCatalogInternalResponse {
            payload: encode_bincode(&catalog, "AggregateGroupCatalog")?,
        }))
    }

    async fn commit_aggregate_group_catalog_internal(
        &self,
        request: Request<CommitAggregateGroupCatalogInternalRequest>,
    ) -> Result<Response<CommitAggregateGroupCatalogInternalResponse>, Status> {
        let request = request.into_inner();
        self.require_control_shard(request.control_shard_id).await?;
        let command = decode_bincode(&request.payload, "AggregateGroupCatalogCommand")?;
        let shard = self.local_leader(request.control_shard_id).await?;
        let response = shard
            .raft
            .client_write(es_storage::EsRequest::CommitAggregateGroupCatalog { command })
            .await
            .map_err(client_write_to_status)?;
        let applied = match response.data {
            es_storage::EsResponse::AggregateGroupCatalogApplied(applied) => applied,
            other => {
                return Err(Status::internal(format!(
                    "消费者组 catalog 返回意外结果: {other:?}"
                )));
            }
        };
        Ok(Response::new(CommitAggregateGroupCatalogInternalResponse {
            payload: encode_bincode(&applied, "AggregateGroupCatalogApply")?,
        }))
    }

    async fn fetch_aggregate_group_partition_internal(
        &self,
        request: Request<FetchAggregateGroupPartitionInternalRequest>,
    ) -> Result<Response<FetchAggregateGroupPartitionInternalResponse>, Status> {
        let request = request.into_inner();
        let input: GroupPartitionFetchInput =
            decode_bincode(&request.payload, "GroupPartitionFetchInput")?;
        let shard = self.local_leader(request.shard_id).await?;
        let output = fetch_group_partition_local(shard, input).await?;
        Ok(Response::new(
            FetchAggregateGroupPartitionInternalResponse {
                payload: encode_bincode(&output, "GroupPartitionFetchOutput")?,
            },
        ))
    }

    async fn apply_aggregate_group_partition_internal(
        &self,
        request: Request<ApplyAggregateGroupPartitionInternalRequest>,
    ) -> Result<Response<ApplyAggregateGroupPartitionInternalResponse>, Status> {
        let request = request.into_inner();
        let command: es_storage::EsRequest =
            decode_bincode(&request.payload, "AggregateGroupPartition request")?;
        if !matches!(
            command,
            es_storage::EsRequest::AggregateGroupPartition { .. }
        ) {
            return Err(Status::invalid_argument(
                "内部接口只接受 AggregateGroupPartition 请求",
            ));
        }
        let shard = self.local_leader(request.shard_id).await?;
        let response = shard
            .raft
            .client_write(command)
            .await
            .map_err(client_write_to_status)?;
        Ok(Response::new(
            ApplyAggregateGroupPartitionInternalResponse {
                payload: encode_bincode(&response.data, "AggregateGroupPartition response")?,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aggregate_type() -> AggregateTypeId {
        AggregateTypeId::new("orders", "order").unwrap()
    }

    fn group_definition() -> es_core::AggregateGroupDefinition {
        es_core::AggregateGroupDefinition {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            revision: 2,
            epoch: 3,
            start: es_core::AggregateGroupStart::Beginning,
            partition_starts: BTreeMap::from([(0, 0)]),
            settings: es_core::AggregateGroupSettings::default(),
            create_operation_id: uuid::Uuid::new_v4(),
            last_operation_id: uuid::Uuid::new_v4(),
        }
    }

    #[test]
    fn cursor_rejects_other_aggregate_type_and_trailing_bytes() {
        let aggregate_type = AggregateTypeId::new("orders", "order").unwrap();
        let mut cursor = AggregateCursor {
            version: CURSOR_VERSION,
            aggregate_type: aggregate_type.clone(),
            next_positions: vec![0; 256],
        };
        let bytes = encode_cursor(&cursor).unwrap();
        assert!(decode_cursor(&bytes, &aggregate_type, 256).is_ok());
        let other = AggregateTypeId::new("billing", "invoice").unwrap();
        assert!(decode_cursor(&bytes, &other, 256).is_err());
        let mut trailing = bytes;
        trailing.push(0);
        assert!(decode_cursor(&trailing, &aggregate_type, 256).is_err());

        cursor.version = CURSOR_VERSION + 1;
        let bytes = encode_cursor(&cursor).unwrap();
        assert!(decode_cursor(&bytes, &aggregate_type, 256).is_err());
        cursor.version = CURSOR_VERSION;
        cursor.next_positions.pop();
        let bytes = encode_cursor(&cursor).unwrap();
        assert!(decode_cursor(&bytes, &aggregate_type, 256).is_err());
    }

    #[test]
    fn state_page_token_validates_partition_count() {
        let aggregate_type = AggregateTypeId::new("orders", "order").unwrap();
        let mut token = StatePageToken {
            version: STATE_PAGE_TOKEN_VERSION,
            aggregate_type: aggregate_type.clone(),
            after_aggregate_ids: vec![String::new(); 255],
        };
        let bytes = encode_bincode(&token, "token").unwrap();
        assert!(decode_state_page_token(&bytes, &aggregate_type, 256).is_err());

        token.after_aggregate_ids.push(String::new());
        let bytes = encode_bincode(&token, "token").unwrap();
        assert!(decode_state_page_token(&bytes, &aggregate_type, 256).is_ok());
        let other = AggregateTypeId::new("billing", "invoice").unwrap();
        assert!(decode_state_page_token(&bytes, &other, 256).is_err());

        token.version = STATE_PAGE_TOKEN_VERSION + 1;
        let bytes = encode_bincode(&token, "token").unwrap();
        assert!(decode_state_page_token(&bytes, &aggregate_type, 256).is_err());
    }

    #[test]
    fn protocol_inputs_and_aggregate_type_statuses_map_all_variants() {
        assert!(proto_aggregate_type(None).is_err());
        assert!(
            proto_aggregate_type(Some(AggregateTypeRef {
                business_space: "bad/path".into(),
                aggregate_type: "order".into(),
            }))
            .is_err()
        );
        assert_eq!(
            proto_aggregate_type(Some(aggregate_type_to_proto(&aggregate_type()))).unwrap(),
            aggregate_type()
        );

        use expected_aggregate_version::Kind as AggregateKind;
        for (input, expected) in [
            (None, es_core::ExpectedAggregateVersion::Any),
            (
                Some(ExpectedAggregateVersion {
                    kind: Some(AggregateKind::Any(Empty {})),
                }),
                es_core::ExpectedAggregateVersion::Any,
            ),
            (
                Some(ExpectedAggregateVersion {
                    kind: Some(AggregateKind::NoAggregate(Empty {})),
                }),
                es_core::ExpectedAggregateVersion::NoAggregate,
            ),
            (
                Some(ExpectedAggregateVersion {
                    kind: Some(AggregateKind::AggregateExists(Empty {})),
                }),
                es_core::ExpectedAggregateVersion::AggregateExists,
            ),
            (
                Some(ExpectedAggregateVersion {
                    kind: Some(AggregateKind::Exact(9)),
                }),
                es_core::ExpectedAggregateVersion::Exact(9),
            ),
        ] {
            assert_eq!(proto_expected_version(input).unwrap(), expected);
        }

        assert!(proto_expected_state_revision(None).is_err());
        assert_eq!(
            proto_expected_state_revision(Some(ExpectedStateRevision {
                kind: Some(expected_state_revision::Kind::Absent(Empty {})),
            }))
            .unwrap(),
            es_core::ExpectedStateRevision::Absent
        );
        assert_eq!(
            proto_expected_state_revision(Some(ExpectedStateRevision {
                kind: Some(expected_state_revision::Kind::Exact(4)),
            }))
            .unwrap(),
            es_core::ExpectedStateRevision::Exact(4)
        );

        for (status, expected) in [
            (
                AggregateTypeStatus::Registering,
                es_proto::eventstore::AggregateTypeStatus::AggregateTypeRegistering as i32,
            ),
            (
                AggregateTypeStatus::Active,
                es_proto::eventstore::AggregateTypeStatus::AggregateTypeActive as i32,
            ),
        ] {
            let value = es_core::AggregateTypeDefinition {
                id: aggregate_type(),
                create_operation_id: uuid::Uuid::new_v4(),
                create_plan_fingerprint: 0,
                seed: [0; 16],
                partition_count: 256,
                hash_algorithm: es_core::EventPartitionHash::Xxh3V1,
                status,
                placements: BTreeMap::new(),
            };
            assert_eq!(aggregate_type_info(&value, 8).status, expected);
        }
    }

    #[test]
    fn append_fence_and_group_responses_preserve_grpc_categories() {
        use es_storage::EsResponse;
        assert_eq!(
            map_append_response(EsResponse::AggregateAppendOk {
                aggregate_version: 4,
                partition_position: 9,
            })
            .unwrap()
            .aggregate_version,
            4
        );
        for (response, code) in [
            (
                EsResponse::AggregateOptimisticConflict {
                    actual_version: Some(3),
                },
                tonic::Code::FailedPrecondition,
            ),
            (
                EsResponse::AggregateIdempotencyConflict,
                tonic::Code::AlreadyExists,
            ),
            (
                EsResponse::AggregatePartitionFenced {
                    current_generation: 2,
                },
                tonic::Code::Unavailable,
            ),
            (
                EsResponse::AggregateInvalid {
                    reason: "bad".into(),
                },
                tonic::Code::InvalidArgument,
            ),
            (EsResponse::Noop, tonic::Code::Internal),
        ] {
            assert_eq!(map_append_response(response).unwrap_err().code(), code);
        }
        assert_eq!(
            map_fence_response(EsResponse::AggregatePartitionFenceInstalled { generation: 7 })
                .unwrap(),
            7
        );
        assert_eq!(
            map_fence_response(EsResponse::AggregateInvalid {
                reason: "bad".into(),
            })
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            map_fence_response(EsResponse::Noop).unwrap_err().code(),
            tonic::Code::Internal
        );

        let domain_results = vec![
            es_core::AggregateSettlementResult::Applied,
            es_core::AggregateSettlementResult::AlreadySettled,
            es_core::AggregateSettlementResult::StaleLease,
            es_core::AggregateSettlementResult::WrongConsumer,
        ];
        assert_eq!(
            map_group_settlement_response(
                EsResponse::AggregateGroupSettled(domain_results.clone()),
                false,
            )
            .unwrap()
            .len(),
            4
        );
        assert!(
            map_group_settlement_response(EsResponse::AggregateGroupRenewed(domain_results), true,)
                .is_ok()
        );
        for (response, renew, code) in [
            (
                EsResponse::AggregateGroupSettled(Vec::new()),
                true,
                tonic::Code::Internal,
            ),
            (
                EsResponse::AggregateGroupRenewed(Vec::new()),
                false,
                tonic::Code::Internal,
            ),
            (
                EsResponse::AggregateGroupStaleEpoch { current_epoch: 2 },
                false,
                tonic::Code::FailedPrecondition,
            ),
            (
                EsResponse::AggregatePartitionFenced {
                    current_generation: 2,
                },
                false,
                tonic::Code::Unavailable,
            ),
            (
                EsResponse::AggregateInvalid {
                    reason: "bad".into(),
                },
                false,
                tonic::Code::InvalidArgument,
            ),
            (EsResponse::Noop, false, tonic::Code::Internal),
        ] {
            assert_eq!(
                map_group_settlement_response(response, renew)
                    .unwrap_err()
                    .code(),
                code
            );
        }
    }

    #[test]
    fn group_settings_starts_and_delivery_tokens_validate_identity() {
        assert_eq!(
            proto_group_start(None).unwrap(),
            es_core::AggregateGroupStart::Beginning
        );
        assert_eq!(
            proto_group_start(Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Now(Empty {})),
            }))
            .unwrap(),
            es_core::AggregateGroupStart::Now
        );
        assert!(matches!(
            group_start_to_proto(es_core::AggregateGroupStart::Beginning).kind,
            Some(aggregate_group_start::Kind::Beginning(_))
        ));
        assert!(matches!(
            group_start_to_proto(es_core::AggregateGroupStart::Now).kind,
            Some(aggregate_group_start::Kind::Now(_))
        ));
        assert!(proto_group_settings(None).is_ok());
        assert!(
            proto_group_settings(Some(AggregateGroupSettings {
                max_unacked_per_consumer: 0,
                ..group_settings_to_proto(&es_core::AggregateGroupSettings::default())
            }))
            .is_err()
        );
        let definition = group_definition();
        let info = group_info(&definition);
        assert_eq!(info.name, "workers");

        let delivery_id = uuid::Uuid::new_v4();
        let bytes = encode_delivery_token(&definition, 0, delivery_id).unwrap();
        assert_eq!(
            decode_delivery_token(&bytes, &definition.aggregate_type, &definition.name)
                .unwrap()
                .delivery_id,
            delivery_id
        );
        assert!(decode_delivery_token(&bytes, &definition.aggregate_type, "other").is_err());
        let other = AggregateTypeId::new("billing", "invoice").unwrap();
        assert!(decode_delivery_token(&bytes, &other, &definition.name).is_err());
        let mut bad_version: es_core::AggregateDeliveryToken =
            decode_bincode(&bytes, "delivery token").unwrap();
        bad_version.version = 2;
        let bad_bytes = encode_bincode(&bad_version, "delivery token").unwrap();
        assert!(
            decode_delivery_token(&bad_bytes, &definition.aggregate_type, &definition.name)
                .is_err()
        );

        let empty = decode_state_page_token(&[], &definition.aggregate_type, 2).unwrap();
        assert_eq!(empty.after_aggregate_ids, vec![String::new(); 2]);
        assert!(decode_bincode::<AggregateCursor>(&[0xff], "cursor").is_err());
    }

    #[test]
    fn aggregate_read_merger_handles_invalid_frames_degradation_and_recovery() {
        let aggregate_type = aggregate_type();
        let cursor = AggregateCursor {
            version: CURSOR_VERSION,
            aggregate_type: aggregate_type.clone(),
            next_positions: vec![0, 0],
        };
        let mut merger = AggregateReadMerger::new(cursor, BTreeSet::from([3, 7]));

        let invalid_partition = InternalAggregateEvent {
            partition_id: u32::MAX,
            partition_position: 1,
            event: None,
        };
        assert!(
            merger
                .apply(AggregateSourceMessage::Event(3, invalid_partition))
                .is_empty()
        );
        let out_of_catalog = InternalAggregateEvent {
            partition_id: 2,
            partition_position: 1,
            event: None,
        };
        assert!(
            merger
                .apply(AggregateSourceMessage::Event(3, out_of_catalog))
                .is_empty()
        );
        let missing_event = InternalAggregateEvent {
            partition_id: 0,
            partition_position: 1,
            event: None,
        };
        assert!(
            merger
                .apply(AggregateSourceMessage::Event(3, missing_event))
                .is_empty()
        );

        let event = AggregateEvent {
            aggregate_id: "order-1".into(),
            aggregate_version: 0,
            event_id: vec![1; 16],
            event_type: "created".into(),
            data: b"{}".to_vec(),
            metadata: b"{}".to_vec(),
            hlc: None,
        };
        let responses = merger.apply(AggregateSourceMessage::Event(
            3,
            InternalAggregateEvent {
                partition_id: 0,
                partition_position: 5,
                event: Some(event),
            },
        ));
        assert_eq!(responses.len(), 1);
        let decoded = decode_cursor(&responses[0].cursor, &aggregate_type, 2).unwrap();
        assert_eq!(decoded.next_positions, vec![6, 0]);
        match responses[0].payload.as_ref().unwrap() {
            follow_aggregate_type_events_response::Payload::Event(event) => {
                assert_eq!(event.aggregate_id, "order-1");
            }
            other => panic!("预期事件，实际为 {other:?}"),
        }

        let degraded = merger.apply(AggregateSourceMessage::Degraded(3));
        assert_eq!(degraded.len(), 1);
        assert!(merger.apply(AggregateSourceMessage::Degraded(3)).is_empty());
        let first_source = merger.apply(AggregateSourceMessage::CaughtUp(
            3,
            vec![
                AggregatePartitionCursor {
                    partition_id: u32::MAX,
                    next_position: 99,
                },
                AggregatePartitionCursor {
                    partition_id: 2,
                    next_position: 99,
                },
                AggregatePartitionCursor {
                    partition_id: 0,
                    next_position: 9,
                },
            ],
        ));
        assert!(first_source.is_empty());

        let caught_up = merger.apply(AggregateSourceMessage::CaughtUp(7, Vec::new()));
        assert_eq!(caught_up.len(), 1);
        assert!(matches!(
            caught_up[0].payload,
            Some(follow_aggregate_type_events_response::Payload::CaughtUp(_))
        ));

        assert_eq!(merger.apply(AggregateSourceMessage::Degraded(3)).len(), 1);
        let recovered = merger.apply(AggregateSourceMessage::CaughtUp(3, Vec::new()));
        assert_eq!(recovered.len(), 1);
        assert!(matches!(
            recovered[0].payload,
            Some(follow_aggregate_type_events_response::Payload::Recovered(_))
        ));
        assert!(
            merger
                .apply(AggregateSourceMessage::CaughtUp(3, Vec::new()))
                .is_empty()
        );
    }
}
