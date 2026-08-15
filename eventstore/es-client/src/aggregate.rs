//! 聚合事件集客户端。
//!
//! 模块负责节点轮换、leader hint 重定向和 follow cursor 续读；调用方只处理
//! 聚合版本、状态 revision 与公开事件，不接触物理 Shard 或分区位置。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use es_core::{LeaderRetryPlan, parse_leader_hint};
use es_proto::endpoint::normalize_endpoint;
use es_proto::eventstore::aggregate_store_client::AggregateStoreClient as GrpcClient;
use es_proto::eventstore::*;
use es_proto::tls::{TlsClientConfig, apply_endpoint_tls};
use tokio_stream::{Stream, wrappers::ReceiverStream};
use tonic::transport::Channel;
use tonic::{Code, Streaming};

use crate::ClientError;

const ELECTION_RETRY_DELAY: Duration = Duration::from_millis(200);
const FOLLOW_RECONNECT_DELAY: Duration = Duration::from_millis(200);

/// 自动续读的聚合事件流。
///
/// 每个成功 frame 都携带服务端 opaque cursor。可重试断线后客户端用最后一个
/// cursor 重建订阅，因此批边界可能重投，但不会主动跳过已确认给调用方的 frame。
pub type AggregateFollowStream =
    Pin<Box<dyn Stream<Item = Result<ReadAggregateEventsResponse, ClientError>> + Send>>;

/// AggregateStore 客户端。
///
/// unary 请求会轮换节点并消费 leader hint；只有幂等请求会在普通瞬时错误后重试，
/// `PutState` 与 `Fetch` 仅在服务端明确返回 leader hint 时重定向。[`Self::follow`]
/// 会在 `Unavailable`、`DeadlineExceeded`、`Internal` 或连接结束后自动续读。
#[derive(Debug, Clone)]
pub struct AggregateStoreClient {
    inner: Arc<AggregateStoreClientInner>,
}

#[derive(Debug)]
struct AggregateStoreClientInner {
    clients: tokio::sync::RwLock<HashMap<String, GrpcClient<Channel>>>,
    nodes: Vec<String>,
    tls: Option<TlsClientConfig>,
    cursor: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RetrySafety {
    Idempotent,
    Ambiguous,
}

impl AggregateStoreClient {
    /// 连接集群并使用默认 HTTPS 信任策略。
    ///
    /// # 参数
    /// `nodes` 至少包含一个 HTTP/HTTPS 节点地址。
    ///
    /// # 返回
    /// 返回可复用的 AggregateStore 客户端。
    ///
    /// # 错误
    /// 节点列表为空、地址非法或首节点连接失败时返回 [`ClientError`]。
    pub async fn connect(nodes: Vec<String>) -> Result<Self, ClientError> {
        Self::connect_with_tls(nodes, None).await
    }

    /// 连接集群并指定 HTTPS 信任策略。
    ///
    /// # 参数
    /// `nodes` 是候选节点；`tls` 仅作用于 HTTPS 地址。
    ///
    /// # 返回
    /// 返回可复用的 AggregateStore 客户端。
    ///
    /// # 错误
    /// 配置非法或首节点不可达时返回 [`ClientError`]。
    pub async fn connect_with_tls(
        nodes: Vec<String>,
        tls: Option<TlsClientConfig>,
    ) -> Result<Self, ClientError> {
        if nodes.is_empty() {
            return Err(ClientError::InvalidConfig(
                "nodes list cannot be empty".into(),
            ));
        }
        let nodes: Vec<String> = nodes
            .into_iter()
            .map(|node| normalize_endpoint(&node))
            .collect();
        let first = nodes[0].clone();
        let client = Self::connect_one(&first, tls.as_ref()).await?;
        Ok(Self {
            inner: Arc::new(AggregateStoreClientInner {
                clients: tokio::sync::RwLock::new(HashMap::from([(first, client)])),
                nodes,
                tls,
                cursor: AtomicUsize::new(0),
            }),
        })
    }

    async fn connect_one(
        address: &str,
        tls: Option<&TlsClientConfig>,
    ) -> Result<GrpcClient<Channel>, ClientError> {
        let endpoint =
            tonic::transport::Endpoint::from_shared(address.to_string()).map_err(|error| {
                ClientError::InvalidConfig(format!("非法节点地址 {address}: {error}"))
            })?;
        let endpoint = apply_endpoint_tls(endpoint, tls).map_err(|error| {
            ClientError::InvalidConfig(format!("节点 {address} TLS 配置失败: {error}"))
        })?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| ClientError::ConnectionFailed(error.to_string()))?;
        Ok(GrpcClient::new(channel)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }

    async fn get_or_connect(&self, address: &str) -> Result<GrpcClient<Channel>, ClientError> {
        if let Some(client) = self.inner.clients.read().await.get(address) {
            return Ok(client.clone());
        }
        let client = Self::connect_one(address, self.inner.tls.as_ref()).await?;
        Ok(self
            .inner
            .clients
            .write()
            .await
            .entry(address.to_string())
            .or_insert(client)
            .clone())
    }

    fn rotated_nodes(&self) -> Vec<String> {
        let start = self.inner.cursor.fetch_add(1, Ordering::Relaxed) % self.inner.nodes.len();
        (0..self.inner.nodes.len())
            .map(|offset| self.inner.nodes[(start + offset) % self.inner.nodes.len()].clone())
            .collect()
    }

    async fn call<T, F, Fut>(&self, safety: RetrySafety, operation: F) -> Result<T, ClientError>
    where
        F: Fn(GrpcClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let mut plan = LeaderRetryPlan::new(self.rotated_nodes());
        let mut last_redirect = None;
        let mut errors = Vec::new();
        while let Some(target) = plan.next() {
            if plan.needs_backoff(&target) {
                tokio::time::sleep(ELECTION_RETRY_DELAY).await;
            }
            let client = match self.get_or_connect(&target).await {
                Ok(client) => client,
                Err(error) => {
                    errors.push(format!("{target}: {error}"));
                    plan.retry_later(target);
                    continue;
                }
            };
            match operation(client).await {
                Ok(response) => return Ok(response),
                Err(status) if status.code() == Code::Unavailable => {
                    if let Some(address) = parse_leader_hint(status.message()) {
                        let address = normalize_endpoint(&address);
                        last_redirect = Some(address.clone());
                        plan.redirect_to(address);
                    } else if safety == RetrySafety::Idempotent {
                        plan.retry_later(target);
                    } else {
                        return Err(ClientError::from_status(status));
                    }
                }
                Err(status)
                    if safety == RetrySafety::Idempotent && is_retryable_code(status.code()) =>
                {
                    errors.push(format!("{target}: {}", status.message()));
                    plan.retry_later(target);
                }
                Err(status) => return Err(ClientError::from_status(status)),
            }
        }
        match (last_redirect, errors.is_empty()) {
            (Some(address), _) => Err(ClientError::NotLeader(Some(address))),
            (None, false) => Err(ClientError::AllNodesFailed(errors.join("；"))),
            (None, true) => Err(ClientError::NotLeader(None)),
        }
    }

    async fn open_follow(
        &mut self,
        request: ReadAggregateEventsRequest,
    ) -> Result<Streaming<ReadAggregateEventsResponse>, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .read_aggregate_events(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 查询服务端 AggregateStore 能力。
    ///
    /// # 返回
    /// 返回协议版本、固定分区数和 payload 限制。
    ///
    /// # 错误
    /// 全部节点不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn capabilities(&mut self) -> Result<AggregateStoreCapabilities, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| async move {
            client
                .get_aggregate_store_capabilities(GetAggregateStoreCapabilitiesRequest {})
                .await
                .map(|response| response.into_inner())
        })
        .await
    }

    /// 幂等创建聚合事件集。
    ///
    /// # 参数
    /// `request.operation_id` 必须为 16 字节 UUID，重试时必须保持不变。
    ///
    /// # 返回
    /// 返回激活后的事件集定义。
    ///
    /// # 错误
    /// 身份非法、operation ID 冲突、leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn create_event_set(
        &mut self,
        request: CreateEventSetRequest,
    ) -> Result<AggregateEventSetInfo, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .create_event_set(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 枚举聚合事件集。
    ///
    /// # 返回
    /// 返回 catalog 中的全部事件集。
    ///
    /// # 错误
    /// control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn list_event_sets(&mut self) -> Result<Vec<AggregateEventSetInfo>, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| async move {
            client
                .list_event_sets(ListEventSetsRequest {})
                .await
                .map(|response| response.into_inner().event_sets)
        })
        .await
    }

    /// 获取指定聚合事件集。
    ///
    /// # 参数
    /// `event_set` 由业务空间和聚合类型构成。
    ///
    /// # 返回
    /// 返回事件集定义。
    ///
    /// # 错误
    /// 事件集不存在、control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn get_event_set(
        &mut self,
        event_set: AggregateEventSetRef,
    ) -> Result<AggregateEventSetInfo, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = GetEventSetRequest {
                event_set: Some(event_set.clone()),
            };
            async move {
                client
                    .get_event_set(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 以实例级 OCC 追加一条事件。
    ///
    /// # 参数
    /// `request` 包含事件集、聚合 ID、期望版本和带稳定 UUID 的事件。
    ///
    /// # 返回
    /// 返回服务端分配的聚合版本；不暴露物理 position。
    ///
    /// # 错误
    /// OCC/幂等冲突、payload 超限、leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn append(
        &mut self,
        request: AppendAggregateEventRequest,
    ) -> Result<AppendAggregateEventResponse, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .append_aggregate_event(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 从 Beginning、Now 或 opaque cursor 持续跟随聚合事件。
    ///
    /// # 参数
    /// `request.start` 决定首连起点；续连始终使用最近成功 frame 的 opaque cursor。
    ///
    /// # 返回
    /// 返回自动重连的流，包含事件、caught-up、degraded 和 recovered frame。
    ///
    /// # 错误
    /// 首次建流失败时直接返回 [`ClientError`]；流建立后的永久错误通过流元素返回。
    pub async fn follow(
        &mut self,
        request: ReadAggregateEventsRequest,
    ) -> Result<AggregateFollowStream, ClientError> {
        let initial = self.open_follow(request.clone()).await?;
        let mut worker = self.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            run_follow(&mut worker, request, initial, tx).await;
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    /// 分页枚举状态实例。
    ///
    /// # 参数
    /// `request.page_token` 必须原样使用上一页返回值。
    ///
    /// # 返回
    /// 返回本页身份/revision 与下一页 opaque token。
    ///
    /// # 错误
    /// token 非法、任一数据源不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn list_states(
        &mut self,
        request: ListAggregateStatesRequest,
    ) -> Result<ListAggregateStatesResponse, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .list_aggregate_states(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 读取单个聚合实例状态。
    ///
    /// # 参数
    /// `request` 指定事件集与聚合 ID。
    ///
    /// # 返回
    /// 返回状态 revision 和原始 JSON bytes。
    ///
    /// # 错误
    /// 状态不存在、leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn get_state(
        &mut self,
        request: GetAggregateStateRequest,
    ) -> Result<GetAggregateStateResponse, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .get_aggregate_state(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 以 revision CAS 覆盖单个聚合实例状态。
    ///
    /// # 参数
    /// `request.expected_revision` 必填，使用 Absent 或 Exact(n)。
    ///
    /// # 返回
    /// 返回提交后的 revision。
    ///
    /// # 错误
    /// 聚合不存在、revision 冲突、leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn put_state(
        &mut self,
        request: PutAggregateStateRequest,
    ) -> Result<PutAggregateStateResponse, ClientError> {
        self.call(RetrySafety::Ambiguous, |mut client| {
            let request = request.clone();
            async move {
                client
                    .put_aggregate_state(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 查询 AggregateStore catalog 状态。
    ///
    /// # 返回
    /// 返回 catalog revision 及创建中/已激活事件集数量。
    ///
    /// # 错误
    /// control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn status(&mut self) -> Result<AggregateStoreStatus, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| async move {
            client
                .get_aggregate_store_status(GetAggregateStoreStatusRequest {})
                .await
                .map(|response| response.into_inner())
        })
        .await
    }

    /// 创建聚合消费者组。
    ///
    /// # 参数
    /// `request.operation_id` 必须在模糊重试时保持稳定。
    ///
    /// # 返回
    /// 返回 revision 1、epoch 1 的组定义。
    ///
    /// # 错误
    /// 组已存在、参数非法、control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn create_group(
        &mut self,
        request: CreateAggregateGroupRequest,
    ) -> Result<AggregateGroupInfo, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .create_aggregate_group(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 以 revision CAS 更新组设置或 reset 起点。
    ///
    /// # 参数
    /// `request` 携带 expected revision 和稳定 operation ID。
    ///
    /// # 返回
    /// 返回递增 revision/epoch 后的组定义。
    ///
    /// # 错误
    /// revision 冲突、组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn update_group(
        &mut self,
        request: UpdateAggregateGroupRequest,
    ) -> Result<AggregateGroupInfo, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .update_aggregate_group(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 以 revision CAS 删除消费者组定义。
    ///
    /// # 参数
    /// `request` 携带事件集、组名、expected revision 和 operation ID。
    ///
    /// # 返回
    /// 删除成功返回 `Ok(())`。
    ///
    /// # 错误
    /// revision 冲突、组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn delete_group(
        &mut self,
        request: DeleteAggregateGroupRequest,
    ) -> Result<(), ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move { client.delete_aggregate_group(request).await.map(|_| ()) }
        })
        .await
    }

    /// 获取一个聚合消费者组。
    ///
    /// # 参数
    /// `request` 指定事件集和组名。
    ///
    /// # 返回
    /// 返回当前组定义。
    ///
    /// # 错误
    /// 组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn get_group(
        &mut self,
        request: GetAggregateGroupRequest,
    ) -> Result<AggregateGroupInfo, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .get_aggregate_group(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 枚举事件集的聚合消费者组。
    ///
    /// # 参数
    /// `event_set` 指定业务空间和聚合类型。
    ///
    /// # 返回
    /// 返回按 catalog 顺序排列的组定义。
    ///
    /// # 错误
    /// control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn list_groups(
        &mut self,
        event_set: AggregateEventSetRef,
    ) -> Result<Vec<AggregateGroupInfo>, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = ListAggregateGroupsRequest {
                event_set: Some(event_set.clone()),
            };
            async move {
                client
                    .list_aggregate_groups(request)
                    .await
                    .map(|response| response.into_inner().groups)
            }
        })
        .await
    }

    /// 按服务端额度长轮询一批 delivery。
    ///
    /// # 参数
    /// `request.consumer_id` 标识成员；返回 token 不泄露分区或 epoch。
    ///
    /// # 返回
    /// 返回 delivery、caught-up 和 throttled 状态。
    ///
    /// # 错误
    /// 组不存在、数据分区不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn fetch_group(
        &mut self,
        request: FetchAggregateGroupRequest,
    ) -> Result<FetchAggregateGroupResponse, ClientError> {
        self.call(RetrySafety::Ambiguous, |mut client| {
            let request = request.clone();
            async move {
                client
                    .fetch_aggregate_group(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 批量显式结算 delivery。
    ///
    /// # 参数
    /// `request.settlements` 使用 Fetch 返回的 opaque token。
    ///
    /// # 返回
    /// 返回与输入顺序一致的逐条状态。
    ///
    /// # 错误
    /// token 格式非法、组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn settle_group(
        &mut self,
        request: SettleAggregateGroupRequest,
    ) -> Result<SettleAggregateGroupResponse, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .settle_aggregate_group(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 批量续租未结算 delivery。
    ///
    /// # 参数
    /// `request.delivery_ids` 使用 Fetch 返回的 opaque token。
    ///
    /// # 返回
    /// 返回与输入顺序一致的逐条状态。
    ///
    /// # 错误
    /// token 格式非法、组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn renew_group(
        &mut self,
        request: RenewAggregateGroupRequest,
    ) -> Result<RenewAggregateGroupResponse, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = request.clone();
            async move {
                client
                    .renew_aggregate_group(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 查询事件集的内部放置状态，供运维诊断使用。
    ///
    /// # 参数
    /// `event_set` 指定业务空间与聚合类型。
    ///
    /// # 返回
    /// 返回固定虚拟分区的 Shard、generation 与迁移状态。
    ///
    /// # 错误
    /// 事件集不存在、control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn list_partitions(
        &mut self,
        event_set: AggregateEventSetRef,
    ) -> Result<Vec<AggregatePartitionInfo>, ClientError> {
        self.call(RetrySafety::Idempotent, |mut client| {
            let request = ListAggregatePartitionsRequest {
                event_set: Some(event_set.clone()),
            };
            async move {
                client
                    .list_aggregate_partitions(request)
                    .await
                    .map(|response| response.into_inner().partitions)
            }
        })
        .await
    }
}

async fn run_follow(
    client: &mut AggregateStoreClient,
    mut request: ReadAggregateEventsRequest,
    mut stream: Streaming<ReadAggregateEventsResponse>,
    tx: tokio::sync::mpsc::Sender<Result<ReadAggregateEventsResponse, ClientError>>,
) {
    loop {
        let reconnect = loop {
            match stream.message().await {
                Ok(Some(frame)) => {
                    if !frame.cursor.is_empty() {
                        request.start = Some(AggregateReadStart {
                            kind: Some(aggregate_read_start::Kind::Cursor(frame.cursor.clone())),
                        });
                    }
                    if tx.send(Ok(frame)).await.is_err() {
                        return;
                    }
                }
                Ok(None) => break true,
                Err(status) if is_retryable_code(status.code()) => break true,
                Err(status) => {
                    let _ = tx.send(Err(ClientError::from_status(status))).await;
                    return;
                }
            }
        };
        if !reconnect {
            return;
        }
        loop {
            if tx.is_closed() {
                return;
            }
            tokio::time::sleep(FOLLOW_RECONNECT_DELAY).await;
            match client.open_follow(request.clone()).await {
                Ok(next) => {
                    stream = next;
                    break;
                }
                Err(error) if is_retryable_client_error(&error) => continue,
                Err(error) => {
                    let _ = tx.send(Err(error)).await;
                    return;
                }
            }
        }
    }
}

fn is_retryable_code(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable | Code::DeadlineExceeded | Code::Internal
    )
}

fn is_retryable_client_error(error: &ClientError) -> bool {
    match error {
        ClientError::ConnectionFailed(_)
        | ClientError::AllNodesFailed(_)
        | ClientError::NotLeader(_) => true,
        ClientError::RpcFailed { code, .. } => is_retryable_code(*code),
        ClientError::InvalidConfig(_) | ClientError::PayloadTooLarge(_) => false,
    }
}
