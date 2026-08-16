//! gRPC 服务实现。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use es_proto::eventstore::event_store_client::EventStoreClient;
use es_proto::eventstore::event_store_server::EventStore;
use es_proto::eventstore::internal_subscription_client::InternalSubscriptionClient;
use es_proto::eventstore::internal_subscription_server::InternalSubscription;
use es_proto::eventstore::*;
use es_proto::tls::TlsClientConfig;
use es_raft::ShardManager;

use crate::config::Config;
use crate::ownership::{AppendTarget, StreamOwnership};
use crate::route_table::{RouteTableManager, routes_path};

/// 远程 shard 的 leader 探测器。
///
/// 本节点只承载放置表分配的子集；客户端请求路由到本节点不承载的 shard 时，
/// 无法在本地处理，需要向 peers 探测该 shard 的 leader 并把客户端引过去。
/// 探测结果与本地 openraft `ForwardToLeader` 的错误格式对齐，
/// 客户端 `LeaderRetryPlan` 无需感知差别。
#[derive(Clone)]
pub struct RemoteShards {
    /// 本节点 ID（探测时跳过自己）
    self_id: u64,
    /// peers 的 RaftAdmin 客户端（惰性连接）
    clients: BTreeMap<
        u64,
        es_proto::eventstore::raft_admin_client::RaftAdminClient<tonic::transport::Channel>,
    >,
    /// node_id -> 已 normalize 的地址（重定向提示用）
    addrs: BTreeMap<u64, String>,
    /// node_id -> 仅节点间可访问的内部订阅地址。
    internal_addrs: BTreeMap<u64, String>,
    /// 内部订阅转发沿用节点间 TLS 信任策略。
    tls: Option<TlsClientConfig>,
}

impl RemoteShards {
    /// 从配置构建（地址 normalize 与 bootstrap 同源）。
    pub fn new(config: &Config) -> Result<Self, String> {
        let members: BTreeMap<u64, openraft::BasicNode> = config
            .node
            .peers
            .iter()
            .map(|p| {
                let uri = es_raft::normalize_endpoint(&p.addr);
                (p.id, openraft::BasicNode { addr: uri.clone() })
            })
            .collect();
        let tls: Option<TlsClientConfig> = match &config.tls {
            Some(t) => Some(t.client_trust().map_err(|e| e)?),
            None => None,
        };
        let clients = crate::bootstrap::build_clients(&members, tls.as_ref())?;
        let addrs = members.into_iter().map(|(id, n)| (id, n.addr)).collect();
        let internal_addrs = config
            .node
            .peers
            .iter()
            .filter_map(|peer| {
                peer.internal_addr
                    .as_ref()
                    .map(|addr| (peer.id, es_raft::normalize_endpoint(addr)))
            })
            .collect();
        Ok(Self {
            self_id: config.node.id,
            clients,
            addrs,
            internal_addrs,
            tls,
        })
    }

    /// 探测目标 shard 的 leader，返回 `(leader_node_id, addr)`。
    ///
    /// 轮询所有 peers（跳过自己）调 GetRaftState；未承载该 shard 的节点
    /// 返回 NotFound，直接跳过。全部不可达或尚无 leader → None。
    pub(crate) async fn find_leader(&self, shard_id: u64) -> Option<(u64, String)> {
        for (&id, client) in &self.clients {
            if id == self.self_id {
                continue;
            }
            let mut c = client.clone();
            match c.get_raft_state(GetRaftStateRequest { shard_id }).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if r.is_leader {
                        return self
                            .addrs
                            .get(&id)
                            .map(|addr| (id, addr.clone()))
                            .or_else(|| Some((id, String::new())));
                    }
                }
                Err(_) => continue, // 节点未就绪或未承载该 shard
            }
        }
        None
    }

    /// 目标 shard 不在本节点时的标准重定向提示（与本地 ForwardToLeader 同格式）。
    pub(crate) async fn leader_hint_status(&self, shard_id: u64) -> Status {
        match self.find_leader(shard_id).await {
            Some((id, addr)) => {
                Status::unavailable(format!("not leader; leader_id={id} leader_addr={addr}"))
            }
            None => Status::unavailable("not leader; leader unknown, retry later"),
        }
    }

    /// 从目标 shard leader 读取单分片 `$all` 数据。
    ///
    /// `ReadAll` 的一个请求可能覆盖多个分片，而任一节点只承载其中一部分；
    /// 此方法只发送单分片请求，确保目标 leader 可在本地终止处理，不会递归代理。
    async fn read_all_shard(
        &self,
        cursor: &ShardPosition,
        max_count: u64,
        direction: i32,
    ) -> Result<Vec<Event>, Status> {
        let (_, addr) = self
            .find_leader(cursor.shard_id)
            .await
            .ok_or_else(|| Status::unavailable("read-all shard source unavailable"))?;
        let endpoint = tonic::transport::Endpoint::from_shared(addr)
            .map_err(|_| Status::unavailable("read-all shard source unavailable"))?;
        let endpoint = es_proto::tls::apply_endpoint_tls(endpoint, self.tls.as_ref())
            .map_err(|_| Status::unavailable("read-all shard source unavailable"))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| Status::unavailable("read-all shard source unavailable"))?;
        let mut client = EventStoreClient::new(channel)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let mut responses = client
            .read_all(ReadAllRequest {
                shard_ids: Vec::new(),
                from_position: 0,
                max_count,
                direction,
                from_positions: vec![cursor.clone()],
            })
            .await?
            .into_inner();
        let mut events = Vec::new();
        while let Some(response) = responses.message().await? {
            events.extend(response.events);
        }
        Ok(events)
    }

    /// 连接远程 shard leader 的内部订阅服务。
    pub(crate) async fn internal_client(
        &self,
        shard_id: u64,
    ) -> Result<InternalSubscriptionClient<tonic::transport::Channel>, Status> {
        let (leader_id, _) = self
            .find_leader(shard_id)
            .await
            .ok_or_else(|| Status::unavailable("internal subscription source unavailable"))?;
        let addr = self
            .internal_addrs
            .get(&leader_id)
            .ok_or_else(|| Status::unavailable("internal subscription source unavailable"))?;
        let endpoint = tonic::transport::Endpoint::from_shared(addr.clone())
            .map_err(|_| Status::unavailable("internal subscription source unavailable"))?;
        let endpoint = es_proto::tls::apply_endpoint_tls(endpoint, self.tls.as_ref())
            .map_err(|_| Status::unavailable("internal subscription source unavailable"))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| Status::unavailable("internal subscription source unavailable"))?;
        Ok(InternalSubscriptionClient::new(channel)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }

    /// 连接远程 Shard leader 的 AggregateStore 内部服务。
    pub(crate) async fn aggregate_internal_client(
        &self,
        shard_id: u64,
    ) -> Result<
        es_proto::eventstore::aggregate_store_internal_client::AggregateStoreInternalClient<
            tonic::transport::Channel,
        >,
        Status,
    > {
        let (leader_id, _) = self
            .find_leader(shard_id)
            .await
            .ok_or_else(|| Status::unavailable("aggregate store source unavailable"))?;
        let addr = self
            .internal_addrs
            .get(&leader_id)
            .ok_or_else(|| Status::unavailable("aggregate store source unavailable"))?;
        let endpoint = tonic::transport::Endpoint::from_shared(addr.clone())
            .map_err(|_| Status::unavailable("aggregate store source unavailable"))?;
        let endpoint = es_proto::tls::apply_endpoint_tls(endpoint, self.tls.as_ref())
            .map_err(|_| Status::unavailable("aggregate store source unavailable"))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| Status::unavailable("aggregate store source unavailable"))?;
        Ok(es_proto::eventstore::aggregate_store_internal_client::AggregateStoreInternalClient::new(channel)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }
}

/// 归并用的堆元素：按 (hlc, shard_id, position) 定序
#[derive(PartialEq, Eq)]
struct MergeHead {
    hlc: (u64, u32),
    shard_id: u64,
    position: u64,
    /// 该元素来自第几路输入
    stream_idx: usize,
}

impl Ord for MergeHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hlc
            .cmp(&other.hlc)
            .then(self.shard_id.cmp(&other.shard_id))
            .then(self.position.cmp(&other.position))
    }
}

impl PartialOrd for MergeHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// 把多个已排序的分片流归并为一路，按 HLC 定序。
///
/// 用 k 路归并而非「合并后整体排序」：整体排序会让同一分片内的
/// 事件顺序完全由 HLC 决定，而 HLC 由各 leader 的墙上时钟推进，
/// 时钟回拨时同分片内的事件顺序就会被打乱——这违反了分片内
/// 严格按提交序（position）的保证。k 路归并只在「各路队首」之间比较，
/// 每路内部顺序原样保留，因此分片内的 position 序恒成立。
///
/// - `streams`: 各分片的事件流(已按 position 排序,升序或降序由调用方保证)
/// - `limit`: 最多返回多少条,0 表示不限量
/// - `descending`: true 表示降序归并(反向读),false 表示升序归并(正向读)
///
/// 返回 `(合并事件, 每路最后消费的 position)`：
/// 归并可能因 `limit` 全局截断而把某些路已读到的缓冲尾部丢弃，
/// 游标必须按「消费水位」推进（未消费的路为 None，下一页重读），
/// 否则被丢弃的数据会永久丢失。
fn merge_by_hlc(
    streams: Vec<Vec<Event>>,
    limit: u64,
    descending: bool,
) -> (Vec<Event>, Vec<Option<u64>>) {
    let mut consumed: Vec<Option<u64>> = vec![None; streams.len()];

    // 单路无需归并，直接返回，保持 position 序
    if streams.len() == 1 {
        let mut only = streams.into_iter().next().unwrap_or_default();
        if limit != 0 && only.len() as u64 > limit {
            only.truncate(limit as usize);
        }
        if let Some(last) = only.last() {
            consumed[0] = Some(last.position);
        }
        return (only, consumed);
    }

    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // 堆里只放游标而非事件本身：prost 生成的 Event 只有 PartialEq 没有 Ord，
    // 放进堆会导致类型不满足约束。事件留在 streams 里按下标取。
    let mut cursors = vec![0usize; streams.len()];

    let head_of = |idx: usize, e: &Event| MergeHead {
        hlc: e
            .hlc
            .as_ref()
            .map(|h| (h.wall, h.logical))
            .unwrap_or((0, 0)),
        shard_id: e.shard_id,
        position: e.position,
        stream_idx: idx,
    };

    let mut out = Vec::new();

    if descending {
        // 降序归并:用最大堆(不包 Reverse)
        let mut heap: BinaryHeap<MergeHead> = BinaryHeap::new();
        for (idx, s) in streams.iter().enumerate() {
            if let Some(e) = s.first() {
                heap.push(head_of(idx, e));
            }
        }

        while let Some(head) = heap.pop() {
            let idx = head.stream_idx;
            let pos = cursors[idx];
            consumed[idx] = Some(streams[idx][pos].position);
            out.push(streams[idx][pos].clone());
            cursors[idx] += 1;

            if limit != 0 && out.len() as u64 >= limit {
                break;
            }
            if let Some(next) = streams[idx].get(cursors[idx]) {
                heap.push(head_of(idx, next));
            }
        }
    } else {
        // 升序归并:用最小堆(包 Reverse)
        let mut heap: BinaryHeap<Reverse<MergeHead>> = BinaryHeap::new();
        for (idx, s) in streams.iter().enumerate() {
            if let Some(e) = s.first() {
                heap.push(Reverse(head_of(idx, e)));
            }
        }

        while let Some(Reverse(head)) = heap.pop() {
            let idx = head.stream_idx;
            let pos = cursors[idx];
            consumed[idx] = Some(streams[idx][pos].position);
            out.push(streams[idx][pos].clone());
            cursors[idx] += 1;

            if limit != 0 && out.len() as u64 >= limit {
                break;
            }
            if let Some(next) = streams[idx].get(cursors[idx]) {
                heap.push(Reverse(head_of(idx, next)));
            }
        }
    }

    (out, consumed)
}

/// 把 Raft 写入错误映射为 gRPC 状态。
///
/// 重点是 `ForwardToLeader`：本节点不是 leader 时 openraft 会返回它，且带上
/// leader 的 node_id 与地址。必须把地址透出给客户端，否则客户端只能盲目重试
/// 其它节点。错误码用 `Unavailable`——它是 gRPC 的可重试语义，
/// 客户端拿到后应重定向到 message 中的地址。
/// pub(crate)：migration_service（AppendMigrated/DeleteStreamFromShard）复用。
pub(crate) fn client_write_to_status(
    e: openraft::error::RaftError<u64, openraft::error::ClientWriteError<u64, openraft::BasicNode>>,
) -> Status {
    use openraft::error::{ClientWriteError, RaftError};

    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) => {
            let addr = fwd
                .leader_node
                .as_ref()
                .map(|n| n.addr.clone())
                .unwrap_or_default();
            match fwd.leader_id {
                Some(id) => {
                    Status::unavailable(format!("not leader; leader_id={id} leader_addr={addr}"))
                }
                // 选举中，暂无 leader，客户端应稍后重试
                None => Status::unavailable("not leader; leader unknown, retry later"),
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(err)) => {
            Status::failed_precondition(format!("成员变更错误: {err}"))
        }
        RaftError::Fatal(f) => Status::internal(format!("Raft 致命错误: {f}")),
    }
}

/// proto ExpectedVersion 转领域模型
/// pub(crate)：migration_service（AppendMigrated）复用。
pub(crate) fn proto_to_expected_version(ev: ExpectedVersion) -> es_core::ExpectedVersion {
    match ev.kind {
        Some(expected_version::Kind::Any(_)) => es_core::ExpectedVersion::Any,
        Some(expected_version::Kind::NoStream(_)) => es_core::ExpectedVersion::NoStream,
        Some(expected_version::Kind::StreamExists(_)) => es_core::ExpectedVersion::StreamExists,
        Some(expected_version::Kind::Exact(v)) => es_core::ExpectedVersion::Exact(v),
        None => es_core::ExpectedVersion::Any, // 默认
    }
}

/// EventStore gRPC 服务
#[derive(Clone)]
pub struct EsService {
    pub(crate) shard_manager: Arc<ShardManager>,
    /// 请求大小限制（append 权威校验）
    limits: crate::config::LimitsSection,
    /// 流路由表（stream → shard 归属；写路径权威）
    pub(crate) route_table: Arc<RouteTableManager>,
    /// 强一致归属；Append 只能通过它取得带 generation 的目标。
    pub(crate) ownership: Arc<StreamOwnership>,
    /// 远程 shard 定位（本节点不承载的目标分片）
    pub(crate) remote: RemoteShards,
    /// 建立 `$all` 聚合订阅时的 shard 快照；公共接口不暴露这些内部 ID。
    all_shards: Vec<u64>,
}

impl EsService {
    /// 创建服务实例（默认大小限制；自建路由表管理器——测试/独立使用便捷路径）。
    pub fn new(shard_manager: Arc<ShardManager>, config: &Config) -> Result<Self, String> {
        let route_table = Arc::new(RouteTableManager::new(
            config,
            routes_path(&config.storage.data_dir),
        )?);
        Self::with_limits(shard_manager, config.limits.clone(), route_table, config)
    }

    /// 创建服务实例（自定义大小限制 + 共享路由表管理器）。
    pub fn with_limits(
        shard_manager: Arc<ShardManager>,
        limits: crate::config::LimitsSection,
        route_table: Arc<RouteTableManager>,
        config: &Config,
    ) -> Result<Self, String> {
        let ownership = Arc::new(StreamOwnership::new(
            config,
            shard_manager.clone(),
            route_table.clone(),
        )?);
        Self::with_ownership(shard_manager, limits, route_table, ownership, config)
    }

    /// 创建服务实例，并复用服务器持有的强一致归属 module。
    pub fn with_ownership(
        shard_manager: Arc<ShardManager>,
        limits: crate::config::LimitsSection,
        route_table: Arc<RouteTableManager>,
        ownership: Arc<StreamOwnership>,
        config: &Config,
    ) -> Result<Self, String> {
        Ok(Self {
            shard_manager,
            limits,
            route_table,
            ownership,
            remote: RemoteShards::new(config)?,
            all_shards: config
                .placement
                .nodes
                .iter()
                .flat_map(|node| node.primary.iter().chain(node.replica.iter()))
                .copied()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect(),
        })
    }

    /// 写路径取得强一致归属目标；未知 Stream 不允许本地分配。
    async fn resolve_write_target(&self, stream_id: &str) -> Result<AppendTarget, Status> {
        self.ownership
            .for_append(stream_id)
            .await
            .map_err(crate::ownership::OwnershipError::into_status)
    }

    /// 读路径解析 stream 归属：只查不分配（读无副作用）。
    /// 未知流（从未创建或路由表缺失）→ NotFound，客户端可直接判定。
    async fn resolve_read_stream_shard(&self, stream_id: &str) -> Result<u64, Status> {
        if let Some(shard_id) = self.route_table.lookup(stream_id).await {
            return Ok(shard_id);
        }
        // 手动成员集群可能没有静态 peers 广播；从本地已应用 catalog 修复投影，
        // 不提交 Raft 命令，因此读 miss 仍不产生领域副作用。
        self.ownership
            .refresh_local_projection()
            .await
            .map_err(crate::ownership::OwnershipError::into_status)?;
        self.route_table
            .lookup(stream_id)
            .await
            .ok_or_else(|| Status::not_found(format!("stream '{stream_id}' not found")))
    }

    /// 取目标 shard；本节点不承载时给出标准重定向提示（写路径用）。
    async fn resolve_write_shard(&self, shard_id: u64) -> Result<Arc<es_raft::Shard>, Status> {
        match self.shard_manager.get_shard(shard_id).await {
            Ok(s) => Ok(s),
            // 本节点不承载该 shard：向 peers 探测 leader 并返回与本地
            // ForwardToLeader 相同格式的重定向提示，客户端原样重定向
            Err(_) => Err(self.remote.leader_hint_status(shard_id).await),
        }
    }

    /// 取目标 shard；本节点不承载时返回 Unavailable（读路径用）。
    ///
    /// 客户端（es-client/es-ctl）对 Unavailable 轮换其它节点，直到命中
    /// 承载该 shard 的节点；不能用 NotFound——那是客户端语义错误，
    /// 轮换逻辑不会触发。
    async fn resolve_read_shard(&self, shard_id: u64) -> Result<Arc<es_raft::Shard>, Status> {
        match self.shard_manager.get_shard(shard_id).await {
            Ok(s) => Ok(s),
            Err(_) => Err(Status::unavailable(format!(
                "shard {shard_id} not on this node, retry other nodes"
            ))),
        }
    }
}

/// 将内部事件投影为公开订阅事件，避免向客户端泄露分片位置与 shard ID。
pub(crate) fn public_subscription_event(event: Event) -> SubscriptionEvent {
    SubscriptionEvent {
        stream_id: event.stream_id,
        version: event.version,
        event_id: event.event_id,
        event_type: event.event_type,
        data: event.data,
        metadata: event.metadata,
        hlc: event.hlc,
    }
}

/// 聚合器接收的内部来源状态；不会序列化到公开协议。
enum SourceMessage {
    Event(Event),
    CaughtUp(u64),
    Degraded(u64),
}

/// 将内部来源消息转换为公开订阅消息，并维护聚合阶段状态。
fn aggregate_source_message(
    message: SourceMessage,
    pending_catch_up: &mut BTreeSet<u64>,
    caught_up: &mut bool,
    degraded: &mut bool,
) -> Option<SubscribeResponse> {
    match message {
        SourceMessage::Event(event) => Some(SubscribeResponse {
            payload: Some(subscribe_response::Payload::Event(
                public_subscription_event(event),
            )),
        }),
        SourceMessage::CaughtUp(source) => {
            pending_catch_up.remove(&source);
            if !pending_catch_up.is_empty() || *caught_up {
                return None;
            }
            *caught_up = true;
            Some(SubscribeResponse {
                payload: Some(subscribe_response::Payload::CaughtUp(Empty {})),
            })
        }
        SourceMessage::Degraded(source) if !*degraded => {
            *degraded = true;
            tracing::warn!(source, "聚合订阅的内部来源已降级");
            Some(SubscribeResponse {
                payload: Some(subscribe_response::Payload::Degraded(Empty {})),
            })
        }
        SourceMessage::Degraded(_) => None,
    }
}

/// 在一个本地 shard 上执行“先注册广播、再补历史、最后实时推送”的内部订阅。
///
/// stream_ids 为空时订阅该 shard 的全部 stream；非空时仅转发指定 stream。
async fn run_local_subscription(
    shard_id: u64,
    storage: Arc<es_storage::EsStorage>,
    stream_ids: Vec<String>,
    tx: tokio::sync::mpsc::Sender<Result<InternalSubscribeResponse, Status>>,
) {
    let streams: BTreeSet<String> = stream_ids.into_iter().collect();
    let all_streams = streams.is_empty();
    let mut event_rx = storage.subscribe_events();

    let historical = if all_streams {
        storage.read_all_events(0, 0).unwrap_or_default()
    } else {
        let mut events = Vec::new();
        for stream_id in &streams {
            events.extend(
                storage
                    .read_stream_events(stream_id, 0, 0)
                    .unwrap_or_default(),
            );
        }
        events
    };

    let mut watermarks: BTreeMap<String, u64> = BTreeMap::new();
    for event in &historical {
        watermarks.insert(event.stream_id.clone(), event.version);
    }

    for event in historical {
        let event = Event {
            stream_id: event.stream_id,
            version: event.version,
            event_id: event.event_id.as_bytes().to_vec(),
            event_type: event.event_type,
            data: event.data,
            metadata: event.metadata,
            hlc: Some(Hlc {
                wall: event.hlc.wall,
                logical: event.hlc.logical,
            }),
            position: event.position,
            shard_id,
        };
        if tx
            .send(Ok(InternalSubscribeResponse {
                payload: Some(internal_subscribe_response::Payload::Event(event)),
            }))
            .await
            .is_err()
        {
            return;
        }
    }

    if tx
        .send(Ok(InternalSubscribeResponse {
            payload: Some(internal_subscribe_response::Payload::CaughtUp(Empty {})),
        }))
        .await
        .is_err()
    {
        return;
    }

    loop {
        match event_rx.recv().await {
            Ok(event) => {
                if (!all_streams && !streams.contains(&event.stream_id))
                    || watermarks
                        .get(&event.stream_id)
                        .is_some_and(|version| event.version <= *version)
                {
                    continue;
                }
                watermarks.insert(event.stream_id.clone(), event.version);
                let event = Event {
                    stream_id: event.stream_id,
                    version: event.version,
                    event_id: event.event_id.as_bytes().to_vec(),
                    event_type: event.event_type,
                    data: event.data,
                    metadata: event.metadata,
                    hlc: Some(Hlc {
                        wall: event.hlc.wall,
                        logical: event.hlc.logical,
                    }),
                    position: event.position,
                    shard_id,
                };
                if tx
                    .send(Ok(InternalSubscribeResponse {
                        payload: Some(internal_subscribe_response::Payload::Event(event)),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => return,
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
        }
    }
}

/// 在一个数据 Shard 上按多个 Stream checkpoint 读取候选事件。
///
/// 返回顺序与 cursor 顺序一致，每个 Stream 内按 version 递增；`max_events=0`
/// 时只读取 head，供 FromNow 初始化使用。
pub(crate) fn read_persistent_local(
    shard_id: u64,
    storage: &es_storage::EsStorage,
    cursors: &[InternalPersistentCursor],
    max_events: u32,
    max_bytes: u64,
) -> Result<InternalPersistentReadResponse, Status> {
    use prost::Message as _;

    let mut heads = Vec::with_capacity(cursors.len());
    let mut events = Vec::new();
    let mut used_bytes = 0u64;
    for cursor in cursors {
        let meta = storage
            .read_stream_meta(&cursor.stream_id)
            .map_err(|error| Status::internal(format!("读取持久化订阅 head 失败: {error}")))?;
        heads.push(InternalPersistentHead {
            stream_id: cursor.stream_id.clone(),
            exists: meta.is_some(),
            current_version: meta.as_ref().map(|item| item.current_version).unwrap_or(0),
        });
        if max_events == 0 || meta.is_none() {
            continue;
        }
        let remaining = max_events.saturating_sub(events.len() as u32);
        if remaining == 0 {
            break;
        }
        let per_stream = if cursor.max_count == 0 {
            remaining
        } else {
            cursor.max_count.min(remaining)
        };
        let read = storage
            .read_stream_events(&cursor.stream_id, cursor.from_version, per_stream as u64)
            .map_err(|error| Status::internal(format!("读取持久化订阅事件失败: {error}")))?;
        for event in read {
            let event = Event {
                stream_id: event.stream_id,
                version: event.version,
                event_id: event.event_id.as_bytes().to_vec(),
                event_type: event.event_type,
                data: event.data,
                metadata: event.metadata,
                hlc: Some(Hlc {
                    wall: event.hlc.wall,
                    logical: event.hlc.logical,
                }),
                position: event.position,
                shard_id,
            };
            let event_bytes = event.encoded_len() as u64;
            if !events.is_empty()
                && max_bytes != 0
                && used_bytes.saturating_add(event_bytes) > max_bytes
            {
                return Ok(InternalPersistentReadResponse { events, heads });
            }
            used_bytes = used_bytes.saturating_add(event_bytes);
            events.push(event);
            if events.len() as u32 >= max_events {
                return Ok(InternalPersistentReadResponse { events, heads });
            }
        }
    }
    Ok(InternalPersistentReadResponse { events, heads })
}

#[tonic::async_trait]
impl EventStore for EsService {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        use prost::Message;

        // 权威校验：proto 请求精确字节数（encoded_len）。仅按 data+metadata
        // 总和近似（见 check_append_limits）堵不住「海量小事件 × 逐事件头」
        // 造成的线缆膨胀，这里以精确编码长度为准——超限直接拒绝，而不是让
        // 请求在传输层被 gRPC 上限拒绝后语义模糊（客户端拿不到可操作的错误）。
        let encoded_len = request.get_ref().encoded_len();
        if encoded_len as u64 > self.limits.max_append_batch_bytes {
            return Err(Status::failed_precondition(format!(
                "append payload too large: request {} bytes exceeds limit {} bytes",
                encoded_len, self.limits.max_append_batch_bytes
            )));
        }

        let req = request.into_inner();
        let stream_id = &req.stream_id;

        tracing::debug!("Append request for stream: {}", stream_id);

        // 逐事件校验：单事件超限直接拒绝。一条 append 批在 raft 里是一条
        // 日志条目，openraft 对单条超限的 AppendEntries 没有拆小路径，
        // 必须从源头拦截（否则复制停滞）。
        for e in &req.events {
            let n = e.data.len() + e.metadata.len();
            if n as u64 > self.limits.max_event_bytes {
                return Err(Status::failed_precondition(format!(
                    "append payload too large: single event data+metadata {} bytes exceeds limit {} bytes",
                    n, self.limits.max_event_bytes
                )));
            }
        }

        // 1. 解析 stream 归属 shard（未知流 = 隐式建流：分配并记录路由表）；
        //    本节点不承载时给出 leader 重定向提示
        let target = self.resolve_write_target(stream_id).await?;
        let shard = self.resolve_write_shard(target.shard_id()).await?;

        // 2. 转换 proto 请求为领域模型
        let expected_version = req
            .expected_version
            .ok_or_else(|| Status::invalid_argument("expected_version is required"))?;
        let expected = proto_to_expected_version(expected_version);

        // event_id 必须是合法 16 字节 UUID：静默替换为随机值会破坏幂等
        // 去重（客户端重试同一请求生成新 id，重复追加）——显式报错
        let mut events = Vec::with_capacity(req.events.len());
        for e in req.events {
            let event_id = uuid::Uuid::from_slice(&e.event_id).map_err(|_| {
                Status::invalid_argument(format!(
                    "event_id 必须是 16 字节 UUID，实际 {} 字节",
                    e.event_id.len()
                ))
            })?;
            events.push(es_core::NewEvent {
                event_id,
                event_type: e.event_type,
                data: e.data,
                metadata: e.metadata,
            });
        }

        // 3. 分配 HLC（leader 在提交前分配，保证所有副本一致）
        let hlc = es_core::Hlc::now();

        let mut es_request = es_storage::EsRequest::AppendOwned {
            stream_id: stream_id.clone(),
            ownership_generation: target.generation(),
            expected_version: expected,
            events,
            hlc,
        };

        // 4. 通过 Raft 提交（client_write 返回 apply 后的响应）
        let mut resp = shard
            .raft
            .client_write(es_request.clone())
            .await
            .map_err(client_write_to_status)?;
        let mut successful_shard_id = shard.id();

        if matches!(resp.data, es_storage::EsResponse::OwnershipFenced { .. }) {
            let refreshed = self
                .ownership
                .recover_fenced(stream_id, target.generation())
                .await
                .map_err(crate::ownership::OwnershipError::into_status)?;
            let refreshed_shard = self.resolve_write_shard(refreshed.shard_id()).await?;
            if let es_storage::EsRequest::AppendOwned {
                ownership_generation,
                ..
            } = &mut es_request
            {
                *ownership_generation = refreshed.generation();
            }
            resp = refreshed_shard
                .raft
                .client_write(es_request)
                .await
                .map_err(client_write_to_status)?;
            successful_shard_id = refreshed_shard.id();
        }

        // 5. 转换响应
        match resp.data {
            es_storage::EsResponse::AppendOk {
                next_expected_version,
                first_position,
                last_position,
            } => Ok(Response::new(AppendResponse {
                shard_id: successful_shard_id,
                next_expected_version,
                first_position,
                last_position,
            })),
            es_storage::EsResponse::OptimisticConflict { actual_version } => {
                Err(Status::failed_precondition(format!(
                    "optimistic conflict: actual_version={}",
                    actual_version
                )))
            }
            es_storage::EsResponse::OwnershipFenced { current_generation } => {
                Err(Status::unavailable(format!(
                    "stream ownership generation advanced to {current_generation}; retry"
                )))
            }
            other => Err(Status::internal(format!("append 返回意外结果: {other:?}"))),
        }
    }

    type ReadStreamStream = ReceiverStream<Result<ReadEventsResponse, Status>>;

    async fn read_stream(
        &self,
        request: Request<ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let req = request.into_inner();
        tracing::debug!("ReadStream request for stream: {}", req.stream_id);

        // 路由到分片（只查不分配——读无副作用）；本节点不承载 → Unavailable 轮换
        let shard_id = self.resolve_read_stream_shard(&req.stream_id).await?;
        let shard = self.resolve_read_shard(shard_id).await?;

        // 读取事件（直接从存储层读，无需走 Raft —— 读本地副本即可）
        let desc = req.direction == Direction::Backward as i32;
        let events = if desc {
            shard.storage.read_stream_events_backward(
                &req.stream_id,
                req.from_version,
                req.max_count,
            )
        } else {
            shard
                .storage
                .read_stream_events(&req.stream_id, req.from_version, req.max_count)
        }
        .map_err(|e| Status::internal(format!("read_stream_events 失败: {e}")))?;

        // 转换为 proto Event
        let proto_events: Vec<Event> = events
            .into_iter()
            .map(|e| Event {
                stream_id: e.stream_id,
                version: e.version,
                event_id: e.event_id.as_bytes().to_vec(),
                event_type: e.event_type,
                data: e.data,
                metadata: e.metadata,
                hlc: Some(Hlc {
                    wall: e.hlc.wall,
                    logical: e.hlc.logical,
                }),
                position: e.position,
                shard_id: shard.id(),
            })
            .collect();

        // 流式返回（简化实现：一次性发送全部）
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let resp = ReadEventsResponse {
                events: proto_events,
                next_positions: Vec::new(), // 单流读不填充，仅 ReadAll 使用
            };
            let _ = tx.send(Ok(resp)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ReadAllStream = ReceiverStream<Result<ReadEventsResponse, Status>>;

    async fn read_all(
        &self,
        request: Request<ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let req = request.into_inner();
        tracing::debug!("ReadAll request for shards: {:?}", req.shard_ids);

        // from_positions 非空时它自带分片列表，此时 shard_ids 允许为空
        if req.shard_ids.is_empty() && req.from_positions.is_empty() {
            return Err(Status::invalid_argument(
                "shard_ids 与 from_positions 不能同时为空",
            ));
        }

        let desc = req.direction == Direction::Backward as i32;

        // 确定要读的分片及各自起点：from_positions 非空时优先，用于翻页
        let cursors: Vec<ShardPosition> = if !req.from_positions.is_empty() {
            req.from_positions
        } else {
            req.shard_ids
                .iter()
                .map(|&s| ShardPosition {
                    shard_id: s,
                    from_position: req.from_position,
                    ended: false,
                })
                .collect()
        };

        // 逐分片读取。每个分片内部已按 position 排序(升序或降序)。
        // 各取 max_count 条即可保证归并出的首 max_count 条正确。
        let per_shard_limit = req.max_count;
        let mut streams: Vec<Vec<Event>> = Vec::with_capacity(cursors.len());
        for cursor in &cursors {
            let shard_id = cursor.shard_id;

            // 反向读尽（ended=true）的分片不再有更早事件，直接给空流；
            // 不能仅靠 from==0 判断——「消费到 position 1 → 游标 0」时
            // position 0 仍未读，from=0 必须能读到它
            let proto_events = if desc && cursor.ended {
                Vec::new()
            } else {
                match self.shard_manager.get_shard(shard_id).await {
                    Ok(shard) => {
                        let events = if desc {
                            shard
                                .storage
                                .read_all_events_backward(cursor.from_position, per_shard_limit)
                                .map_err(|e| {
                                    Status::internal(format!("read_all_events_backward 失败: {e}"))
                                })?
                        } else {
                            shard
                                .storage
                                .read_all_events(cursor.from_position, per_shard_limit)
                                .map_err(|e| {
                                    Status::internal(format!("read_all_events 失败: {e}"))
                                })?
                        };
                        events
                            .into_iter()
                            .map(|e| Event {
                                stream_id: e.stream_id,
                                version: e.version,
                                event_id: e.event_id.as_bytes().to_vec(),
                                event_type: e.event_type,
                                data: e.data,
                                metadata: e.metadata,
                                hlc: Some(Hlc {
                                    wall: e.hlc.wall,
                                    logical: e.hlc.logical,
                                }),
                                position: e.position,
                                shard_id,
                            })
                            .collect()
                    }
                    Err(_) => {
                        self.remote
                            .read_all_shard(cursor, per_shard_limit, req.direction)
                            .await?
                    }
                }
            };

            streams.push(proto_events);
        }

        let (proto_events, consumed) = merge_by_hlc(streams, req.max_count, desc);

        // 每分片「下一页的续读起点」= 归并消费水位推进，而非读水位：
        // 读到但被全局截断丢弃的缓冲尾部必须留在游标之后（下一页重读），
        // 否则数据永久丢失。服务端驱动游标，客户端翻页原样透传。
        // 正序 = 最后消费 position + 1；倒序 = 最后消费 position - 1；
        // 倒序消费到 position 0 时置 ended=true（该分片已读尽，服务端对
        // 它返回空页且游标不变）——空页是正反两个方向的统一终止条件。
        // 未消费的路 = 起点不变（含 ended 状态）。
        let mut next_positions: Vec<ShardPosition> = Vec::with_capacity(cursors.len());
        for (cursor, consumed_pos) in cursors.iter().zip(consumed.iter()) {
            // 每分片恒得一条游标（正反向统一，含读尽标记），客户端翻页原样透传
            let next = match (desc, consumed_pos) {
                (false, Some(p)) => ShardPosition {
                    shard_id: cursor.shard_id,
                    from_position: p.saturating_add(1),
                    ended: false,
                },
                (true, Some(p)) => ShardPosition {
                    shard_id: cursor.shard_id,
                    from_position: p.saturating_sub(1),
                    ended: *p == 0,
                },
                (_, None) => ShardPosition {
                    shard_id: cursor.shard_id,
                    from_position: cursor.from_position,
                    ended: cursor.ended, // 未消费：起点不变，下一页重读
                },
            };
            next_positions.push(next);
        }

        // 流式返回
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let resp = ReadEventsResponse {
                events: proto_events,
                next_positions,
            };
            let _ = tx.send(Ok(resp)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeStream = ReceiverStream<Result<SubscribeResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let target = request
            .into_inner()
            .target
            .ok_or_else(|| Status::invalid_argument("target is required"))?;

        let mut groups: BTreeMap<u64, Vec<String>> = BTreeMap::new();
        match target {
            subscribe_request::Target::Streams(streams) => {
                if streams.stream_ids.is_empty() {
                    return Err(Status::invalid_argument("stream_ids cannot be empty"));
                }
                for stream_id in streams.stream_ids.into_iter().collect::<BTreeSet<_>>() {
                    let shard_id = self.resolve_read_stream_shard(&stream_id).await?;
                    groups.entry(shard_id).or_default().push(stream_id);
                }
            }
            subscribe_request::Target::All(_) => {
                for shard_id in &self.all_shards {
                    groups.insert(*shard_id, Vec::new());
                }
            }
        }

        let (public_tx, public_rx) = tokio::sync::mpsc::channel(100);
        let (source_tx, mut source_rx) = tokio::sync::mpsc::channel(100);
        let mut pending_catch_up: BTreeSet<u64> = groups.keys().copied().collect();

        for (shard_id, stream_ids) in groups {
            let source_tx = source_tx.clone();
            let local_shard = self
                .shard_manager
                .get_shard(shard_id)
                .await
                .ok()
                .filter(|shard| shard.raft.metrics().borrow().state.is_leader());
            let remote = self.remote.clone();
            tokio::spawn(async move {
                let (child_tx, mut child_rx) = tokio::sync::mpsc::channel(100);
                if let Some(shard) = local_shard {
                    tokio::spawn(run_local_subscription(
                        shard_id,
                        shard.storage.clone(),
                        stream_ids,
                        child_tx,
                    ));
                    while let Some(item) = child_rx.recv().await {
                        match item {
                            Ok(InternalSubscribeResponse {
                                payload: Some(internal_subscribe_response::Payload::Event(event)),
                            }) => {
                                if source_tx.send(SourceMessage::Event(event)).await.is_err() {
                                    return;
                                }
                            }
                            Ok(InternalSubscribeResponse {
                                payload: Some(internal_subscribe_response::Payload::CaughtUp(_)),
                            }) => {
                                if source_tx
                                    .send(SourceMessage::CaughtUp(shard_id))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            _ => break,
                        }
                    }
                } else {
                    let request = InternalSubscribeRequest {
                        shard_id,
                        stream_ids,
                    };
                    match remote.internal_client(shard_id).await {
                        Ok(mut client) => match client.subscribe_internal(request).await {
                            Ok(response) => {
                                let mut stream = response.into_inner();
                                while let Ok(Some(item)) = stream.message().await {
                                    match item.payload {
                                        Some(internal_subscribe_response::Payload::Event(
                                            event,
                                        )) => {
                                            if source_tx
                                                .send(SourceMessage::Event(event))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                        }
                                        Some(internal_subscribe_response::Payload::CaughtUp(_)) => {
                                            if source_tx
                                                .send(SourceMessage::CaughtUp(shard_id))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                        }
                                        None => break,
                                    }
                                }
                            }
                            Err(_) => {}
                        },
                        Err(_) => {}
                    }
                }

                // 任一内部来源结束都可能造成后续事件缺口，必须显式降级。
                let _ = source_tx.send(SourceMessage::Degraded(shard_id)).await;
            });
        }
        drop(source_tx);

        tokio::spawn(async move {
            let mut caught_up = false;
            let mut degraded = false;
            while let Some(message) = source_rx.recv().await {
                let Some(response) = aggregate_source_message(
                    message,
                    &mut pending_catch_up,
                    &mut caught_up,
                    &mut degraded,
                ) else {
                    continue;
                };
                if public_tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(public_rx)))
    }

    async fn get_stream_meta(
        &self,
        request: Request<GetStreamMetaRequest>,
    ) -> Result<Response<GetStreamMetaResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("GetStreamMeta request for stream: {}", req.stream_id);

        // 路由表只查不分配：未知流（未创建/路由表缺失）直接返回 exists=false
        let shard_id = match self.route_table.lookup(&req.stream_id).await {
            Some(s) => s,
            None => {
                return Ok(Response::new(GetStreamMetaResponse {
                    shard_id: 0,
                    exists: false,
                    current_version: 0,
                }));
            }
        };
        // 本节点不承载 → Unavailable，客户端轮换其它节点
        let shard = self.resolve_read_shard(shard_id).await?;

        let meta = shard
            .storage
            .read_stream_meta(&req.stream_id)
            .map_err(|e| Status::internal(format!("read_stream_meta failed: {}", e)))?;

        match meta {
            Some(m) => Ok(Response::new(GetStreamMetaResponse {
                shard_id: shard.id(),
                exists: true,
                current_version: m.current_version,
            })),
            None => Ok(Response::new(GetStreamMetaResponse {
                shard_id: shard.id(),
                exists: false,
                current_version: 0,
            })),
        }
    }

    /// 显式创建流：服务端分配 shard（大致最少流）并记录路由表。
    ///
    /// 幂等：流已存在时返回现有归属（exists=true），不重复分配。
    /// 返回目标 shard 的 leader 地址（探测尽力而为，未知返回空串，
    /// 调用方经常规重定向路径定位亦可）。
    async fn create_stream(
        &self,
        request: Request<CreateStreamRequest>,
    ) -> Result<Response<CreateStreamResponse>, Status> {
        let req = request.into_inner();
        if req.stream_id.is_empty() {
            return Err(Status::invalid_argument("stream_id 不能为空"));
        }
        let target = self.resolve_write_target(&req.stream_id).await?;
        let shard_id = target.shard_id();

        // 尽力探测 leader（仅提示用；失败不阻塞创建）
        let leader_addr = self
            .remote
            .find_leader(shard_id)
            .await
            .map(|(_, addr)| addr)
            .unwrap_or_default();

        Ok(Response::new(CreateStreamResponse {
            shard_id,
            leader_addr,
            exists: !target.created_now(),
        }))
    }
}

#[tonic::async_trait]
impl InternalSubscription for EsService {
    type SubscribeInternalStream = ReceiverStream<Result<InternalSubscribeResponse, Status>>;

    async fn subscribe_internal(
        &self,
        request: Request<InternalSubscribeRequest>,
    ) -> Result<Response<Self::SubscribeInternalStream>, Status> {
        let request = request.into_inner();
        let shard = self.resolve_read_shard(request.shard_id).await?;
        if !shard.raft.metrics().borrow().state.is_leader() {
            return Err(Status::unavailable(
                "internal subscription source unavailable",
            ));
        }
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(run_local_subscription(
            request.shard_id,
            shard.storage.clone(),
            request.stream_ids,
            tx,
        ));
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn read_persistent_batch(
        &self,
        request: Request<InternalPersistentReadRequest>,
    ) -> Result<Response<InternalPersistentReadResponse>, Status> {
        let request = request.into_inner();
        let shard = self.resolve_read_shard(request.shard_id).await?;
        if !shard.raft.metrics().borrow().state.is_leader() {
            return Err(Status::unavailable(
                "persistent subscription source is not leader",
            ));
        }
        if request.max_events > es_core::persistent::MAX_FETCH_EVENTS {
            return Err(Status::invalid_argument("max_events exceeds 1000"));
        }
        if request.max_bytes > es_core::persistent::MAX_FETCH_BYTES {
            return Err(Status::invalid_argument("max_bytes exceeds 7 MiB"));
        }
        let response = read_persistent_local(
            request.shard_id,
            &shard.storage,
            &request.cursors,
            request.max_events,
            request.max_bytes,
        )?;
        Ok(Response::new(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn test_config(data_dir: std::path::PathBuf, node_id: u64) -> Config {
        Config {
            node: crate::config::NodeConfig {
                id: node_id,
                listen_addr: "127.0.0.1:0".into(),
                internal_listen_addr: None,
                peers: Vec::new(),
            },
            storage: crate::config::StorageConfig {
                data_dir,
                memtable_arena_bytes: 4 * 1024 * 1024,
            },
            placement: crate::config::PlacementConfig {
                replication_factor: 1,
                nodes: vec![crate::config::PlacementNode {
                    id: node_id,
                    primary: vec![0],
                    replica: Vec::new(),
                }],
            },
            snapshot: Default::default(),
            tls: None,
            limits: Default::default(),
        }
    }

    /// 构造事件：hlc 相同（wall, logical）时按 shard_id 再按 position 定序
    fn ev(shard: u64, pos: u64, wall: u64) -> Event {
        Event {
            stream_id: format!("s{shard}"),
            version: pos,
            event_id: Vec::new(),
            event_type: "T".into(),
            data: Vec::new(),
            metadata: Vec::new(),
            hlc: Some(Hlc { wall, logical: 0 }),
            position: pos,
            shard_id: shard,
        }
    }

    /// 消费水位语义：全局截断丢弃的路不推进（None），否则下一页会丢数据
    #[test]
    fn merge_truncated_unconsumed_watermark_none() {
        // 分片 0 的 HLC 最早，limit=2 时两条全来自分片 0；分片 1 被丢弃
        let streams = vec![
            vec![ev(0, 1, 100), ev(0, 2, 200), ev(0, 3, 300)],
            vec![ev(1, 1, 1000)],
        ];
        let (events, consumed) = merge_by_hlc(streams, 2, false);
        assert_eq!(events.len(), 2);
        assert_eq!(consumed, vec![Some(2), None], "分片 1 未消费应为 None");
    }

    /// 正序：消费水位 = 最后消费的 position，续读从 +1 开始
    #[test]
    fn forward_merge_watermark_advances() {
        let streams = vec![
            vec![ev(0, 1, 100), ev(0, 2, 300)],
            vec![ev(1, 1, 200), ev(1, 2, 400)],
        ];
        // 顺序：100(0,1) → 200(1,1) → 300(0,2) → 400(1,2)
        let (events, consumed) = merge_by_hlc(streams, 0, false);
        assert_eq!(events.len(), 4);
        assert_eq!(consumed, vec![Some(2), Some(2)]);
    }

    /// 倒序：消费水位 = 最后消费（最小）position，续读从 -1 开始
    #[test]
    fn backward_merge_watermark_min_position() {
        let streams = vec![
            vec![ev(0, 9, 100), ev(0, 8, 300), ev(0, 7, 500)],
            vec![ev(1, 5, 200)],
        ];
        // 降序：500(0,7) → 300(0,8) → 200(1,5) → 100(0,9)
        let (_, consumed) = merge_by_hlc(streams, 0, true);
        assert_eq!(
            consumed,
            vec![Some(7), Some(5)],
            "倒序水位 = 各路最后输出的最小 position"
        );
    }

    /// 单路：截断后水位 = 最后输出的 position
    #[test]
    fn single_stream_truncated_watermark() {
        let streams = vec![vec![ev(0, 1, 1), ev(0, 2, 1), ev(0, 3, 1)]];
        let (events, consumed) = merge_by_hlc(streams, 2, false);
        assert_eq!(events.len(), 2);
        assert_eq!(consumed, vec![Some(2)]);
    }

    proptest! {
        #[test]
    fn public_subscription_projection_keeps_stream_version_identity(
            stream_id in "[a-z]{1,20}",
            version in any::<u64>(),
            data in prop::collection::vec(any::<u8>(), 0..64),
        ) {
            let event = Event {
                stream_id: stream_id.clone(), version, event_id: vec![1; 16], event_type: "T".into(),
                data: data.clone(), metadata: vec![], hlc: None, position: 99, shard_id: 7,
            };
            let projected = public_subscription_event(event);
            prop_assert_eq!(projected.stream_id, stream_id);
            prop_assert_eq!(projected.version, version);
            prop_assert_eq!(projected.data, data);
        }
    }

    #[test]
    fn aggregate_source_messages_emit_each_state_transition_once() {
        let mut pending = BTreeSet::from([3, 7]);
        let mut caught_up = false;
        let mut degraded = false;

        assert!(matches!(
            aggregate_source_message(
                SourceMessage::Event(ev(3, 4, 9)),
                &mut pending,
                &mut caught_up,
                &mut degraded,
            )
            .and_then(|response| response.payload),
            Some(subscribe_response::Payload::Event(event))
                if event.stream_id == "s3" && event.version == 4
        ));
        assert!(
            aggregate_source_message(
                SourceMessage::CaughtUp(3),
                &mut pending,
                &mut caught_up,
                &mut degraded,
            )
            .is_none()
        );
        assert!(matches!(
            aggregate_source_message(
                SourceMessage::CaughtUp(7),
                &mut pending,
                &mut caught_up,
                &mut degraded,
            )
            .and_then(|response| response.payload),
            Some(subscribe_response::Payload::CaughtUp(_))
        ));
        assert!(
            aggregate_source_message(
                SourceMessage::CaughtUp(7),
                &mut pending,
                &mut caught_up,
                &mut degraded,
            )
            .is_none()
        );
        assert!(matches!(
            aggregate_source_message(
                SourceMessage::Degraded(3),
                &mut pending,
                &mut caught_up,
                &mut degraded,
            )
            .and_then(|response| response.payload),
            Some(subscribe_response::Payload::Degraded(_))
        ));
        assert!(
            aggregate_source_message(
                SourceMessage::Degraded(7),
                &mut pending,
                &mut caught_up,
                &mut degraded,
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn leader_probe_skips_self_and_rejects_uninitialized_peer() {
        let dir = tempfile::tempdir().expect("临时目录");
        let server = crate::server::Server::new(test_config(dir.path().to_path_buf(), 2))
            .expect("创建未初始化节点");
        server.init().await.expect("初始化未选主节点");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定管理端口");
        let peer_addr = format!("http://{}", listener.local_addr().expect("读取管理端口"));
        let admin = es_raft::RaftAdminService::new(server.shard_manager().clone());
        let handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(es_proto::eventstore::raft_admin_server::RaftAdminServer::new(admin))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut config = test_config(dir.path().join("remote"), 1);
        config.node.peers = vec![
            crate::config::PeerConfig {
                id: 1,
                addr: "http://127.0.0.1:1".into(),
                internal_addr: None,
            },
            crate::config::PeerConfig {
                id: 2,
                addr: peer_addr,
                internal_addr: None,
            },
        ];
        let remote = RemoteShards::new(&config).expect("构造远程 shard 探测器");
        assert_eq!(
            remote.find_leader(0).await,
            None,
            "自身必须跳过，未选主 peer 不能被误判为 leader"
        );

        handle.abort();
    }

    #[tokio::test]
    async fn local_subscription_exits_when_receiver_is_closed() {
        let dir = tempfile::tempdir().expect("临时目录");
        let server = crate::server::Server::new(test_config(dir.path().to_path_buf(), 1))
            .expect("创建服务器");
        server.init().await.expect("初始化服务器");
        let storage = server
            .shard_manager()
            .get_shard(0)
            .await
            .expect("读取本地分片")
            .storage
            .clone();
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(rx);

        // 下游已断开时，发送 caught_up 必须立即失败并释放任务，避免泄漏订阅循环。
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            run_local_subscription(0, storage, Vec::new(), tx),
        )
        .await
        .expect("下游断开后本地订阅必须退出");
    }
}
