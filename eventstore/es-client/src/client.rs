//! EventStore 客户端。

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio_stream::{Stream, StreamExt};
use tonic::transport::Channel;
use tonic::{Code, Status};

use es_core::{LeaderRetryPlan, parse_leader_hint};
use es_proto::endpoint::normalize_endpoint;
use es_proto::eventstore::event_store_client::EventStoreClient as GrpcClient;
use es_proto::eventstore::*;
use es_proto::tls::{apply_endpoint_tls, TlsClientConfig};

/// 选举中（`leader unknown`）退避间隔，与 es-ctl 一致。
/// 单节点选举最坏情况 append 耗时 ≈ 4 次重试 × 200ms ≈ 800ms。
const ELECTION_RETRY_DELAY: Duration = Duration::from_millis(200);

/// 订阅目标：订阅单个流或全部分片（对应 proto `SubscribeRequest.target` oneof）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeTarget {
    /// 订阅单个流
    Stream(String),
    /// 订阅全部分片（`shard_id` 指定分片，默认 0）
    All { shard_id: u64 },
}

/// 订阅响应流：逐条投递 `SubscribeResponse`（事件或 `caught_up` 分界信号）。
///
/// 断线/服务端关流时错误上抛，**不自动重订阅**（调用方决定是否以已读
/// 位置重新发起订阅）。
pub type SubscribeStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeResponse, ClientError>> + Send>>;

/// EventStore 客户端
///
/// 提供连接管理、分片路由与 leader 重定向。
#[derive(Debug)]
pub struct EventStoreClient {
    /// 节点地址 -> gRPC 客户端映射
    clients: HashMap<String, GrpcClient<Channel>>,

    /// 默认节点地址列表
    nodes: Vec<String>,

    /// https 节点的信任策略；None = 默认跳过校验（自签友好）
    tls: Option<TlsClientConfig>,

    /// 轮询起点游标：读方法轮换节点时起点后移，负载分散
    cursor: AtomicUsize,
}

impl EventStoreClient {
    /// 连接到 EventStore 集群。
    ///
    /// http 节点走明文；https 节点默认跳过证书校验（自签友好）。
    /// 需要严格校验时用 [`Self::connect_with_tls`]。
    ///
    /// # 参数
    /// - `nodes`: 集群节点地址列表，如 `vec!["http://127.0.0.1:50051"]`
    pub async fn connect(nodes: Vec<String>) -> Result<Self, ClientError> {
        Self::connect_with_tls(nodes, None).await
    }

    /// 连接到 EventStore 集群并指定 https 节点的信任策略。
    ///
    /// - `tls`: `Some(Ca(pem))` 严格校验对端证书链；`None` 或
    ///   `Some(SkipVerify)` 跳过校验。http 节点不受影响。
    pub async fn connect_with_tls(
        nodes: Vec<String>,
        tls: Option<TlsClientConfig>,
    ) -> Result<Self, ClientError> {
        if nodes.is_empty() {
            return Err(ClientError::InvalidConfig(
                "nodes list cannot be empty".to_string(),
            ));
        }

        let mut clients = HashMap::new();

        // 连接到第一个节点（懒加载其他节点）
        let first_node = &nodes[0];
        let client = Self::connect_one(first_node, tls.as_ref()).await?;

        clients.insert(first_node.clone(), client);

        Ok(Self {
            clients,
            nodes,
            tls,
            cursor: AtomicUsize::new(0),
        })
    }

    /// 构建到单个节点的连接（显式 Endpoint 构建，https 按信任策略装配 TLS）。
    async fn connect_one(
        addr: &str,
        tls: Option<&TlsClientConfig>,
    ) -> Result<GrpcClient<Channel>, ClientError> {
        let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
            .map_err(|e| ClientError::InvalidConfig(format!("非法节点地址 {addr}: {e}")))?;
        let endpoint = apply_endpoint_tls(endpoint, tls)
            .map_err(|e| ClientError::InvalidConfig(format!("节点 {addr} TLS 配置失败: {e}")))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| ClientError::ConnectionFailed(e.to_string()))?;
        Ok(GrpcClient::new(channel))
    }

    /// 获取或创建到指定节点的连接
    async fn get_or_connect(&mut self, addr: &str) -> Result<GrpcClient<Channel>, ClientError> {
        if let Some(client) = self.clients.get(addr) {
            return Ok(client.clone());
        }

        let client = Self::connect_one(addr, self.tls.as_ref()).await?;

        self.clients.insert(addr.to_string(), client.clone());
        Ok(client)
    }

    /// 从轮询游标开始的节点顺序（每次调用起点后移一位，负载分散）。
    fn rotated_nodes(&self) -> Vec<String> {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % self.nodes.len();
        (0..self.nodes.len())
            .map(|i| self.nodes[(start + i) % self.nodes.len()].clone())
            .collect()
    }

    /// 读操作：在任一可达节点执行。
    ///
    /// 依序尝试全部节点（轮询起点后移），建连失败与 RPC 错误都换下一节点
    /// （读走本地存储，follower 也服务，天然幂等故不可重试）；全部失败时
    /// 汇总为 [`ClientError::AllNodesFailed`]。
    async fn with_any_node<T, F, Fut>(&mut self, f: F) -> Result<T, ClientError>
    where
        F: Fn(GrpcClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let mut errors: Vec<String> = Vec::new();
        for node in self.rotated_nodes() {
            let client = match self.get_or_connect(&node).await {
                Ok(c) => c,
                // 建连失败：本节点不可用，继续尝试下一个（故障转移）
                Err(e) => {
                    errors.push(format!("{node}: 连接失败: {e}"));
                    continue;
                }
            };
            match f(client).await {
                Ok(v) => return Ok(v),
                Err(status) => errors.push(format!("{node}: {}", status.message())),
            }
        }
        Err(ClientError::AllNodesFailed(format!(
            "所有节点均不可用（{}）：{}",
            self.nodes.join(", "),
            errors.join("；")
        )))
    }

    /// 追加事件到流。
    ///
    /// 写路径自动打 leader：命中 `Unavailable` 且消息带 `leader_addr` 时
    /// 重定向到该地址；选举中（`leader unknown`）退避后重试；`FailedPrecondition`
    ///（乐观冲突）等不可重试错误原样上抛。
    ///
    /// # 参数
    /// - `stream_id`: 流 ID
    /// - `expected_version`: 期望版本
    /// - `events`: 待追加事件列表
    ///
    /// # 错误
    /// - 重试预算耗尽（集群可能长期处于选举中）→ [`ClientError::NotLeader`]
    pub async fn append(
        &mut self,
        stream_id: String,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent>,
    ) -> Result<AppendResponse, ClientError> {
        let request = AppendRequest {
            stream_id,
            expected_version: Some(expected_version),
            events,
        };

        // 重定向地址可能不在初始节点列表，预算 = 节点数 × 2 + 2
        let mut plan = LeaderRetryPlan::new(self.rotated_nodes());
        let mut last_redirect: Option<String> = None;

        while let Some(target) = plan.next() {
            // 建连失败：本节点不可用，继续试下一个（不重入队，避免空转）
            let Ok(client) = self.get_or_connect(&target).await else {
                continue;
            };
            match client.clone().append(request.clone()).await {
                Ok(resp) => return Ok(resp.into_inner()),
                Err(status) if status.code() == Code::Unavailable => {
                    match parse_leader_hint(status.message()) {
                        Some(addr) => {
                            let norm = normalize_endpoint(&addr);
                            last_redirect = Some(norm.clone());
                            // 重定向地址优先尝试；即使已试过也可能正处选举中，
                            // 由 plan 的去重兜底，重试有界（预算 2N+2）
                            plan.redirect_to(norm);
                        }
                        // 提示缺失（选举中 leader unknown，或 leader_addr 为空）：
                        // 本节点稍后重试，但先把队列里其它节点试完，避免死等。
                        None => {
                            plan.retry_later(target);
                            tokio::time::sleep(ELECTION_RETRY_DELAY).await;
                        }
                    }
                }
                Err(status) => {
                    // FailedPrecondition（乐观冲突）等不可重试错误原样上抛
                    return Err(ClientError::RpcFailed(status.message().to_string()));
                }
            }
        }
        Err(ClientError::NotLeader(last_redirect))
    }

    /// 读取流事件。
    ///
    /// 节点连接失败时轮换到下一节点；流建立后的中途错误原样上抛。
    ///
    /// # 参数
    /// - `stream_id`: 流 ID
    /// - `from_version`: 起始版本（含）
    /// - `max_count`: 最大条数
    /// - `direction`: 读取方向
    pub async fn read_stream(
        &mut self,
        stream_id: String,
        from_version: u64,
        max_count: u64,
        direction: Direction,
    ) -> Result<Vec<Event>, ClientError> {
        self.with_any_node(|mut client| {
            let request = ReadStreamRequest {
                stream_id: stream_id.clone(),
                from_version,
                max_count,
                direction: direction as i32,
            };
            async move {
                let mut stream = client.read_stream(request).await?.into_inner();
                let mut events = Vec::new();
                while let Some(response) = stream.message().await? {
                    events.extend(response.events);
                }
                Ok(events)
            }
        })
        .await
    }

    /// 读取全部分片（`$all`）。
    ///
    /// 返回本页事件与逐分片续读位置（`next_positions`）：翻页时把返回的
    /// `next_positions` **原样透传**为下页的 `from_positions`，不要自行构造。
    /// 反向读时 `from_position` 传 `u64::MAX` 哨兵。
    ///
    /// # 翻页终止
    /// 服务端对页内无事件（读尽）的分片仍返回起点不变的非空游标（未消费
    /// 的路下一页重读），因此**以空页为终止条件**，不能以游标为空判断。
    ///
    /// # 参数
    /// - `shard_ids`: 目标分片列表（`from_positions` 非空时被覆盖）
    /// - `from_position`: 全部分片统一的起始位置，仅适合首页
    /// - `max_count`: 最大条数
    /// - `direction`: 读取方向
    /// - `from_positions`: 逐分片游标，非空时覆盖 `shard_ids` 与 `from_position`
    pub async fn read_all(
        &mut self,
        shard_ids: Vec<u64>,
        from_position: u64,
        max_count: u64,
        direction: Direction,
        from_positions: Vec<ShardPosition>,
    ) -> Result<(Vec<Event>, Vec<ShardPosition>), ClientError> {
        self.with_any_node(|mut client| {
            let request = ReadAllRequest {
                shard_ids: shard_ids.clone(),
                from_position,
                max_count,
                direction: direction as i32,
                from_positions: from_positions.clone(),
            };
            async move {
                let mut stream = client.read_all(request).await?.into_inner();
                let mut events = Vec::new();
                let mut next_positions = Vec::new();
                while let Some(response) = stream.message().await? {
                    events.extend(response.events);
                    // 与 es-ctl collect_page 同语义：非空才更新，避免空页清掉游标
                    if !response.next_positions.is_empty() {
                        next_positions = response.next_positions;
                    }
                }
                Ok((events, next_positions))
            }
        })
        .await
    }

    /// 订阅事件流：catch-up 历史事件后转实时推送（`caught_up` 信号分界）。
    ///
    /// 建立流阶段节点连接失败时轮换到下一节点；流建立后断线**不自动重订阅**，
    /// 错误在流上以 `Err` 投递，由调用方决定是否以已读位置重新发起订阅。
    ///
    /// # 参数
    /// - `target`: 订阅单个流或全部分片
    /// - `from_exclusive`: 订阅流时按 version、订阅 all 时按 position（不含起点）
    /// - `from_start`: true 从头开始，忽略 `from_exclusive`
    pub async fn subscribe(
        &mut self,
        target: SubscribeTarget,
        from_exclusive: u64,
        from_start: bool,
    ) -> Result<SubscribeStream, ClientError> {
        let request = match &target {
            SubscribeTarget::Stream(stream_id) => SubscribeRequest {
                target: Some(subscribe_request::Target::StreamId(stream_id.clone())),
                from_exclusive,
                from_start,
                shard_id: 0,
            },
            SubscribeTarget::All { shard_id } => SubscribeRequest {
                target: Some(subscribe_request::Target::All(Empty {})),
                from_exclusive,
                from_start,
                shard_id: *shard_id,
            },
        };

        self.with_any_node(|mut client| {
            let request = request.clone();
            async move {
                let stream = client.subscribe(request).await?.into_inner();
                // 流内错误映射为 ClientError 上抛（不重订阅）
                let mapped = stream.map(|item| {
                    item.map_err(|e| ClientError::RpcFailed(e.to_string()))
                });
                Ok(Box::pin(mapped) as SubscribeStream)
            }
        })
        .await
    }

    /// 查询流元数据（是否存在、当前版本、所在分片）。
    ///
    /// 节点连接失败时轮换到下一节点。
    pub async fn get_stream_meta(
        &mut self,
        stream_id: String,
    ) -> Result<GetStreamMetaResponse, ClientError> {
        self.with_any_node(|mut client| {
            let request = GetStreamMetaRequest {
                stream_id: stream_id.clone(),
            };
            async move { Ok(client.get_stream_meta(request).await?.into_inner()) }
        })
        .await
    }
}

/// 客户端错误
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("RPC failed: {0}")]
    RpcFailed(String),

    /// append 重试预算耗尽仍未成功（集群可能长期处于选举中），
    /// 附最近一次收到的重定向地址
    #[error("Not leader, redirect to: {0:?}")]
    NotLeader(Option<String>),

    /// 读方法轮换全部节点后仍失败，附各节点错误详情
    #[error("All nodes failed: {0}")]
    AllNodesFailed(String),
}
