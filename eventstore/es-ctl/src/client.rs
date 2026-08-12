//! 集群连接管理：端点归一化、TLS 装配、惰性通道缓存、leader 发现与重定向。

use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

use es_core::{LeaderRetryPlan, parse_leader_hint};
use es_core::route::RouteTable;
use es_proto::endpoint::normalize_endpoint;
use es_proto::eventstore::event_store_client::EventStoreClient;
use es_proto::eventstore::migration_client::MigrationClient;
use es_proto::eventstore::raft_admin_client::RaftAdminClient;
use es_proto::eventstore::{
    CreateStreamRequest, CreateStreamResponse, GetRaftStateRequest, GetRaftStateResponse,
    GetRouteTableRequest, ListShardsRequest, ListShardsResponse, RecountStreamsRequest,
};
use es_proto::tls::{TlsClientConfig, apply_endpoint_tls};

/// 集群客户端。
///
/// 通道按端点惰性建立并缓存（克隆廉价）；写操作走 leader 发现，
/// 读操作任一可达端点即可（服务端读走本地存储，follower 也服务读）。
pub struct ClusterClient {
    /// 归一化（补 http:// 前缀）且去重保序的端点列表
    endpoints: Vec<String>,
    /// https 端点的信任策略；http 端点不受影响
    tls: Option<TlsClientConfig>,
    dial_timeout: Duration,
    request_timeout: Duration,
    /// endpoint -> 惰性通道（首次 RPC 时才真正建连）
    channels: RwLock<HashMap<String, Channel>>,
    /// 轮询起点游标：逐次调用后移一位，避免每次都先打第一个端点
    cursor: AtomicUsize,
}

impl ClusterClient {
    pub fn new(
        endpoints: &[String],
        tls: Option<TlsClientConfig>,
        dial_timeout: Duration,
        request_timeout: Duration,
    ) -> Result<Self, anyhow::Error> {
        if endpoints.is_empty() {
            bail!("--endpoints 不能为空");
        }
        let mut seen = HashSet::new();
        let mut normalized = Vec::new();
        for ep in endpoints {
            let ep = normalize_endpoint(ep);
            if seen.insert(ep.clone()) {
                normalized.push(ep);
            }
        }
        Ok(Self {
            endpoints: normalized,
            tls,
            dial_timeout,
            request_timeout,
            channels: RwLock::new(HashMap::new()),
            cursor: AtomicUsize::new(0),
        })
    }

    /// 归一化后的端点列表（只读）
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// 从轮询游标开始的端点顺序（每次调用起点后移一位，负载分散）。
    /// pub(crate)：init 等命令需按此顺序逐端点尝试。
    pub(crate) fn rotated_endpoints(&self) -> Vec<String> {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
        (0..self.endpoints.len())
            .map(|i| self.endpoints[(start + i) % self.endpoints.len()].clone())
            .collect()
    }

    /// 取单个端点（订阅等长连接场景：单端点直连，起点轮询分散）
    pub fn pick_endpoint(&self) -> String {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
        self.endpoints[start].clone()
    }

    /// 获取（或建立）到指定端点的通道。
    ///
    /// 显式 `connect()`（含 TLS 握手）而非 `connect_lazy()`：lazy 模式下
    /// TLS 握手在 hyper 连接池层进行，不受 tower `timeout` 覆盖，握手挂起时
    /// RPC 会无限等待；显式建连由 `connect_timeout` 兜底，失败立即返回。
    /// 装配链与 bootstrap 一致：归一化 → 超时 → TLS。
    ///
    /// 超时值 0 = 不设超时：tonic 的 `.timeout()`/`.connect_timeout()` 不接受
    /// 禁用开关，`Duration::ZERO` 会让 GrpcTimeout 的 sleep(0) 首次 poll 即到期，
    /// 所有 RPC 立即失败——只能条件装配。
    async fn channel_for(&self, endpoint: &str) -> Result<Channel, anyhow::Error> {
        if let Some(ch) = self.channels.read().expect("读锁").get(endpoint) {
            return Ok(ch.clone());
        }
        let mut ep = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("非法端点 {endpoint}"))?;
        if !self.dial_timeout.is_zero() {
            ep = ep.connect_timeout(self.dial_timeout);
        }
        if !self.request_timeout.is_zero() {
            ep = ep.timeout(self.request_timeout);
        }
        let ep = apply_endpoint_tls(ep, self.tls.as_ref())
            .map_err(|e| anyhow!("端点 {endpoint} TLS 装配失败: {e}"))?;
        let ch = ep
            .connect()
            .await
            .with_context(|| format!("连接端点 {endpoint} 失败"))?;
        self.channels
            .write()
            .expect("写锁")
            .insert(endpoint.to_string(), ch.clone());
        Ok(ch)
    }

    /// 数据面客户端（EventStore 服务）
    pub async fn event_client(
        &self,
        endpoint: &str,
    ) -> Result<EventStoreClient<Channel>, anyhow::Error> {
        Ok(EventStoreClient::new(self.channel_for(endpoint).await?)
            // 与系统级 8MB 上限对齐：tonic 解码默认 4MB，不设置的话
            // read/read_all 单页响应超 4MB 时（如多条大事件）解码失败
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }

    /// 管理面客户端（RaftAdmin 服务）
    pub async fn admin_client(
        &self,
        endpoint: &str,
    ) -> Result<RaftAdminClient<Channel>, anyhow::Error> {
        Ok(RaftAdminClient::new(self.channel_for(endpoint).await?)
            // 与系统级 8MB 上限对齐（管理面响应虽小，保持契约一致）
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }

    /// Migration 服务客户端（路由表同步，与其它服务同端口）
    pub async fn migration_client(
        &self,
        endpoint: &str,
    ) -> Result<MigrationClient<Channel>, anyhow::Error> {
        Ok(MigrationClient::new(self.channel_for(endpoint).await?)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }

    /// Migration 操作：在任一可达端点执行（语义同 with_any_endpoint）。
    pub async fn with_any_migration_endpoint<T, F, Fut>(
        &self,
        f: F,
    ) -> Result<T, anyhow::Error>
    where
        F: Fn(MigrationClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let mut errors: Vec<String> = Vec::new();
        for ep in self.rotated_endpoints() {
            let client = match self.migration_client(&ep).await {
                Ok(c) => c,
                Err(e) => {
                    errors.push(format!("{ep}: 连接失败: {e:#}"));
                    continue;
                }
            };
            match f(client).await {
                Ok(v) => return Ok(v),
                Err(status) => errors.push(format!("{ep}: {}", status.message())),
            }
        }
        Err(anyhow!(
            "所有端点均不可用（{}）：{}",
            self.endpoints.join(", "),
            errors.join("；")
        ))
    }

    /// 显式创建流：服务端分配 shard（大致最少流）。任一端点即可——
    /// 分配在本节点路由表上完成并广播收敛，幂等。
    pub async fn create_stream(
        &self,
        stream_id: &str,
    ) -> Result<CreateStreamResponse, anyhow::Error> {
        self.with_any_endpoint(|mut c| async move {
            c.create_stream(CreateStreamRequest {
                stream_id: stream_id.to_string(),
            })
            .await
            .map(|r| r.into_inner())
        })
        .await
    }

    /// 拉取路由表（任一端点；节点重启后从 peer 拉取的同一来源）。
    pub async fn get_route_table(&self) -> Result<RouteTable, anyhow::Error> {
        self.with_any_migration_endpoint(|mut c| async move {
            let resp = c
                .get_route_table(GetRouteTableRequest {})
                .await
                .map(|r| r.into_inner())?;
            Ok(proto_table_to_core(resp.table))
        })
        .await
    }

    /// 校准 per-shard 流计数，返回校准后的路由表。
    pub async fn recount_streams(&self) -> Result<RouteTable, anyhow::Error> {
        self.with_any_migration_endpoint(|mut c| async move {
            let resp = c
                .recount_streams(RecountStreamsRequest {})
                .await
                .map(|r| r.into_inner())?;
            Ok(proto_table_to_core(resp.table))
        })
        .await
    }

    /// 调单个端点的 GetRaftState（NotFound 等状态原样返回）。
    ///
    /// pub(crate)：命令层（member list / status / 分片探测）直接复用。
    pub(crate) async fn get_raft_state_via(
        &self,
        endpoint: &str,
        shard_id: u64,
    ) -> Result<GetRaftStateResponse, Status> {
        let mut client = self
            .admin_client(endpoint)
            .await
            .map_err(|e| Status::internal(format!("连接失败: {e}")))?;
        let resp = client
            .get_raft_state(GetRaftStateRequest { shard_id })
            .await?
            .into_inner();
        Ok(resp)
    }

    /// 定位 shard 的 leader 端点（GetRaftState 探测全部端点，is_leader 命中即返回）。
    ///
    /// 迁移工具用：写/读统一走 shard leader（leader 必承载该 shard）。
    /// 返回已 normalize 的端点地址；无 leader（选举中/集群未组建）返回 None。
    pub async fn find_shard_leader(&self, shard_id: u64) -> Option<String> {
        for ep in self.endpoints() {
            if let Ok(r) = self.get_raft_state_via(ep, shard_id).await {
                if r.is_leader {
                    return Some(ep.clone());
                }
            }
        }
        None
    }

    /// 调单个端点的 ListShards（返回该节点承载的分片；NotFound 等状态原样返回）。
    ///
    /// 分片探测用：各节点只承载放置表分配的子集，集群全部分片 = 全部端点并集。
    pub(crate) async fn list_shards_via(
        &self,
        endpoint: &str,
    ) -> Result<ListShardsResponse, Status> {
        let mut client = self
            .admin_client(endpoint)
            .await
            .map_err(|e| Status::internal(format!("连接失败: {e}")))?;
        let resp = client.list_shards(ListShardsRequest {}).await?.into_inner();
        Ok(resp)
    }
    /// 读操作：在任一可达端点执行。
    ///
    /// 依序尝试全部端点（轮询起点后移），第一个成功的返回；全部失败时
    /// 汇总错误（含建连失败）。读操作不可重试（天然幂等），故不解析 leader 提示。
    pub async fn with_any_endpoint<T, F, Fut>(&self, f: F) -> Result<T, anyhow::Error>
    where
        F: Fn(EventStoreClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let mut errors: Vec<String> = Vec::new();
        for ep in self.rotated_endpoints() {
            let client = match self.event_client(&ep).await {
                Ok(c) => c,
                // 建连失败：本端点不可用，继续尝试下一个（故障转移）
                Err(e) => {
                    errors.push(format!("{ep}: 连接失败: {e:#}"));
                    continue;
                }
            };
            match f(client).await {
                Ok(v) => return Ok(v),
                Err(status) => errors.push(format!("{ep}: {}", status.message())),
            }
        }
        Err(anyhow!(
            "所有端点均不可用（{}）：{}",
            self.endpoints.join(", "),
            errors.join("；")
        ))
    }

    /// 数据面写操作：打 leader。
    ///
    /// 组合策略：依序尝试各端点（轮询起点分散负载）→ `Unavailable` 且消息带
    /// `leader_addr` 时优先重定向到该地址 → `leader unknown`（选举中）退避重试 →
    /// `FailedPrecondition`（乐观冲突等）原样上抛。全部尝试完仍失败则报无 leader。
    ///
    /// 队列/预算/去重由 [`LeaderRetryPlan`]（es-core）驱动。
    pub async fn with_leader<T, F, Fut>(&self, shard_id: u64, f: F) -> Result<T, anyhow::Error>
    where
        F: Fn(EventStoreClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        // 重定向地址可能不在初始端点列表，总预算 = 初始端点 × 2 + 2 轮
        let mut plan = LeaderRetryPlan::new(self.rotated_endpoints());
        let mut errors: Vec<String> = Vec::new();

        while let Some(target) = plan.next() {
            let client = match self.event_client(&target).await {
                Ok(c) => c,
                // 建连失败：本端点不可用，继续试下一个（不重入队，避免空转）
                Err(e) => {
                    errors.push(format!("{target}: 连接失败: {e:#}"));
                    continue;
                }
            };
            match f(client).await {
                Ok(v) => return Ok(v),
                Err(status) if status.code() == Code::Unavailable => {
                    match parse_leader_hint(status.message()) {
                        Some(addr) => {
                            // 重定向地址优先；即使已试过也可能正处选举中，
                            // 由 plan 的去重兜底，重试有界（预算 2N+2）
                            plan.redirect_to(normalize_endpoint(&addr));
                        }
                        // 提示缺失（选举中 leader unknown，或 leader_addr 为空）：
                        // 本端点稍后重试，但先把队列里其它端点试完，避免死等。
                        // retry_later 先移出已试集合再入队，否则重试会被去重挡下。
                        None => {
                            plan.retry_later(target);
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
                Err(status) => {
                    // FailedPrecondition（乐观冲突等）及其它不可重试错误
                    // 原样上抛（命令层据此翻译中文提示）
                    return Err(anyhow!(status.message().to_string()));
                }
            }
        }
        Err(anyhow!(
            "未找到分片 {shard_id} 的 leader：所有端点无响应或集群处于选举中（端点: {}{}）",
            self.endpoints.join(", "),
            if errors.is_empty() {
                String::new()
            } else {
                format!("；详情: {}", errors.join("；"))
            }
        ))
    }

    /// 管理面写操作：先找 leader 端点，再在其上执行 f。
    ///
    /// 与数据面不同，管理面错误（admin_service）不带 leader 提示，只能靠
    /// GetRaftState 主动探测：找到 `is_leader` 的端点即为目标。
    /// leader 探测失败（选举中/端点不可达）与 RPC 失败都纳入 3 轮重试；
    /// 只有「分片未初始化」是永久错误，直接返回。
    pub async fn with_admin_leader<T, F, Fut>(
        &self,
        shard_id: u64,
        f: F,
    ) -> Result<T, anyhow::Error>
    where
        F: Fn(RaftAdminClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let mut last_err: Option<String> = None;
        for _ in 0..3 {
            let (leader_ep, _) = match self.try_find_leader(shard_id).await {
                Ok(v) => v,
                Err(LeaderLookupError::NotInitialized) => {
                    // 永久错误：重试无意义（消息与 find_leader 一致，命令层据此翻译）
                    return Err(anyhow!(
                        "分片 {shard_id} 未初始化：所有端点均返回 not_found（请先运行 esctl init）"
                    ));
                }
                Err(LeaderLookupError::NoLeader { detail }) => {
                    last_err = Some(format!("leader 探测失败：{detail}"));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            let client = match self.admin_client(&leader_ep).await {
                Ok(c) => c,
                // 建连失败：瞬态，下一轮重新探测
                Err(e) => {
                    last_err = Some(format!("{leader_ep}: 连接失败: {e:#}"));
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            match f(client).await {
                Ok(v) => return Ok(v),
                Err(status) => {
                    last_err = Some(format!("{leader_ep}: {}", status.message()));
                    // leader 可能已变更或 CAS 冲突，下一轮重新发现并重试
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        Err(anyhow!(
            "分片 {shard_id} 管理操作失败（3 轮重试后）：{}",
            last_err.unwrap_or_default()
        ))
    }

    /// 找某分片的 leader：轮询全部端点取 GetRaftState。
    ///
    /// 返回 (leader 端点, leader 节点 ID)。
    pub async fn find_leader(&self, shard_id: u64) -> Result<(String, u64), anyhow::Error> {
        match self.try_find_leader(shard_id).await {
            Ok(v) => Ok(v),
            Err(LeaderLookupError::NotInitialized) => Err(anyhow!(
                "分片 {shard_id} 未初始化：所有端点均返回 not_found（请先运行 esctl init）"
            )),
            Err(LeaderLookupError::NoLeader { detail }) => Err(anyhow!(
                "分片 {shard_id} 当前无 leader（选举中或全部端点不可达：{detail}）"
            )),
        }
    }

    /// 结构化版本的 find_leader：错误按可重试性分类，供 with_admin_leader 使用。
    async fn try_find_leader(&self, shard_id: u64) -> Result<(String, u64), LeaderLookupError> {
        let mut any_initialized = false;
        let mut errors: Vec<String> = Vec::new();

        for ep in self.rotated_endpoints() {
            match self.get_raft_state_via(&ep, shard_id).await {
                Ok(state) => {
                    any_initialized = true;
                    if state.is_leader {
                        return Ok((ep, state.node_id));
                    }
                }
                Err(status) if status.code() == Code::NotFound => {
                    // 该端点未注册此分片：可能是分片数不一致的节点，继续看其它端点
                }
                Err(status) => errors.push(format!("{ep}: {}", status.message())),
            }
        }

        if !any_initialized {
            Err(LeaderLookupError::NotInitialized)
        } else {
            Err(LeaderLookupError::NoLeader {
                detail: if errors.is_empty() {
                    "无错误".into()
                } else {
                    errors.join("；")
                },
            })
        }
    }
}

/// leader 探测失败的原因：区分永久错误与瞬态错误，调用方决定是否重试。
enum LeaderLookupError {
    /// 分片未初始化：所有端点均 not_found，重试无意义
    NotInitialized,
    /// 选举中或全部端点不可达：瞬态，可重试
    NoLeader { detail: String },
}

/// proto RouteTable → 领域模型（table 缺失视为空表）
fn proto_table_to_core(t: Option<es_proto::eventstore::RouteTable>) -> RouteTable {
    match t {
        Some(t) => RouteTable {
            version: t.version,
            streams: t.streams.into_iter().collect(),
            shard_stream_counts: t.shard_stream_counts.into_iter().collect(),
        },
        None => RouteTable::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints_normalized_deduped_ordered() {
        let client = ClusterClient::new(
            &[
                "127.0.0.1:50051".into(),
                "http://127.0.0.1:50051".into(),
                "https://n:50052".into(),
            ],
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect("创建客户端");
        assert_eq!(
            client.endpoints(),
            &[
                "http://127.0.0.1:50051".to_string(),
                "https://n:50052".to_string()
            ]
        );
    }

    #[test]
    fn empty_endpoints_rejected() {
        assert!(
            ClusterClient::new(&[], None, Duration::from_secs(1), Duration::from_secs(1)).is_err()
        );
    }

    #[test]
    fn rotated_cursor_advances() {
        let client = ClusterClient::new(
            &["a:1".into(), "b:2".into()],
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect("创建客户端");
        assert_eq!(client.rotated_endpoints(), vec!["http://a:1", "http://b:2"]);
        assert_eq!(client.rotated_endpoints(), vec!["http://b:2", "http://a:1"]);
        assert_eq!(client.rotated_endpoints(), vec!["http://a:1", "http://b:2"]);
    }
}
