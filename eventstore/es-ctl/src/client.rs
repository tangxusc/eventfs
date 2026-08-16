//! 集群管理连接：端点、TLS、Raft leader 发现与管理写重试。

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use es_proto::endpoint::normalize_endpoint;
use es_proto::eventstore::raft_admin_client::RaftAdminClient;
use es_proto::eventstore::{
    GetRaftStateRequest, GetRaftStateResponse, ListShardsRequest, ListShardsResponse,
};
use es_proto::tls::{TlsClientConfig, apply_endpoint_tls};
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};

/// esctl 使用的集群管理客户端。
pub struct ClusterClient {
    endpoints: Vec<String>,
    tls: Option<TlsClientConfig>,
    dial_timeout: Duration,
    request_timeout: Duration,
    channels: RwLock<HashMap<String, Channel>>,
    cursor: AtomicUsize,
}

impl ClusterClient {
    /// 创建客户端并归一化、去重端点。
    ///
    /// `endpoints` 为空或地址格式非法时返回错误；通道在首次使用时建立。
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
        let endpoints = endpoints
            .iter()
            .map(|endpoint| normalize_endpoint(endpoint))
            .filter(|endpoint| seen.insert(endpoint.clone()))
            .collect();
        Ok(Self {
            endpoints,
            tls,
            dial_timeout,
            request_timeout,
            channels: RwLock::new(HashMap::new()),
            cursor: AtomicUsize::new(0),
        })
    }

    /// 返回归一化后的端点列表。
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// 返回 AggregateStore SDK 可复用的 TLS 信任策略。
    pub fn tls(&self) -> Option<&TlsClientConfig> {
        self.tls.as_ref()
    }

    /// 从轮询游标开始返回全部端点，每次调用推进起点。
    pub(crate) fn rotated_endpoints(&self) -> Vec<String> {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
        (0..self.endpoints.len())
            .map(|offset| self.endpoints[(start + offset) % self.endpoints.len()].clone())
            .collect()
    }

    async fn channel_for(&self, endpoint: &str) -> Result<Channel, anyhow::Error> {
        if let Some(channel) = self.channels.read().expect("读锁").get(endpoint) {
            return Ok(channel.clone());
        }
        let mut config = Endpoint::from_shared(endpoint.to_string())
            .with_context(|| format!("非法端点 {endpoint}"))?;
        if !self.dial_timeout.is_zero() {
            config = config.connect_timeout(self.dial_timeout);
        }
        if !self.request_timeout.is_zero() {
            config = config.timeout(self.request_timeout);
        }
        let config = apply_endpoint_tls(config, self.tls.as_ref())
            .map_err(|error| anyhow!("端点 {endpoint} TLS 装配失败: {error}"))?;
        let channel = config
            .connect()
            .await
            .with_context(|| format!("连接端点 {endpoint} 失败"))?;
        self.channels
            .write()
            .expect("写锁")
            .insert(endpoint.to_string(), channel.clone());
        Ok(channel)
    }

    /// 获取指定端点的 RaftAdmin 客户端。
    ///
    /// 建连或 TLS 失败时返回错误；客户端配置系统统一的消息大小上限。
    pub async fn admin_client(
        &self,
        endpoint: &str,
    ) -> Result<RaftAdminClient<Channel>, anyhow::Error> {
        Ok(RaftAdminClient::new(self.channel_for(endpoint).await?)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }

    /// 在指定端点查询 Shard Raft 状态，保留服务端 gRPC 错误。
    pub(crate) async fn get_raft_state_via(
        &self,
        endpoint: &str,
        shard_id: u64,
    ) -> Result<GetRaftStateResponse, Status> {
        let mut client = self
            .admin_client(endpoint)
            .await
            .map_err(|error| Status::internal(format!("连接失败: {error}")))?;
        Ok(client
            .get_raft_state(GetRaftStateRequest { shard_id })
            .await?
            .into_inner())
    }

    /// 在指定端点枚举本节点承载的 Shard，保留服务端 gRPC 错误。
    pub(crate) async fn list_shards_via(
        &self,
        endpoint: &str,
    ) -> Result<ListShardsResponse, Status> {
        let mut client = self
            .admin_client(endpoint)
            .await
            .map_err(|error| Status::internal(format!("连接失败: {error}")))?;
        Ok(client.list_shards(ListShardsRequest {}).await?.into_inner())
    }

    /// 定位 Shard leader，返回端点与节点 ID。
    ///
    /// 全部端点未初始化或无 leader 时返回带诊断的错误。
    pub async fn find_leader(&self, shard_id: u64) -> Result<(String, u64), anyhow::Error> {
        match self.try_find_leader(shard_id).await {
            Ok(value) => Ok(value),
            Err(LeaderLookupError::NotInitialized) => Err(anyhow!(
                "分片 {shard_id} 未初始化：所有端点均返回 not_found（请先运行 esctl init）"
            )),
            Err(LeaderLookupError::NoLeader { detail }) => Err(anyhow!(
                "分片 {shard_id} 当前无 leader（选举中或全部端点不可达：{detail}）"
            )),
        }
    }

    async fn try_find_leader(&self, shard_id: u64) -> Result<(String, u64), LeaderLookupError> {
        let mut initialized = false;
        let mut errors = Vec::new();
        for endpoint in self.rotated_endpoints() {
            match self.get_raft_state_via(&endpoint, shard_id).await {
                Ok(state) => {
                    initialized = true;
                    if state.is_leader {
                        return Ok((endpoint, state.node_id));
                    }
                }
                Err(status) if status.code() == Code::NotFound => {}
                Err(status) => errors.push(format!("{endpoint}: {}", status.message())),
            }
        }
        if initialized {
            Err(LeaderLookupError::NoLeader {
                detail: errors.join("；"),
            })
        } else {
            Err(LeaderLookupError::NotInitialized)
        }
    }

    /// 在目标 Shard leader 上执行管理 RPC，瞬态失败时最多重试三轮。
    ///
    /// `f` 接收已连接的 RaftAdmin 客户端；成功返回其结果。未初始化、leader
    /// 发现失败、连接失败或三轮 RPC 均失败时返回错误。
    pub async fn with_admin_leader<T, F, Fut>(
        &self,
        shard_id: u64,
        f: F,
    ) -> Result<T, anyhow::Error>
    where
        F: Fn(RaftAdminClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let mut last_error = String::new();
        for _ in 0..3 {
            let (endpoint, _) = match self.try_find_leader(shard_id).await {
                Ok(value) => value,
                Err(LeaderLookupError::NotInitialized) => {
                    return Err(anyhow!("分片 {shard_id} 未初始化（请先运行 esctl init）"));
                }
                Err(LeaderLookupError::NoLeader { detail }) => {
                    last_error = detail;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };
            match self.admin_client(&endpoint).await {
                Ok(client) => match f(client).await {
                    Ok(value) => return Ok(value),
                    Err(status) => last_error = status.message().to_string(),
                },
                Err(error) => last_error = error.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(anyhow!("分片 {shard_id} 管理操作失败：{last_error}"))
    }
}

enum LeaderLookupError {
    NotInitialized,
    NoLeader { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ElectingAdmin {
        leader: std::sync::atomic::AtomicBool,
    }

    #[tonic::async_trait]
    impl es_proto::eventstore::raft_admin_server::RaftAdmin for ElectingAdmin {
        async fn initialize(
            &self,
            _request: tonic::Request<es_proto::eventstore::InitializeRequest>,
        ) -> Result<tonic::Response<es_proto::eventstore::InitializeResponse>, Status> {
            Err(Status::unimplemented("测试未使用"))
        }

        async fn add_learner(
            &self,
            _request: tonic::Request<es_proto::eventstore::AddLearnerRequest>,
        ) -> Result<tonic::Response<es_proto::eventstore::AddLearnerResponse>, Status> {
            Err(Status::unimplemented("测试未使用"))
        }

        async fn change_membership(
            &self,
            _request: tonic::Request<es_proto::eventstore::ChangeMembershipRequest>,
        ) -> Result<tonic::Response<es_proto::eventstore::ChangeMembershipResponse>, Status>
        {
            Err(Status::unimplemented("测试未使用"))
        }

        async fn get_raft_state(
            &self,
            _request: tonic::Request<GetRaftStateRequest>,
        ) -> Result<tonic::Response<GetRaftStateResponse>, Status> {
            let is_leader = self.leader.swap(true, std::sync::atomic::Ordering::SeqCst);
            Ok(tonic::Response::new(GetRaftStateResponse {
                node_id: 1,
                server_state: if is_leader {
                    "Leader".into()
                } else {
                    "Candidate".into()
                },
                is_leader,
                has_leader: is_leader,
                current_leader: u64::from(is_leader),
                current_term: 1,
                has_last_log_index: false,
                last_log_index: 0,
                has_last_applied: false,
                last_applied: 0,
                voter_ids: vec![1],
            }))
        }

        async fn list_shards(
            &self,
            _request: tonic::Request<ListShardsRequest>,
        ) -> Result<tonic::Response<ListShardsResponse>, Status> {
            Ok(tonic::Response::new(ListShardsResponse {
                node_id: 1,
                shard_ids: vec![0],
            }))
        }
    }

    #[test]
    fn endpoints_are_normalized_and_deduplicated() {
        let client = ClusterClient::new(
            &["127.0.0.1:50051".into(), "http://127.0.0.1:50051".into()],
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect("创建客户端");
        assert_eq!(client.endpoints(), ["http://127.0.0.1:50051"]);
    }

    #[test]
    fn empty_endpoints_are_rejected() {
        assert!(
            ClusterClient::new(&[], None, Duration::from_secs(1), Duration::from_secs(1)).is_err()
        );
    }

    #[tokio::test]
    async fn unreachable_endpoint_covers_timeout_and_leader_failure_paths() {
        let endpoint = "http://127.0.0.1:9".to_string();
        for timeout in [Duration::ZERO, Duration::from_millis(20)] {
            let client =
                ClusterClient::new(std::slice::from_ref(&endpoint), None, timeout, timeout)
                    .expect("创建不可达端点客户端");
            assert!(client.admin_client(&endpoint).await.is_err());
            assert!(client.get_raft_state_via(&endpoint, 0).await.is_err());
            assert!(client.list_shards_via(&endpoint).await.is_err());
            assert!(client.find_leader(0).await.is_err());
            assert!(
                client
                    .with_admin_leader(0, |_client| async { Ok::<_, Status>(()) })
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn cached_channel_and_not_found_leader_are_distinguished() {
        let manager = std::sync::Arc::new(es_raft::ShardManager::new(1, 1));
        let service = es_raft::RaftAdminService::new(manager);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定 RaftAdmin 测试端口");
        let endpoint = format!("http://{}", listener.local_addr().expect("读取测试端口"));
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(es_proto::eventstore::raft_admin_server::RaftAdminServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("RaftAdmin 测试服务退出");
        });
        let client = ClusterClient::new(
            std::slice::from_ref(&endpoint),
            None,
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect("创建集群客户端");
        assert_eq!(
            client
                .list_shards_via(&endpoint)
                .await
                .expect("首次枚举 Shard")
                .node_id,
            1
        );
        assert_eq!(
            client
                .list_shards_via(&endpoint)
                .await
                .expect("复用缓存通道")
                .shard_ids,
            Vec::<u64>::new()
        );
        let error = client.find_leader(0).await.expect_err("未注册 Shard");
        assert!(error.to_string().contains("未初始化"));
        task.abort();
        let _ = task.await;
    }

    #[tokio::test]
    async fn electing_then_leader_states_are_distinguished() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定选举测试端口");
        let endpoint = format!("http://{}", listener.local_addr().expect("读取测试端口"));
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    es_proto::eventstore::raft_admin_server::RaftAdminServer::new(ElectingAdmin {
                        leader: std::sync::atomic::AtomicBool::new(false),
                    }),
                )
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("选举测试服务退出");
        });
        let client = ClusterClient::new(
            std::slice::from_ref(&endpoint),
            None,
            Duration::ZERO,
            Duration::ZERO,
        )
        .expect("创建选举客户端");
        assert!(client.find_leader(0).await.is_err(), "首次查询仍在选举");
        assert_eq!(
            client.find_leader(0).await.expect("第二次查询找到 leader"),
            (endpoint, 1)
        );
        task.abort();
        let _ = task.await;
    }
}
