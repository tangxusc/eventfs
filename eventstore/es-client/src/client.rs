//! EventStore 客户端。

use std::collections::HashMap;
use tonic::transport::Channel;

use es_proto::eventstore::event_store_client::EventStoreClient as GrpcClient;
use es_proto::eventstore::*;
use es_proto::tls::{apply_endpoint_tls, TlsClientConfig};

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

        Ok(Self { clients, nodes, tls })
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

    /// 追加事件到流
    ///
    /// # 参数
    /// - `stream_id`: 流 ID
    /// - `expected_version`: 期望版本
    /// - `events`: 待追加事件列表
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

        // 尝试第一个节点
        let first_node = self.nodes[0].clone();
        let client = self.get_or_connect(&first_node).await?;

        let response = client
            .clone()
            .append(request)
            .await
            .map_err(|e| ClientError::RpcFailed(e.to_string()))?;

        Ok(response.into_inner())
    }

    /// 读取流事件
    pub async fn read_stream(
        &mut self,
        stream_id: String,
        from_version: u64,
        max_count: u64,
        direction: Direction,
    ) -> Result<Vec<Event>, ClientError> {
        let request = ReadStreamRequest {
            stream_id,
            from_version,
            max_count,
            direction: direction as i32,
        };

        let first_node = self.nodes[0].clone();
        let client = self.get_or_connect(&first_node).await?;

        let mut stream = client
            .clone()
            .read_stream(request)
            .await
            .map_err(|e| ClientError::RpcFailed(e.to_string()))?
            .into_inner();

        let mut events = Vec::new();
        while let Some(response) = stream
            .message()
            .await
            .map_err(|e| ClientError::RpcFailed(e.to_string()))?
        {
            events.extend(response.events);
        }

        Ok(events)
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

    #[error("Not leader, redirect to: {0:?}")]
    NotLeader(Option<String>),
}
