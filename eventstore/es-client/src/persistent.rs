//! 持久化订阅客户端。
//!
//! 所有操作由 control Shard leader 协调。客户端消费 leader hint，在节点故障或
//! 选举期间有界轮换；Fetch 仍是 unary long-poll，未在客户端预取或缓存事件。

use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

use es_core::{LeaderRetryPlan, parse_leader_hint};
use es_proto::endpoint::normalize_endpoint;
use es_proto::eventstore::persistent_subscriptions_client::PersistentSubscriptionsClient as GrpcClient;
use es_proto::eventstore::*;
use es_proto::tls::{TlsClientConfig, apply_endpoint_tls};
use tonic::Code;
use tonic::transport::Channel;

use crate::ClientError;

const ELECTION_RETRY_DELAY: Duration = Duration::from_millis(200);

/// 命名持久化订阅组客户端。
///
/// 客户端不保存 checkpoint；服务端 Raft 状态机持久化组进度、租约、重试与 parked。
/// 方法接收 protobuf 请求类型，便于调用方完整使用协议能力。
#[derive(Debug)]
pub struct PersistentSubscriptionsClient {
    clients: HashMap<String, GrpcClient<Channel>>,
    nodes: Vec<String>,
    tls: Option<TlsClientConfig>,
    cursor: usize,
}

impl PersistentSubscriptionsClient {
    /// 连接集群并使用默认 https 信任策略。
    ///
    /// # 参数
    /// `nodes` 至少包含一个 http/https 节点地址。
    ///
    /// # 返回
    /// 返回可复用的持久化订阅客户端。
    ///
    /// # 错误
    /// 节点列表为空、地址非法或首节点连接失败时返回 [`ClientError`]。
    pub async fn connect(nodes: Vec<String>) -> Result<Self, ClientError> {
        Self::connect_with_tls(nodes, None).await
    }

    /// 连接集群并指定 https 信任策略。
    ///
    /// # 参数
    /// `nodes` 是候选节点；`tls` 仅作用于 https 地址。
    ///
    /// # 返回
    /// 返回可复用的持久化订阅客户端。
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
            clients: HashMap::from([(first, client)]),
            nodes,
            tls,
            cursor: 0,
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

    async fn get_or_connect(&mut self, address: &str) -> Result<GrpcClient<Channel>, ClientError> {
        if let Some(client) = self.clients.get(address) {
            return Ok(client.clone());
        }
        let client = Self::connect_one(address, self.tls.as_ref()).await?;
        self.clients.insert(address.to_string(), client.clone());
        Ok(client)
    }

    fn rotated_nodes(&mut self) -> Vec<String> {
        let start = self.cursor % self.nodes.len();
        self.cursor = self.cursor.wrapping_add(1);
        (0..self.nodes.len())
            .map(|offset| self.nodes[(start + offset) % self.nodes.len()].clone())
            .collect()
    }

    async fn call<T, F, Fut>(&mut self, operation: F) -> Result<T, ClientError>
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
                    } else {
                        plan.retry_later(target);
                    }
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

    /// 创建命名订阅组。
    ///
    /// # 错误
    /// 组已存在、参数非法、control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn create(
        &mut self,
        request: CreatePersistentSubscriptionRequest,
    ) -> Result<PersistentSubscriptionInfo, ClientError> {
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .create_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 以 revision CAS 更新订阅组。
    ///
    /// # 错误
    /// revision 冲突、参数非法、组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn update(
        &mut self,
        request: UpdatePersistentSubscriptionRequest,
    ) -> Result<PersistentSubscriptionInfo, ClientError> {
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .update_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 以 revision CAS 删除订阅组。
    ///
    /// # 错误
    /// revision 冲突、组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn delete(
        &mut self,
        request: DeletePersistentSubscriptionRequest,
    ) -> Result<(), ClientError> {
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .delete_persistent_subscription(request)
                    .await
                    .map(|_| ())
            }
        })
        .await
    }

    /// 获取一个订阅组。
    ///
    /// # 错误
    /// 组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn get(
        &mut self,
        name: impl Into<String>,
    ) -> Result<PersistentSubscriptionInfo, ClientError> {
        let request = GetPersistentSubscriptionRequest { name: name.into() };
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .get_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 枚举全部订阅组。
    ///
    /// # 错误
    /// control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn list(&mut self) -> Result<Vec<PersistentSubscriptionInfo>, ClientError> {
        self.call(|mut client| async move {
            client
                .list_persistent_subscriptions(ListPersistentSubscriptionsRequest {})
                .await
                .map(|response| response.into_inner().subscriptions)
        })
        .await
    }

    /// 按服务端额度拉取一批 delivery；空响应可能表示 caught-up 或长轮询超时。
    ///
    /// # 错误
    /// 请求超限、组不存在、control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn fetch(
        &mut self,
        request: FetchPersistentSubscriptionRequest,
    ) -> Result<FetchPersistentSubscriptionResponse, ClientError> {
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .fetch_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 批量逐消息确认、重试、停放或跳过 delivery。
    ///
    /// # 错误
    /// 请求非法、组不存在、control leader 不可用或 RPC 失败时返回 [`ClientError`]。
    pub async fn settle(
        &mut self,
        request: SettlePersistentSubscriptionRequest,
    ) -> Result<SettlePersistentSubscriptionResponse, ClientError> {
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .settle_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 分页读取 parked 事件。
    ///
    /// # 错误
    /// 组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn list_parked(
        &mut self,
        request: ListParkedPersistentSubscriptionRequest,
    ) -> Result<ListParkedPersistentSubscriptionResponse, ClientError> {
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .list_parked_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await
    }

    /// 将组内全部 parked 事件放回重试队列。
    ///
    /// # 错误
    /// 组不存在或 RPC 失败时返回 [`ClientError`]。
    pub async fn replay_parked(&mut self, name: impl Into<String>) -> Result<u64, ClientError> {
        let request = ReplayParkedPersistentSubscriptionRequest { name: name.into() };
        self.call(|mut client| {
            let request = request.clone();
            async move {
                client
                    .replay_parked_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner().replayed_count)
            }
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rejects_empty_node_list() {
        let error = PersistentSubscriptionsClient::connect(Vec::new())
            .await
            .expect_err("空节点列表必须失败");
        assert!(matches!(error, ClientError::InvalidConfig(_)));
    }
}
