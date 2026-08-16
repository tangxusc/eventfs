//! AggregateStore 与 Raft 共享的中性 RPC 支撑。

use std::collections::BTreeMap;
use std::sync::Arc;

use es_proto::eventstore::{GetRaftStateRequest, raft_admin_client::RaftAdminClient};
use es_proto::tls::TlsClientConfig;
use tonic::Status;

use crate::config::Config;

/// AggregateStore 运行期使用的原子拓扑快照。
///
/// Shard 放置集合与远端节点定位器必须来自同一份配置，调用方取得快照后可在一次
/// 操作内稳定使用；配置热更新只有在本地新增 Shard 全部就绪后才替换该快照。
#[derive(Clone)]
pub(crate) struct RuntimeTopology {
    inner: Arc<tokio::sync::RwLock<RuntimeTopologySnapshot>>,
}

/// 单次 AggregateStore 操作可见的不可变拓扑。
#[derive(Clone)]
pub(crate) struct RuntimeTopologySnapshot {
    pub(crate) remote: RemoteShards,
    pub(crate) all_shards: Vec<u64>,
    pub(crate) control_shard_id: u64,
}

impl RuntimeTopologySnapshot {
    fn from_config(config: &Config) -> Result<Self, String> {
        let all_shards: Vec<u64> = config
            .placement
            .nodes
            .iter()
            .flat_map(|node| node.primary.iter().chain(node.replica.iter()))
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let control_shard_id = *all_shards
            .first()
            .ok_or_else(|| "AggregateStore 要求至少一个 Shard".to_string())?;
        Ok(Self {
            remote: RemoteShards::new(config)?,
            all_shards,
            control_shard_id,
        })
    }
}

impl RuntimeTopology {
    /// 从启动配置建立初始拓扑；放置为空、peer 或 TLS 配置非法时返回错误。
    pub(crate) fn new(config: &Config) -> Result<Self, String> {
        Ok(Self {
            inner: Arc::new(tokio::sync::RwLock::new(
                RuntimeTopologySnapshot::from_config(config)?,
            )),
        })
    }

    /// 返回当前原子快照，后续热更新不会改变已返回值。
    pub(crate) async fn snapshot(&self) -> RuntimeTopologySnapshot {
        self.inner.read().await.clone()
    }

    /// 原子应用经校验的新配置。
    ///
    /// 运行期只允许追加 Shard，且控制 Shard 不得变化；违反约束或远端地址无效时
    /// 返回错误并完整保留旧快照。
    pub(crate) async fn reload(&self, config: &Config) -> Result<(), String> {
        let next = RuntimeTopologySnapshot::from_config(config)?;
        let mut current = self.inner.write().await;
        if next.control_shard_id != current.control_shard_id {
            return Err(format!(
                "运行期不能变更控制 Shard：当前 {}，新配置 {}",
                current.control_shard_id, next.control_shard_id
            ));
        }
        let removed: Vec<_> = current
            .all_shards
            .iter()
            .copied()
            .filter(|shard_id| !next.all_shards.contains(shard_id))
            .collect();
        if !removed.is_empty() {
            return Err(format!("运行期不能移除 Shard：{removed:?}"));
        }
        *current = next;
        Ok(())
    }
}

/// 远程 Shard leader 定位器。
///
/// 根据静态 peer 配置探测目标 Shard 的 leader，并建立节点内部
/// AggregateStore 通道。探测失败返回可重试的 gRPC `Unavailable`。
#[derive(Clone)]
pub struct RemoteShards {
    self_id: u64,
    clients: BTreeMap<u64, RaftAdminClient<tonic::transport::Channel>>,
    addrs: BTreeMap<u64, String>,
    internal_addrs: BTreeMap<u64, String>,
    tls: Option<TlsClientConfig>,
}

impl RemoteShards {
    /// 从服务器配置创建定位器。
    ///
    /// `config` 提供节点 ID、peer 公共/内部地址及 TLS 信任配置；地址或 TLS
    /// 配置非法时返回错误。创建过程不建立网络连接。
    pub fn new(config: &Config) -> Result<Self, String> {
        let members: BTreeMap<u64, openraft::BasicNode> = config
            .node
            .peers
            .iter()
            .map(|peer| {
                let addr = es_raft::normalize_endpoint(&peer.addr);
                (peer.id, openraft::BasicNode { addr })
            })
            .collect();
        let tls = config
            .tls
            .as_ref()
            .map(|tls| tls.client_trust())
            .transpose()?;
        let clients = crate::bootstrap::build_clients(&members, tls.as_ref())?;
        let addrs = members
            .into_iter()
            .map(|(id, node)| (id, node.addr))
            .collect();
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

    /// 探测 `shard_id` 的 leader，返回节点 ID 和公共地址。
    ///
    /// 未承载、不可达或选举中的节点会被跳过；全部探测失败时返回 `None`。
    pub(crate) async fn find_leader(&self, shard_id: u64) -> Option<(u64, String)> {
        for (&node_id, client) in &self.clients {
            if node_id == self.self_id {
                continue;
            }
            let mut client = client.clone();
            let Ok(response) = client
                .get_raft_state(GetRaftStateRequest { shard_id })
                .await
            else {
                continue;
            };
            if response.into_inner().is_leader {
                return Some((
                    node_id,
                    self.addrs.get(&node_id).cloned().unwrap_or_default(),
                ));
            }
        }
        None
    }

    /// 把远程 leader 定位结果转换为标准重定向状态。
    ///
    /// 返回值始终为 `Unavailable`；已定位时携带 leader ID 和地址，否则提示稍后重试。
    pub(crate) async fn leader_hint_status(&self, shard_id: u64) -> Status {
        match self.find_leader(shard_id).await {
            Some((id, addr)) => {
                Status::unavailable(format!("not leader; leader_id={id} leader_addr={addr}"))
            }
            None => Status::unavailable("not leader; leader unknown, retry later"),
        }
    }

    /// 连接 `shard_id` leader 的 AggregateStore 内部服务。
    ///
    /// 成功返回配置好消息上限与 TLS 的客户端；leader 未知、缺少内部地址、
    /// 地址非法或连接失败时返回可重试的 `Unavailable`。
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

/// 把 Raft 写错误映射为 AggregateStore gRPC 状态。
///
/// `ForwardToLeader` 保留节点 ID 和地址以支持客户端重定向；成员变更错误映射为
/// `FailedPrecondition`，Raft 致命错误映射为 `Internal`。
pub(crate) fn client_write_to_status(
    error: openraft::error::RaftError<
        u64,
        openraft::error::ClientWriteError<u64, openraft::BasicNode>,
    >,
) -> Status {
    use openraft::error::{ClientWriteError, RaftError};

    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            let addr = forward
                .leader_node
                .as_ref()
                .map(|node| node.addr.clone())
                .unwrap_or_default();
            match forward.leader_id {
                Some(id) => {
                    Status::unavailable(format!("not leader; leader_id={id} leader_addr={addr}"))
                }
                None => Status::unavailable("not leader; leader unknown, retry later"),
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(error)) => {
            Status::failed_precondition(format!("成员变更错误: {error}"))
        }
        RaftError::Fatal(error) => Status::internal(format!("Raft 致命错误: {error}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PeerConfig;
    use openraft::error::{
        ChangeMembershipError, ClientWriteError, EmptyMembership, Fatal, ForwardToLeader, RaftError,
    };

    #[tokio::test]
    async fn remote_shards_skip_self_and_unreachable_peers() {
        let mut config = Config::default();
        config.node.peers = vec![PeerConfig {
            id: config.node.id,
            addr: "127.0.0.1:9".into(),
            internal_addr: None,
        }];
        let remote = RemoteShards::new(&config).expect("构造自节点定位器");
        assert_eq!(remote.find_leader(0).await, None);

        config.node.peers = vec![PeerConfig {
            id: 2,
            addr: "127.0.0.1:9".into(),
            internal_addr: Some("127.0.0.1:19".into()),
        }];
        let remote = RemoteShards::new(&config).expect("构造不可达节点定位器");
        assert_eq!(remote.find_leader(0).await, None);
        assert_eq!(
            remote.leader_hint_status(0).await.code(),
            tonic::Code::Unavailable
        );
        assert_eq!(
            remote
                .aggregate_internal_client(0)
                .await
                .expect_err("leader 未知必须失败")
                .code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn runtime_topology_reloads_shards_and_peers_atomically() {
        let config = Config::default();
        let topology = RuntimeTopology::new(&config).expect("创建初始拓扑");
        assert_eq!(topology.snapshot().await.all_shards, [0]);

        let mut expanded = config.clone();
        expanded.placement.nodes[0].primary.push(1);
        expanded.node.peers.push(PeerConfig {
            id: 2,
            addr: "127.0.0.1:50052".into(),
            internal_addr: Some("127.0.0.1:51052".into()),
        });
        topology.reload(&expanded).await.expect("扩展运行期拓扑");

        let snapshot = topology.snapshot().await;
        assert_eq!(snapshot.all_shards, [0, 1]);
        assert!(snapshot.remote.addrs.contains_key(&2));
        assert!(snapshot.remote.internal_addrs.contains_key(&2));
    }

    #[tokio::test]
    async fn runtime_topology_rejects_shard_removal_without_partial_update() {
        let mut config = Config::default();
        config.placement.nodes[0].primary.push(1);
        let topology = RuntimeTopology::new(&config).expect("创建双 Shard 拓扑");

        let mut reduced = config;
        reduced.placement.nodes[0]
            .primary
            .retain(|shard| *shard == 0);
        let error = topology
            .reload(&reduced)
            .await
            .expect_err("运行期缩容必须拒绝");
        assert!(error.contains("不能移除 Shard"));
        assert_eq!(topology.snapshot().await.all_shards, [0, 1]);
    }

    #[test]
    fn raft_write_errors_keep_grpc_categories_and_hints() {
        let with_leader =
            RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::new(
                2,
                openraft::BasicNode {
                    addr: "http://127.0.0.1:50052".into(),
                },
            )));
        let status = client_write_to_status(with_leader);
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert!(status.message().contains("leader_id=2"));

        let unknown =
            RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader::empty()));
        assert!(
            client_write_to_status(unknown)
                .message()
                .contains("leader unknown")
        );

        let membership = RaftError::APIError(ClientWriteError::ChangeMembershipError(
            ChangeMembershipError::EmptyMembership(EmptyMembership {}),
        ));
        assert_eq!(
            client_write_to_status(membership).code(),
            tonic::Code::FailedPrecondition
        );

        let fatal = RaftError::Fatal(Fatal::Stopped);
        assert_eq!(client_write_to_status(fatal).code(), tonic::Code::Internal);
    }
}
