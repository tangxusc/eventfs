//! openraft RaftNetwork 实现：通过 gRPC 做节点间通信。

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use es_proto::eventstore::raft_rpc_client::RaftRpcClient;
use es_proto::tls::{TlsClientConfig, apply_endpoint_tls};
use es_storage::TypeConfig;

// 端点归一化规则定义在 es-proto（es-ctl 等客户端共用），此处 re-export 保持原路径 API：
// `es_raft::network::normalize_endpoint` 与 `es_raft::normalize_endpoint` 均仍可用。
pub use es_proto::endpoint::normalize_endpoint;

/// 某分片的网络工厂。
///
/// 每个分片各持一个实例并记住自己的 `shard_id`：`RaftNetworkFactory::new_client`
/// 只传 target 节点，不传分片，所以分片信息必须由工厂自身携带，
/// 否则对端无法知道该把消息投给哪个 Raft 实例。
#[derive(Clone)]
pub struct GrpcNetwork {
    shard_id: u64,
    /// 出站 Raft RPC 的客户端信任策略：目标地址为 https:// 时生效
    /// （缺省跳过校验，自签友好）；http 集群传 None。
    tls: Option<TlsClientConfig>,
}

impl GrpcNetwork {
    /// tls：节点间 Raft RPC 的 TLS 信任策略；明文集群传 None。
    pub fn new(shard_id: u64, tls: Option<TlsClientConfig>) -> Self {
        Self { shard_id, tls }
    }

    pub fn shard_id(&self) -> u64 {
        self.shard_id
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetwork {
    type Network = GrpcConnection;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        // 地址取自 openraft 保存的 BasicNode.addr —— 它随 add_learner/initialize
        // 写入 membership 日志并复制到各节点，因此无需另建 node_id → addr 映射表。
        GrpcConnection::new(self.shard_id, target, node.addr.clone(), self.tls.clone())
    }
}

/// 指向单个目标节点的连接。
pub struct GrpcConnection {
    shard_id: u64,
    target: u64,
    addr: String,
    /// 出站 TLS 信任策略（https 目标时生效）
    tls: Option<TlsClientConfig>,
    /// 惰性建立的 gRPC 通道。
    ///
    /// 用 `connect_lazy` 而非 `connect`：后者在对端未就绪时直接失败，
    /// 而集群启动阶段各节点上线有先后，选举期间必然出现互相未就绪。
    /// 惰性通道会在每次调用时按需重连，且 Channel 内部自带连接复用。
    client: Option<RaftRpcClient<tonic::transport::Channel>>,
}

impl GrpcConnection {
    fn new(shard_id: u64, target: u64, addr: String, tls: Option<TlsClientConfig>) -> Self {
        Self {
            shard_id,
            target,
            addr,
            tls,
            client: None,
        }
    }

    /// 取出（必要时建立）gRPC 客户端
    fn client<E>(
        &mut self,
    ) -> Result<RaftRpcClient<tonic::transport::Channel>, RPCError<u64, BasicNode, E>>
    where
        E: std::error::Error,
    {
        if let Some(c) = &self.client {
            return Ok(c.clone());
        }

        // BasicNode.addr 可能不带 scheme，补上 http:// 才是合法的 endpoint
        let uri = normalize_endpoint(&self.addr);

        let endpoint = tonic::transport::Endpoint::from_shared(uri.clone()).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "节点 {} 地址 {uri} 非法: {e}",
                self.target
            ))))
        })?;

        // https 目标按信任策略装配 TLS（缺省跳过校验）；http 原样
        let endpoint = apply_endpoint_tls(endpoint, self.tls.as_ref()).map_err(|e| {
            RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
                "节点 {} TLS 配置失败（{uri}）: {e}",
                self.target
            ))))
        })?;

        let c = RaftRpcClient::new(endpoint.connect_lazy());
        self.client = Some(c.clone());
        Ok(c)
    }
}

fn net_err<E>(ctx: &str, e: impl std::fmt::Display) -> RPCError<u64, BasicNode, E>
where
    E: std::error::Error,
{
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "{ctx}: {e}"
    ))))
}

impl RaftNetwork<TypeConfig> for GrpcConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())
            .map_err(|e| net_err("序列化 AppendEntries 请求", e))?;

        let mut client = self.client()?;
        let resp = client
            .append_entries(es_proto::eventstore::RaftAppendEntriesRequest {
                shard_id: self.shard_id,
                payload,
            })
            .await
            .map_err(|e| net_err("AppendEntries RPC", e))?
            .into_inner();

        let out = bincode::serde::decode_from_slice(&resp.payload, bincode::config::standard())
            .map_err(|e| net_err("反序列化 AppendEntries 响应", e))?
            .0;
        Ok(out)
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())
            .map_err(|e| net_err("序列化 InstallSnapshot 请求", e))?;

        let mut client = self.client()?;
        let resp = client
            .install_snapshot(es_proto::eventstore::RaftInstallSnapshotRequest {
                shard_id: self.shard_id,
                payload,
            })
            .await
            .map_err(|e| net_err("InstallSnapshot RPC", e))?
            .into_inner();

        let out = bincode::serde::decode_from_slice(&resp.payload, bincode::config::standard())
            .map_err(|e| net_err("反序列化 InstallSnapshot 响应", e))?
            .0;
        Ok(out)
    }

    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let payload = bincode::serde::encode_to_vec(&req, bincode::config::standard())
            .map_err(|e| net_err("序列化 Vote 请求", e))?;

        let mut client = self.client()?;
        let resp = client
            .vote(es_proto::eventstore::RaftVoteRequest {
                shard_id: self.shard_id,
                payload,
            })
            .await
            .map_err(|e| net_err("Vote RPC", e))?
            .into_inner();

        let out = bincode::serde::decode_from_slice(&resp.payload, bincode::config::standard())
            .map_err(|e| net_err("反序列化 Vote 响应", e))?
            .0;
        Ok(out)
    }
}
