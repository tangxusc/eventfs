//! Raft RPC 服务端：接收其它节点的 AppendEntries / Vote / InstallSnapshot。

use std::sync::Arc;

use tonic::{Request, Response, Status};

use es_proto::eventstore::raft_rpc_server::RaftRpc;
use es_proto::eventstore::*;
use es_storage::TypeConfig;

use crate::ShardManager;

/// Raft 节点间 RPC 服务
pub struct RaftRpcService {
    shard_manager: Arc<ShardManager>,
}

impl RaftRpcService {
    pub fn new(shard_manager: Arc<ShardManager>) -> Self {
        Self { shard_manager }
    }
}

fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8], what: &str) -> Result<T, Status> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| Status::invalid_argument(format!("反序列化 {what} 失败: {e}")))
}

fn encode<T: serde::Serialize>(v: &T, what: &str) -> Result<Vec<u8>, Status> {
    bincode::serde::encode_to_vec(v, bincode::config::standard())
        .map_err(|e| Status::internal(format!("序列化 {what} 失败: {e}")))
}

#[tonic::async_trait]
impl RaftRpc for RaftRpcService {
    async fn append_entries(
        &self,
        request: Request<RaftAppendEntriesRequest>,
    ) -> Result<Response<RaftAppendEntriesResponse>, Status> {
        let req = request.into_inner();
        // shard_id 由请求携带，据此路由到本节点对应的 Raft 实例
        let shard = self
            .shard_manager
            .get_shard(req.shard_id)
            .await
            .map_err(|e| Status::not_found(format!("分片 {}: {e}", req.shard_id)))?;

        let raft_req: openraft::raft::AppendEntriesRequest<TypeConfig> =
            decode(&req.payload, "AppendEntriesRequest")?;

        let resp = shard
            .raft
            .append_entries(raft_req)
            .await
            .map_err(|e| Status::internal(format!("append_entries 失败: {e}")))?;

        Ok(Response::new(RaftAppendEntriesResponse {
            payload: encode(&resp, "AppendEntriesResponse")?,
        }))
    }

    async fn vote(
        &self,
        request: Request<RaftVoteRequest>,
    ) -> Result<Response<RaftVoteResponse>, Status> {
        let req = request.into_inner();
        let shard = self
            .shard_manager
            .get_shard(req.shard_id)
            .await
            .map_err(|e| Status::not_found(format!("分片 {}: {e}", req.shard_id)))?;

        let raft_req: openraft::raft::VoteRequest<u64> = decode(&req.payload, "VoteRequest")?;

        let resp = shard
            .raft
            .vote(raft_req)
            .await
            .map_err(|e| Status::internal(format!("vote 失败: {e}")))?;

        Ok(Response::new(RaftVoteResponse {
            payload: encode(&resp, "VoteResponse")?,
        }))
    }

    async fn install_snapshot(
        &self,
        request: Request<RaftInstallSnapshotRequest>,
    ) -> Result<Response<RaftInstallSnapshotResponse>, Status> {
        let req = request.into_inner();
        let shard = self
            .shard_manager
            .get_shard(req.shard_id)
            .await
            .map_err(|e| Status::not_found(format!("分片 {}: {e}", req.shard_id)))?;

        let raft_req: openraft::raft::InstallSnapshotRequest<TypeConfig> =
            decode(&req.payload, "InstallSnapshotRequest")?;

        let resp = shard
            .raft
            .install_snapshot(raft_req)
            .await
            .map_err(|e| Status::internal(format!("install_snapshot 失败: {e}")))?;

        Ok(Response::new(RaftInstallSnapshotResponse {
            payload: encode(&resp, "InstallSnapshotResponse")?,
        }))
    }
}
