//! Raft 集群管理服务：初始化集群、增删成员、查询状态。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::BasicNode;
use tonic::{Request, Response, Status};

use es_proto::eventstore::raft_admin_server::RaftAdmin;
use es_proto::eventstore::*;

use crate::ShardManager;

/// Raft 集群管理服务
pub struct RaftAdminService {
    shard_manager: Arc<ShardManager>,
}

impl RaftAdminService {
    pub fn new(shard_manager: Arc<ShardManager>) -> Self {
        Self { shard_manager }
    }
}

#[tonic::async_trait]
impl RaftAdmin for RaftAdminService {
    async fn initialize(
        &self,
        request: Request<InitializeRequest>,
    ) -> Result<Response<InitializeResponse>, Status> {
        let req = request.into_inner();
        let shard = self
            .shard_manager
            .get_shard(req.shard_id)
            .await
            .map_err(|e| Status::not_found(format!("分片 {}: {e}", req.shard_id)))?;

        if req.members.is_empty() {
            return Err(Status::invalid_argument("members 不能为空"));
        }

        // 地址随 membership 日志复制到各节点，网络层据此回连，
        // 因此这里必须把 addr 一并写入，不能只给 node_id。
        let members: BTreeMap<u64, BasicNode> = req
            .members
            .into_iter()
            .map(|m| (m.node_id, BasicNode { addr: m.addr }))
            .collect();

        shard
            .raft
            .initialize(members)
            .await
            .map_err(|e| Status::failed_precondition(format!("initialize 失败: {e}")))?;

        Ok(Response::new(InitializeResponse {}))
    }

    async fn add_learner(
        &self,
        request: Request<AddLearnerRequest>,
    ) -> Result<Response<AddLearnerResponse>, Status> {
        let req = request.into_inner();
        let shard = self
            .shard_manager
            .get_shard(req.shard_id)
            .await
            .map_err(|e| Status::not_found(format!("分片 {}: {e}", req.shard_id)))?;

        let member = req
            .member
            .ok_or_else(|| Status::invalid_argument("member 不能为空"))?;

        shard
            .raft
            .add_learner(
                member.node_id,
                BasicNode { addr: member.addr },
                req.blocking,
            )
            .await
            .map_err(|e| Status::internal(format!("add_learner 失败: {e}")))?;

        Ok(Response::new(AddLearnerResponse {}))
    }

    async fn change_membership(
        &self,
        request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        let req = request.into_inner();
        let shard = self
            .shard_manager
            .get_shard(req.shard_id)
            .await
            .map_err(|e| Status::not_found(format!("分片 {}: {e}", req.shard_id)))?;

        if req.voter_ids.is_empty() {
            return Err(Status::invalid_argument("voter_ids 不能为空"));
        }

        let voters: BTreeSet<u64> = req.voter_ids.into_iter().collect();

        shard
            .raft
            .change_membership(voters, req.retain)
            .await
            .map_err(|e| Status::internal(format!("change_membership 失败: {e}")))?;

        Ok(Response::new(ChangeMembershipResponse {}))
    }

    async fn get_raft_state(
        &self,
        request: Request<GetRaftStateRequest>,
    ) -> Result<Response<GetRaftStateResponse>, Status> {
        let req = request.into_inner();
        let shard = self
            .shard_manager
            .get_shard(req.shard_id)
            .await
            .map_err(|e| Status::not_found(format!("分片 {}: {e}", req.shard_id)))?;

        // metrics 是 watch channel 的快照，读取不阻塞
        let m = shard.raft.metrics().borrow().clone();

        let voter_ids: Vec<u64> = m.membership_config.membership().voter_ids().collect();

        Ok(Response::new(GetRaftStateResponse {
            node_id: m.id,
            server_state: format!("{:?}", m.state),
            is_leader: m.state.is_leader(),
            has_leader: m.current_leader.is_some(),
            current_leader: m.current_leader.unwrap_or(0),
            current_term: m.current_term,
            has_last_log_index: m.last_log_index.is_some(),
            last_log_index: m.last_log_index.unwrap_or(0),
            has_last_applied: m.last_applied.is_some(),
            last_applied: m.last_applied.map(|l| l.index).unwrap_or(0),
            voter_ids,
        }))
    }
}
