//! RaftAdminService RPC 层测试：参数校验与状态查询（真实 raft 实例）。
//!
//! 网络层用 NoopNet（任何 RPC 返回不可达）——本测试只调 admin 服务
//! 自身的 tonic 方法，不触发节点间通信。

use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft};
use tonic::{Code, Request};

use es_proto::eventstore::raft_admin_server::RaftAdmin;
use es_proto::eventstore::*;
use es_raft::admin_service::RaftAdminService;
use es_raft::{Shard, ShardManager};
use es_storage::{EsStorage, TypeConfig};

/// 无操作网络（与 manager_test.rs 同款）。
#[derive(Clone)]
struct NoopNet(u64);

impl RaftNetworkFactory<TypeConfig> for NoopNet {
    type Network = NoopConn;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        NoopConn {
            from: self.0,
            to: target,
        }
    }
}

fn unreachable<E: std::error::Error>(from: u64, to: u64) -> RPCError<u64, BasicNode, E> {
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "noop net {from} -> {to}"
    ))))
}

struct NoopConn {
    from: u64,
    to: u64,
}

impl RaftNetwork<TypeConfig> for NoopConn {
    async fn append_entries(
        &mut self,
        _req: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(unreachable(self.from, self.to))
    }

    async fn install_snapshot(
        &mut self,
        _req: InstallSnapshotRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(unreachable(self.from, self.to))
    }

    async fn vote(
        &mut self,
        _req: VoteRequest<u64>,
        _o: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(unreachable(self.from, self.to))
    }
}

/// 建一个 Shard 并注册（raft 未初始化）。
async fn make_shard(manager: &ShardManager, id: u64) -> Arc<Shard> {
    let dir = tempfile::tempdir().expect("临时目录");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("开 tree"),
    );
    let store = EsStorage::new(id, tree, Default::default()).expect("建存储");
    store.restore_applied_state().await.expect("恢复状态");
    let cfg = Arc::new(
        Config {
            cluster_name: "admin-test".into(),
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .expect("校验配置"),
    );
    let raft = Raft::new(id, cfg, NoopNet(id), store.clone(), store.clone())
        .await
        .expect("建 Raft");
    let shard = Arc::new(Shard::new(id, raft, Arc::new(store)));
    manager
        .register_shard(shard.clone())
        .await
        .expect("注册分片");
    shard
}

fn svc(manager: ShardManager) -> RaftAdminService {
    RaftAdminService::new(Arc::new(manager))
}

#[tokio::test]
async fn get_raft_state_unregistered_not_found() {
    let manager = ShardManager::new(1, 2);
    let s = svc(manager);
    let err = s
        .get_raft_state(Request::new(GetRaftStateRequest { shard_id: 9 }))
        .await
        .expect_err("未注册分片应报错");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn initialize_empty_members_invalid_argument() {
    let manager = ShardManager::new(1, 2);
    make_shard(&manager, 0).await;
    let s = svc(manager);
    let err = s
        .initialize(Request::new(InitializeRequest {
            shard_id: 0,
            members: vec![],
        }))
        .await
        .expect_err("空成员应报错");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("members"), "{}", err.message());
}

#[tokio::test]
async fn initialize_unregistered_not_found() {
    let manager = ShardManager::new(1, 2);
    let s = svc(manager);
    let err = s
        .initialize(Request::new(InitializeRequest {
            shard_id: 0,
            members: vec![RaftMember {
                node_id: 1,
                addr: "http://127.0.0.1:1".into(),
            }],
        }))
        .await
        .expect_err("未注册分片应报错");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn add_learner_missing_member_invalid_argument() {
    let manager = ShardManager::new(1, 2);
    make_shard(&manager, 0).await;
    let s = svc(manager);
    let err = s
        .add_learner(Request::new(AddLearnerRequest {
            shard_id: 0,
            member: None,
            blocking: false,
        }))
        .await
        .expect_err("缺 member 应报错");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn change_membership_empty_voters_invalid_argument() {
    let manager = ShardManager::new(1, 2);
    make_shard(&manager, 0).await;
    let s = svc(manager);
    let err = s
        .change_membership(Request::new(ChangeMembershipRequest {
            shard_id: 0,
            voter_ids: vec![],
            expected_voters: vec![],
            retain: false,
        }))
        .await
        .expect_err("空 voters 应报错");
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn initialize_then_state_query_and_cas() {
    let manager = ShardManager::new(1, 2);
    make_shard(&manager, 0).await;
    let s = svc(manager);

    // 初始化单节点集群 → 自动成为 leader（raft 节点 id 与分片 id 同为 0）
    s.initialize(Request::new(InitializeRequest {
        shard_id: 0,
        members: vec![RaftMember {
            node_id: 0,
            addr: "http://127.0.0.1:1".into(),
        }],
    }))
    .await
    .expect("initialize 成功");

    // 等 leader 就绪
    let mut is_leader = false;
    for _ in 0..50 {
        let state = s
            .get_raft_state(Request::new(GetRaftStateRequest { shard_id: 0 }))
            .await
            .expect("取状态")
            .into_inner();
        if state.is_leader {
            is_leader = true;
            assert_eq!(state.node_id, 0);
            assert_eq!(state.voter_ids, vec![0]);
            assert!(state.has_last_applied, "initialize 日志应已应用");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(is_leader, "单节点应在 5s 内成为 leader");

    // CAS 冲突：期望 voters=[2] 与实际 [0] 不符 → failed_precondition
    let err = s
        .change_membership(Request::new(ChangeMembershipRequest {
            shard_id: 0,
            voter_ids: vec![0],
            expected_voters: vec![2],
            retain: false,
        }))
        .await
        .expect_err("期望 voters 与实际不符应报错");
    assert_eq!(err.code(), Code::FailedPrecondition);

    // CAS 一致：voters=[0] → 成功
    s.change_membership(Request::new(ChangeMembershipRequest {
        shard_id: 0,
        voter_ids: vec![0],
        expected_voters: vec![0],
        retain: false,
    }))
    .await
    .expect("CAS 一致应成功");
}
