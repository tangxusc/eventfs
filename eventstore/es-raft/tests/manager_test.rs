//! ShardManager 单测：注册校验（越界/重复）、查询、路由、枚举。
//!
//! 构造真实 Raft 实例（单节点，无网络）以得到合法的 Shard 对象；
//! 本测试不触发任何网络 RPC，网络层返回不可达错误即可。

use std::sync::Arc;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft};

use es_raft::{Shard, ShardManager};
use es_storage::{EsStorage, TypeConfig};

/// 无操作网络：任何 RPC 都返回不可达（本测试不需要真实通信）。
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

/// 构造一个已注册的 Shard（真实 raft 实例 + 临时存储）。
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
            cluster_name: "manager-test".into(),
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

#[tokio::test]
async fn register_and_get_basic() {
    let manager = ShardManager::new(1, 2);
    assert_eq!(manager.node_id(), 1);
    assert_eq!(manager.num_shards(), 2);

    let shard = make_shard(&manager, 0).await;
    assert_eq!(shard.id(), 0);
    assert_eq!(shard.shard_id, 0);

    let got = manager.get_shard(0).await.expect("取分片");
    assert_eq!(got.shard_id, 0);
}

#[tokio::test]
async fn register_out_of_range_rejected() {
    let manager = ShardManager::new(1, 2);
    // shard_id >= num_shards 必须拒绝
    let dir = tempfile::tempdir().expect("临时目录");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("开 tree"),
    );
    let store = EsStorage::new(2, tree, Default::default()).expect("建存储");
    store.restore_applied_state().await.expect("恢复状态");
    let cfg = Arc::new(Config::default().validate().expect("校验配置"));
    let raft = Raft::new(2, cfg, NoopNet(2), store.clone(), store.clone())
        .await
        .expect("建 Raft");
    let shard = Arc::new(Shard::new(2, raft, Arc::new(store)));
    let err = manager
        .register_shard(shard)
        .await
        .expect_err("越界应拒绝");
    assert!(err.to_string().contains(">= num_shards"), "{err}");
}

#[tokio::test]
async fn register_duplicate_rejected() {
    let manager = ShardManager::new(1, 2);
    make_shard(&manager, 0).await;
    // 再次注册同 id：contains_key 分支
    let dir = tempfile::tempdir().expect("临时目录");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("开 tree"),
    );
    let store = EsStorage::new(0, tree, Default::default()).expect("建存储");
    store.restore_applied_state().await.expect("恢复状态");
    let cfg = Arc::new(Config::default().validate().expect("校验配置"));
    let raft = Raft::new(0, cfg, NoopNet(0), store.clone(), store.clone())
        .await
        .expect("建 Raft");
    let shard = Arc::new(Shard::new(0, raft, Arc::new(store)));
    let err = manager
        .register_shard(shard)
        .await
        .expect_err("重复注册应拒绝");
    assert!(err.to_string().contains("already registered"), "{err}");
}

#[tokio::test]
async fn get_shard_missing_errors() {
    let manager = ShardManager::new(1, 2);
    let result = manager.get_shard(0).await;
    match result {
        Err(err) => assert!(err.to_string().contains("not found"), "{err}"),
        Ok(_) => panic!("未注册分片应报错"),
    }
}

#[tokio::test]
async fn route_and_shard_ids() {
    let manager = ShardManager::new(1, 2);
    make_shard(&manager, 0).await;
    make_shard(&manager, 1).await;

    // route：根据 stream 哈希选分片（分片 0、1 都在）
    let routed = manager.route_shard("any-stream").await.expect("路由");
    assert!(routed.shard_id < 2);

    let ids = manager.shard_ids().await;
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, vec![0, 1], "应枚举全部已注册分片");
}
