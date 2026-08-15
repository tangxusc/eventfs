//! RaftRpcService RPC 层测试：分片路由与 payload 解码的错误路径。
//!
//! 错误路径不触发 raft 内部逻辑；网络工厂直接用 es_raft::GrpcNetwork
//! （顺带覆盖其构造与 shard_id getter）。

use std::sync::Arc;

use tonic::{Code, Request};

use es_proto::eventstore::raft_rpc_server::RaftRpc;
use es_proto::eventstore::*;
use es_raft::ShardManager;
use es_raft::rpc_service::RaftRpcService;

#[tokio::test]
async fn three_methods_unregistered_not_found() {
    let s = RaftRpcService::new(Arc::new(ShardManager::new(1, 2)));

    let err = s
        .append_entries(Request::new(RaftAppendEntriesRequest {
            shard_id: 9,
            payload: vec![],
        }))
        .await
        .expect_err("未注册分片应报错");
    assert_eq!(err.code(), Code::NotFound);
    assert!(err.message().contains("分片 9"), "{}", err.message());

    let err = s
        .vote(Request::new(RaftVoteRequest {
            shard_id: 9,
            payload: vec![],
        }))
        .await
        .expect_err("未注册分片应报错");
    assert_eq!(err.code(), Code::NotFound);

    let err = s
        .install_snapshot(Request::new(RaftInstallSnapshotRequest {
            shard_id: 9,
            payload: vec![],
        }))
        .await
        .expect_err("未注册分片应报错");
    assert_eq!(err.code(), Code::NotFound);
}

#[tokio::test]
async fn garbage_payload_decode_invalid_argument() {
    // 注册分片后喂垃圾字节：路由成功 → decode 失败 → invalid_argument
    let manager = ShardManager::new(1, 2);
    let dir = tempfile::tempdir().expect("临时目录");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("开 tree"),
    );
    let store = es_storage::EsStorage::new(0, tree, Default::default()).expect("建存储");
    store.restore_applied_state().await.expect("恢复状态");
    let cfg = Arc::new(openraft::Config::default().validate().expect("校验配置"));
    let raft = openraft::Raft::new(
        0,
        cfg,
        es_raft::GrpcNetwork::new(0, None),
        store.clone(),
        store.clone(),
    )
    .await
    .expect("建 Raft");
    manager
        .register_shard(Arc::new(es_raft::Shard::new(0, raft, Arc::new(store))))
        .await
        .expect("注册分片");

    let s = RaftRpcService::new(Arc::new(manager));
    let err = s
        .vote(Request::new(RaftVoteRequest {
            shard_id: 0,
            payload: b"garbage".to_vec(),
        }))
        .await
        .expect_err("垃圾 payload 应报错");
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("反序列化 VoteRequest"),
        "{}",
        err.message()
    );
}
