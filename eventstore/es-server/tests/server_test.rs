//! 服务器启动测试

use std::time::Duration;
use tokio::time::timeout;

use es_server::config::{
    Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig, TlsConfig,
};
use es_server::Server;

#[test]
fn config_validation_rejects_invalid_runtime_configuration() {
    let dir = tempfile::tempdir().expect("临时目录");
    let base = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            internal_listen_addr: None,
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: vec![0],
                replica: vec![],
            }],
        },
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };

    let mut empty_placement = base.clone();
    empty_placement.placement.nodes.clear();
    assert!(
        empty_placement.validate().is_err(),
        "空放置表必须在启动前被拒绝"
    );

    let mut zero_replication = base.clone();
    zero_replication.placement.replication_factor = 0;
    assert!(
        zero_replication.validate().is_err(),
        "零副本因子必须在启动前被拒绝"
    );

    let mut zero_snapshot_retention = base;
    zero_snapshot_retention.snapshot.keep = 0;
    assert!(
        zero_snapshot_retention.validate().is_err(),
        "零快照保留数必须在启动前被拒绝"
    );
}

#[tokio::test]
async fn server_starts_and_inits_raft() {
    let _guard = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    let dir = tempfile::tempdir().expect("临时目录");

    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(), // 端口 0 让 OS 分配，避免冲突
            internal_listen_addr: None,
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        // 单节点 2 分片：rf=1，node1 主承载 [0,1]
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: (0..2).collect(),
                replica: vec![],
            }],
        },
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };

    let server = Server::new(config).expect("创建服务器");

    // init 能成功就说明：
    // 1. surrealkv tree 能打开
    // 2. 所有分片的 EsStorage 能创建
    // 3. restore_applied_state 能执行
    // 4. Raft::new 能成功（即使网络层是 stub）
    timeout(Duration::from_secs(10), server.init())
        .await
        .expect("10 秒超时")
        .expect("初始化应成功");

    // 验证分片已注册
    let shard_ids = server.shard_manager().shard_ids().await;
    assert_eq!(shard_ids.len(), 2, "应有 2 个分片");
    assert!(shard_ids.contains(&0));
    assert!(shard_ids.contains(&1));

    // 重复关闭不能让已停止的后台资源阻塞测试或遗留锁文件。
    server.shutdown().await;
    server.shutdown().await;
}

/// 使用临时端口，避免端到端测试之间相互抢占监听地址。
fn alloc_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("绑定临时端口");
    let addr = listener.local_addr().expect("读取临时端口");
    drop(listener);
    addr.to_string()
}

/// 轮询端口直至服务完成绑定，避免把启动调度延迟误判为失败。
async fn wait_listener(addr: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待监听地址 {addr} 超时"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 内部订阅端口应与公共 API 一同启动，且二者可独立连接。
#[tokio::test]
async fn server_serves_public_and_internal_listeners() {
    let dir = tempfile::tempdir().expect("临时目录");
    let public_addr = alloc_addr();
    let internal_addr = alloc_addr();
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: public_addr.clone(),
            internal_listen_addr: Some(internal_addr.clone()),
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: vec![0],
                replica: vec![],
            }],
        },
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let server = std::sync::Arc::new(Server::new(config).expect("创建服务器"));
    server.init().await.expect("初始化服务器");

    let serving = {
        let server = server.clone();
        tokio::spawn(async move { server.serve().await })
    };
    wait_listener(&public_addr).await;
    wait_listener(&internal_addr).await;

    let request = es_proto::eventstore::InstallOwnershipFenceRequest {
        shard_id: 0,
        stream_id: "listener-scope".into(),
        generation: 1,
    };
    let mut public =
        es_proto::eventstore::ownership_internal_client::OwnershipInternalClient::connect(format!(
            "http://{public_addr}"
        ))
        .await
        .expect("连接公共端口");
    let public_error = public
        .install_ownership_fence(request.clone())
        .await
        .expect_err("公共端口不得暴露归属控制协议");
    assert_eq!(public_error.code(), tonic::Code::Unimplemented);

    let mut internal =
        es_proto::eventstore::ownership_internal_client::OwnershipInternalClient::connect(format!(
            "http://{internal_addr}"
        ))
        .await
        .expect("连接内部端口");
    let internal_error = internal
        .install_ownership_fence(request)
        .await
        .expect_err("未组建 Raft 时应返回业务错误");
    assert_ne!(
        internal_error.code(),
        tonic::Code::Unimplemented,
        "内部端口必须注册归属控制协议"
    );

    serving.abort();
    let _ = serving.await;
    server.shutdown().await;
}

/// TLS 启动路径应同时配置公共 API 与内部订阅监听器。
#[tokio::test]
async fn server_serves_tls_public_and_internal_listeners() {
    let dir = tempfile::tempdir().expect("临时目录");
    let certified =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("生成自签名证书");
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");
    std::fs::write(&cert_path, certified.cert.pem()).expect("写证书");
    std::fs::write(&key_path, certified.key_pair.serialize_pem()).expect("写私钥");

    let public_addr = alloc_addr();
    let internal_addr = alloc_addr();
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: public_addr.clone(),
            internal_listen_addr: Some(internal_addr.clone()),
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: vec![0],
                replica: vec![],
            }],
        },
        snapshot: Default::default(),
        tls: Some(TlsConfig {
            cert_file: Some(cert_path),
            key_file: Some(key_path),
            ca_file: None,
        }),
        limits: Default::default(),
    };
    let server = std::sync::Arc::new(Server::new(config).expect("创建 TLS 服务器"));
    server.init().await.expect("初始化 TLS 服务器");

    let serving = {
        let server = server.clone();
        tokio::spawn(async move { server.serve().await })
    };
    wait_listener(&public_addr).await;
    wait_listener(&internal_addr).await;

    serving.abort();
    let _ = serving.await;
    server.shutdown().await;
}
