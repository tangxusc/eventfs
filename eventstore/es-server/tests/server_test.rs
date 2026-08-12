//! 服务器启动测试

use std::time::Duration;
use tokio::time::timeout;

use es_server::config::{Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig};
use es_server::Server;

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
}
