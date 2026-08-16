//! Aggregate-only 配置 watcher 集成测试。

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use es_proto::eventstore::aggregate_store_server::AggregateStore;
use es_proto::eventstore::{
    AggregateTypeRef, ListAggregatePartitionsRequest, RegisterAggregateTypeRequest,
};
use es_server::Server;

fn write_config(directory: &std::path::Path, shards: &[u64]) -> PathBuf {
    let path = directory.join("config.toml");
    let primary = shards
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    std::fs::write(
        &path,
        format!(
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:0"
peers = []

[storage]
data_dir = "{}"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [{primary}]
"#,
            directory.display()
        ),
    )
    .expect("写配置");
    path
}

async fn start(path: &PathBuf) -> Arc<Server> {
    let config: es_server::Config =
        toml::from_str(&std::fs::read_to_string(path).expect("读取配置")).expect("解析配置");
    let server = Arc::new(Server::new(config).expect("创建服务器"));
    server.init().await.expect("初始化服务器");
    server
}

async fn wait_registered(server: &Server, shard_id: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !server.shard_manager().shard_ids().await.contains(&shard_id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待 Shard 注册超时"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn initialize_and_wait_leader(server: &Server, shard_id: u64) {
    let shard = server
        .shard_manager()
        .get_shard(shard_id)
        .await
        .expect("读取 Shard");
    shard
        .raft
        .initialize(BTreeSet::from([server.config().node.id]))
        .await
        .expect("初始化单节点 Shard");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !shard.raft.metrics().borrow().state.is_leader() {
        assert!(tokio::time::Instant::now() < deadline, "等待 leader 超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn hot_config_adds_shards_without_restart() {
    let directory = tempfile::tempdir().expect("临时目录");
    let path = write_config(directory.path(), &[0]);
    let server = start(&path).await;
    initialize_and_wait_leader(&server, 0).await;
    let aggregate_store = server.aggregate_store_service();
    let watcher = server
        .spawn_config_watcher(path.clone())
        .expect("启动 watcher");

    write_config(directory.path(), &[0, 1]);
    wait_registered(&server, 1).await;
    initialize_and_wait_leader(&server, 1).await;
    assert_eq!(server.shard_manager().shard_ids().await, [0, 1]);

    let aggregate_type = AggregateTypeRef {
        business_space: "orders".into(),
        aggregate_type: "order".into(),
    };
    aggregate_store
        .register_aggregate_type(tonic::Request::new(RegisterAggregateTypeRequest {
            aggregate_type: Some(aggregate_type.clone()),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        }))
        .await
        .expect("热更新后注册聚合类型");
    let partitions = aggregate_store
        .list_aggregate_partitions(tonic::Request::new(ListAggregatePartitionsRequest {
            aggregate_type: Some(aggregate_type),
        }))
        .await
        .expect("读取聚合类型放置")
        .into_inner()
        .partitions;
    assert!(
        partitions.iter().any(|partition| partition.shard_id == 1),
        "热更新前创建的 AggregateStore module 必须使用新增 Shard"
    );

    watcher.stop().await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_hot_config_keeps_current_shards() {
    let directory = tempfile::tempdir().expect("临时目录");
    let path = write_config(directory.path(), &[0]);
    let server = start(&path).await;
    let watcher = server
        .spawn_config_watcher(path.clone())
        .expect("启动 watcher");

    std::fs::write(&path, "not [valid toml").expect("写非法配置");
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(server.shard_manager().shard_ids().await, [0]);

    watcher.stop().await;
    server.shutdown().await;
}
