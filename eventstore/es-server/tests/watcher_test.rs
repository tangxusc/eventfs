//! watcher 集成测试：配置热更新 → 运行期动态创建新 shards（不重启）。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use es_server::Server;

/// 写配置到临时目录（2 shards）
fn write_config(dir: &std::path::Path, shards: &[u64]) -> PathBuf {
    let path = dir.join("config.toml");
    let primary = shards
        .iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let content = format!(
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
        dir.to_str().unwrap()
    );
    std::fs::write(&path, content).expect("写配置");
    path
}

/// 轮询等待某 shard 注册（超时失败）
async fn wait_shard_registered(server: &Arc<Server>, shard_id: u64, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let ids = server.shard_manager().shard_ids().await;
        if ids.contains(&shard_id) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待 shard {shard_id} 注册超时（当前: {ids:?}）"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// 等待单节点 Shard 完成选举，避免把 initialize 返回误当作 leader 已就绪。
async fn wait_shard_leader(server: &Arc<Server>, shard_id: u64, timeout: Duration) {
    let shard = server
        .shard_manager()
        .get_shard(shard_id)
        .await
        .expect("取 shard");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if shard.raft.metrics().borrow().state.is_leader() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待 shard {shard_id} leader 超时"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// 配置热更新：运行中把 2 shards 扩到 4 shards → watcher 动态创建 2/3。
#[tokio::test(flavor = "multi_thread")]
async fn hot_config_adds_shards_dynamically() {
    let dir = tempfile::tempdir().expect("临时目录");
    let config_path = write_config(dir.path(), &[0, 1]);

    // 启动服务器（watcher 由测试直接 spawn，与 main.rs 装配一致）
    let content = std::fs::read_to_string(&config_path).expect("读配置");
    let config: es_server::Config = toml::from_str(&content).expect("解析配置");
    let server = Arc::new(Server::new(config).expect("创建服务器"));
    server.init().await.expect("初始化");
    let mut initial = server.shard_manager().shard_ids().await;
    initial.sort_unstable();
    assert_eq!(initial, vec![0, 1]);

    let watcher = es_server::watcher::spawn(
        config_path.clone(),
        es_server::route_table::routes_path(&server.config().storage.data_dir),
        server.route_table().clone(),
        server.ownership().clone(),
        server.shard_manager().clone(),
        server.config().node.id,
    )
    .expect("watcher 启动");

    // 热更新配置：新增 shard 2/3（模拟运维扩容）
    write_config(dir.path(), &[0, 1, 2, 3]);

    // watcher 检测到变更 → 动态创建并注册
    wait_shard_registered(&server, 2, Duration::from_secs(10)).await;
    wait_shard_registered(&server, 3, Duration::from_secs(10)).await;

    let mut ids = server.shard_manager().shard_ids().await;
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 3], "应注册全部 4 个 shards");

    // 新 shard 可被 raft 初始化（数据面路径打通）
    for id in [2, 3] {
        let shard = server
            .shard_manager()
            .get_shard(id)
            .await
            .expect("取新 shard");
        shard
            .raft
            .initialize(std::collections::BTreeSet::from([1u64]))
            .await
            .expect("初始化新 shard");
    }

    watcher.stop().await;
}

/// 非法配置热更新：fail-soft（保留旧配置，服务不受影响）。
#[tokio::test(flavor = "multi_thread")]
async fn hot_config_invalid_keeps_old_state() {
    let dir = tempfile::tempdir().expect("临时目录");
    let config_path = write_config(dir.path(), &[0, 1]);

    let content = std::fs::read_to_string(&config_path).expect("读配置");
    let config: es_server::Config = toml::from_str(&content).expect("解析配置");
    let server = Arc::new(Server::new(config).expect("创建服务器"));
    server.init().await.expect("初始化");

    let watcher = es_server::watcher::spawn(
        config_path.clone(),
        es_server::route_table::routes_path(&server.config().storage.data_dir),
        server.route_table().clone(),
        server.ownership().clone(),
        server.shard_manager().clone(),
        server.config().node.id,
    )
    .expect("watcher 启动");

    // 写入非法配置（primary 分区重叠：shard 0 出现在两个节点 → validate 拒绝）
    let bad = format!(
        r#"
[node]
id = 1
listen_addr = "127.0.0.1:0"
peers = []

[storage]
data_dir = "{}"

[placement]
replication_factor = 2

[[placement.nodes]]
id = 1
primary = [0, 1]
replica = []

[[placement.nodes]]
id = 2
primary = [0]
replica = []
"#,
        dir.path().to_str().unwrap()
    );
    std::fs::write(&config_path, bad).expect("写坏配置");

    // fail-soft：等 watcher 处理窗口，确认旧 shards 不受影响（不新增不删除）
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut ids = server.shard_manager().shard_ids().await;
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1], "非法配置不应改变已注册 shards: {ids:?}");

    watcher.stop().await;
}

/// 配置缩容只收紧新流的分配范围，不能删除本地已有 shard 或数据。
#[tokio::test(flavor = "multi_thread")]
async fn hot_config_removal_keeps_existing_shards_and_updates_route_pool() {
    let dir = tempfile::tempdir().expect("临时目录");
    let config_path = write_config(dir.path(), &[0, 1]);
    let content = std::fs::read_to_string(&config_path).expect("读配置");
    let config: es_server::Config = toml::from_str(&content).expect("解析配置");
    let server = Arc::new(Server::new(config).expect("创建服务器"));
    server.init().await.expect("初始化");

    let watcher = es_server::watcher::spawn(
        config_path.clone(),
        es_server::route_table::routes_path(&server.config().storage.data_dir),
        server.route_table().clone(),
        server.ownership().clone(),
        server.shard_manager().clone(),
        server.config().node.id,
    )
    .expect("watcher 启动");

    // 移除 shard 1 后，已注册 shard 必须保留，避免把历史数据随配置变更删除。
    write_config(dir.path(), &[0]);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut ids = server.shard_manager().shard_ids().await;
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1], "热更新不得移除已注册 shard");

    let (shard, inserted) = server
        .route_table()
        .allocate("created-after-shard-removal")
        .await
        .expect("缩容后分配新流");
    assert!(inserted, "新流应写入路由表");
    assert_eq!(shard, 0, "缩容后新流不得再分配给已移除的 shard");

    watcher.stop().await;
}

/// 路由表文件是兼容投影：运行时篡改不能覆盖权威归属，且会被恢复。
#[tokio::test(flavor = "multi_thread")]
async fn hot_routes_file_cannot_override_authoritative_ownership() {
    let dir = tempfile::tempdir().expect("临时目录");
    let config_path = write_config(dir.path(), &[0, 1]);
    let content = std::fs::read_to_string(&config_path).expect("读配置");
    let config: es_server::Config = toml::from_str(&content).expect("解析配置");
    let server = Arc::new(Server::new(config).expect("创建服务器"));
    server.init().await.expect("初始化");

    for shard_id in [0, 1] {
        server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("取 shard")
            .raft
            .initialize(std::collections::BTreeSet::from([1]))
            .await
            .expect("初始化单节点 shard");
        wait_shard_leader(&server, shard_id, Duration::from_secs(10)).await;
    }
    let canonical = server
        .ownership()
        .for_append("canonical-route")
        .await
        .expect("创建权威归属");

    let routes_path = es_server::route_table::routes_path(&server.config().storage.data_dir);
    // watcher 监听目录，但预先创建目标文件可避免不同平台对新建文件事件的差异。
    server
        .route_table()
        .ensure_file()
        .await
        .expect("创建空路由表文件");
    let watcher = es_server::watcher::spawn(
        config_path,
        routes_path.clone(),
        server.route_table().clone(),
        server.ownership().clone(),
        server.shard_manager().clone(),
        server.config().node.id,
    )
    .expect("watcher 启动");

    let authoritative = server.route_table().snapshot().await;
    let mut tampered = authoritative.clone();
    tampered.insert("hot-route", 1);
    std::fs::write(
        &routes_path,
        serde_json::to_vec(&tampered).expect("序列化路由表"),
    )
    .expect("写路由表");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let disk: es_core::route::RouteTable =
            serde_json::from_slice(&std::fs::read(&routes_path).expect("读取恢复后的投影"))
                .expect("解析恢复后的投影");
        if disk == authoritative {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "权威投影恢复超时");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(server.ownership().known("hot-route").await.is_none());
    assert_eq!(
        server
            .ownership()
            .known("canonical-route")
            .await
            .expect("原归属仍存在")
            .shard_id(),
        canonical.shard_id()
    );
    watcher.stop().await;
}
