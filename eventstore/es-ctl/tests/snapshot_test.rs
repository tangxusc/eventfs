//! esctl snapshot 端到端：进程内 2 分片写数据 → 造快照 → 停服（释放 LOCK）→
//! esctl snapshot list/restore 子进程 → 重启验证时间点恢复。

use std::collections::BTreeSet;
use std::process::{Command, Output};
use std::time::Duration;

use es_proto::eventstore::event_store_server::EventStoreServer;
use es_server::Server;
use es_server::config::{Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig};

/// 启动测试服务器（单节点、2 分片）。
/// `init`: true 时每分片单成员自举；false 用于 restore 后的重启
/// （vote 保留，节点以快照点直接恢复领导，无需也不能重新 init）。
async fn start_server(
    data_dir: std::path::PathBuf,
    init: bool,
) -> (String, tokio::task::JoinHandle<()>, Server) {
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir,
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
    let server = Server::new(config.clone()).expect("创建服务器");
    server.init().await.expect("初始化");

    if init {
        let members = BTreeSet::from([1u64]);
        for shard_id in 0..2 {
            let shard = server
                .shard_manager()
                .get_shard(shard_id)
                .await
                .expect("取分片");
            shard
                .raft
                .initialize(members.clone())
                .await
                .expect("初始化 raft");
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().expect("取地址"));
    let sm = server.shard_manager().clone();
    // 共享 server 的路由表实例（EsService::new 会自建独立实例，内存态不同步）
    let route_table = server.route_table().clone();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(
                es_server::service::EsService::with_limits(sm, Default::default(), route_table, &config)
                    .expect("创建服务"),
            ))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, handle, server)
}

/// 停服并释放 LOCK（逐分片关存储，共享 tree 最后一个 close 释放锁文件）
async fn stop_and_release_lock(server: &Server, num_shards: u64) {
    for shard_id in 0..num_shards {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("取分片");
        shard.storage.close().await.expect("关闭存储");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
}

/// 运行 esctl（离线命令，不带 --endpoints）
fn esctl_offline(args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(args);
    cmd.output().expect("运行 esctl")
}

/// 运行 esctl（在线命令）
fn esctl(endpoints: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(["--endpoints", endpoints]);
    cmd.args(args);
    cmd.output().expect("运行 esctl")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

/// 等待快照建成（trigger_snapshot 只发命令，build 在 sm worker 异步执行）
async fn wait_snapshot(store: &es_storage::snapshot::SnapshotStore) -> std::path::PathBuf {
    for _ in 0..100 {
        if let Some(p) = store.latest(None).expect("读快照目录") {
            return p;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("快照未在预期时间内建成");
}

/// 往分片 0 写 n 条事件
async fn append_n(addr: &str, stream: &str, n: u64) {
    for i in 0..n {
        let out = esctl(
            addr,
            &[
                "append",
                stream,
                "--event-type",
                "E",
                "--data",
                &format!("data-{i}"),
            ],
        );
        assert!(out.status.success(), "append 失败: {}", stderr(&out));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_list_shows_metadata() {
    let dir = tempfile::tempdir().expect("临时目录");
    let (addr, _handle, server) = start_server(dir.path().to_path_buf(), true).await;

    // 写数据并造快照（s1 路由到的分片）
    append_n(&addr, "s1", 3).await;
    // 显式分配下 s1 的归属由路由表决定（hash 路由已废弃）
    let sid = server.route_table().lookup("s1").await.expect("s1 应已分配");
    // 通过 raft 真实路径触发建快照（build 需要 &mut self，经 openraft 触发）
    let shard = server.shard_manager().get_shard(sid).await.expect("取分片");
    shard.raft.trigger().snapshot().await.expect("触发建快照");
    let snap_path = wait_snapshot(shard.storage.snapshot_store()).await;
    assert!(snap_path.exists(), "快照文件应存在");

    // 列表输出含分片/快照点/压缩算法/大小
    let out = esctl_offline(&["snapshot", "list", dir.path().to_str().unwrap()]);
    assert!(out.status.success(), "list 失败: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains(&format!("shard={sid}")), "应列出分片 {sid}: {text}");
    assert!(text.contains("index="), "应列出 index: {text}");
    assert!(text.contains("zstd"), "默认压缩应为 zstd: {text}");

    // table 模式
    let out = esctl_offline(&[
        "-w",
        "table",
        "snapshot",
        "list",
        dir.path().to_str().unwrap(),
    ]);
    assert!(out.status.success(), "table 失败: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("SNAPSHOT_ID"), "table 应有表头: {text}");
    assert!(text.contains("snap-"), "table 应列出文件名: {text}");

    // json 模式
    let out = esctl_offline(&[
        "-w",
        "json",
        "snapshot",
        "list",
        dir.path().to_str().unwrap(),
    ]);
    assert!(out.status.success());
    let text = stdout(&out);
    assert!(text.contains("\"snapshots\""), "json 应有根字段: {text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_list_marks_corrupted_and_empty() {
    let dir = tempfile::tempdir().expect("临时目录");
    let (addr, _handle, server) = start_server(dir.path().to_path_buf(), true).await;

    // 造一个合法快照 + 一个损坏文件
    append_n(&addr, "s1", 1).await;
    // 显式分配下 s1 的归属由路由表决定（hash 路由已废弃）
    let sid = server.route_table().lookup("s1").await.expect("s1 应已分配");
    let shard = server.shard_manager().get_shard(sid).await.expect("取分片");
    shard.raft.trigger().snapshot().await.expect("建快照");
    let snap_dir = dir.path().join("snapshots");
    std::fs::write(
        snap_dir.join("snap-00000000-00000000000000000099-00000000000000000099.esnap"),
        b"corrupted",
    )
    .expect("写坏文件");

    let out = esctl_offline(&[
        "-w",
        "table",
        "snapshot",
        "list",
        dir.path().to_str().unwrap(),
    ]);
    assert!(out.status.success());
    let text = stdout(&out);
    assert!(text.contains("损坏"), "损坏文件应被标记: {text}");
    assert!(text.contains("snap-00000000-00000000000000000099"), "损坏文件应列出");

    // 空目录（无快照）输出"（无快照）"
    let empty = tempfile::tempdir().expect("临时目录");
    let (addr2, _h2, srv2) = start_server(empty.path().to_path_buf(), true).await;
    let _ = addr2;
    let _ = srv2;
    let out = esctl_offline(&["snapshot", "list", empty.path().to_str().unwrap()]);
    assert!(out.status.success());
    assert!(stdout(&out).contains("无快照"), "空目录应提示无快照");

    // 不存在的目录报错
    let out = esctl_offline(&["snapshot", "list", "/nonexistent-snap-dir"]);
    assert!(!out.status.success(), "目录不存在应报错");
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_restore_point_in_time_and_resume() {
    let dir = tempfile::tempdir().expect("临时目录");
    let data_dir = dir.path().to_path_buf();
    let (addr, handle, server) = start_server(data_dir.clone(), true).await;

    // 写 5 条 → 造快照（快照点）→ 再写 3 条（快照点之后的数据）
    append_n(&addr, "s1", 5).await;
    // 显式分配下 s1 的归属由路由表决定（hash 路由已废弃）
    let sid = server.route_table().lookup("s1").await.expect("s1 应已分配");
    let shard = server.shard_manager().get_shard(sid).await.expect("取分片");
    shard.raft.trigger().snapshot().await.expect("触发建快照");
    let snap_path = wait_snapshot(shard.storage.snapshot_store()).await;
    append_n(&addr, "s1", 3).await;

    // 停服释放 LOCK 后离线恢复
    handle.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop_and_release_lock(&server, 2).await;

    let out = esctl_offline(&[
        "snapshot",
        "restore",
        data_dir.to_str().unwrap(),
        snap_path.to_str().unwrap(),
        "--yes",
    ]);
    assert!(out.status.success(), "restore 失败: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("恢复完成"), "restore 应报告完成: {text}");
    assert!(text.contains("5 条事件"), "应恢复 5 条事件: {text}");

    // 重启 server（同一数据目录）：vote 保留，节点以快照点直接恢复领导，
    // 无需（也不能）重新 initialize
    let (addr2, _handle2, _server2) = start_server(data_dir.clone(), false).await;
    // 等节点恢复领导
    tokio::time::sleep(Duration::from_millis(300)).await;

    // 数据回到快照点：只有 5 条
    let out = esctl(&addr2, &["read", "s1"]);
    assert!(out.status.success(), "读失败: {}", stderr(&out));
    let text = stdout(&out);
    let count = text.lines().count();
    assert_eq!(count, 5, "恢复到快照点应只有 5 条事件: {text}");

    // 快照点之后可继续写入（版本从 5 继续）
    let out = esctl(
        &addr2,
        &[
            "append",
            "s1",
            "--event-type",
            "E",
            "--data",
            "after-restore",
        ],
    );
    assert!(out.status.success(), "恢复后写入失败: {}", stderr(&out));
    let out = esctl(&addr2, &["read", "s1"]);
    assert_eq!(stdout(&out).lines().count(), 6, "恢复后应能继续追加");

    // snapshot list 可见恢复的快照文件
    let out = esctl_offline(&["snapshot", "list", data_dir.to_str().unwrap()]);
    let text = stdout(&out);
    assert!(text.contains(&format!("shard={sid}")), "恢复的快照应可列出: {text}");
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_restore_rejects_locked_and_invalid() {
    let dir = tempfile::tempdir().expect("临时目录");
    let (addr, _handle, server) = start_server(dir.path().to_path_buf(), true).await;

    // 造快照
    append_n(&addr, "s1", 1).await;
    // 显式分配下 s1 的归属由路由表决定（hash 路由已废弃）
    let sid = server.route_table().lookup("s1").await.expect("s1 应已分配");
    let shard = server.shard_manager().get_shard(sid).await.expect("取分片");
    shard.raft.trigger().snapshot().await.expect("触发建快照");
    let snap_path = wait_snapshot(shard.storage.snapshot_store()).await;

    // 1. 集群在线时 restore 必须被 LOCK 拦截
    let out = esctl_offline(&[
        "snapshot",
        "restore",
        dir.path().to_str().unwrap(),
        snap_path.to_str().unwrap(),
        "--yes",
    ]);
    assert!(!out.status.success(), "在线 restore 应报错");
    assert!(
        stderr(&out).contains("LOCK") || stderr(&out).contains("locked"),
        "错误应说明 LOCK 占用: {}",
        stderr(&out)
    );

    // 2. 损坏的快照文件被拒绝且数据目录未改动
    let bad = dir.path().join("bad.snap");
    std::fs::write(&bad, b"not a snapshot").expect("写坏文件");
    let out = esctl_offline(&[
        "snapshot",
        "restore",
        dir.path().to_str().unwrap(),
        bad.to_str().unwrap(),
        "--yes",
    ]);
    assert!(!out.status.success(), "坏快照应被拒绝");
    assert!(
        stderr(&out).contains("快照文件无效"),
        "错误应说明快照无效: {}",
        stderr(&out)
    );

    // 3. 快照文件不存在被拒绝
    let out = esctl_offline(&[
        "snapshot",
        "restore",
        dir.path().to_str().unwrap(),
        "/nonexistent.snap",
        "--yes",
    ]);
    assert!(!out.status.success());
    assert!(stderr(&out).contains("快照文件不存在"));

    // 数据仍可读（未被破坏）
    let out = esctl(&addr, &["read", "s1"]);
    assert!(out.status.success());
    assert_eq!(stdout(&out).lines().count(), 1, "失败路径不得改动数据");
}
