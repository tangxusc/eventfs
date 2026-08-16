//! esctl Aggregate-only 二进制端到端测试。

use std::collections::BTreeSet;
use std::io::Write;
use std::process::Stdio;
use std::process::{Command, Output};
use std::time::Duration;

use es_proto::eventstore::aggregate_store_server::AggregateStoreServer;
use es_proto::eventstore::raft_admin_server::RaftAdminServer;
use es_server::Server;
use es_server::config::{Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig};

async fn start_server() -> (
    String,
    tokio::task::JoinHandle<()>,
    Server,
    tempfile::TempDir,
) {
    let directory = tempfile::tempdir().expect("临时目录");
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: None,
            peers: Vec::new(),
        },
        storage: StorageConfig {
            data_dir: directory.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: vec![0, 1],
                replica: Vec::new(),
            }],
        },
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let server = Server::new(config.clone()).expect("创建服务器");
    server.init().await.expect("初始化服务器");
    for shard_id in [0, 1] {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("读取 Shard");
        shard
            .raft
            .initialize(BTreeSet::from([1]))
            .await
            .expect("初始化 Raft");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !shard.raft.metrics().borrow().state.is_leader() {
            assert!(tokio::time::Instant::now() < deadline, "等待 leader 超时");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let address = format!("http://{}", listener.local_addr().expect("读取端口"));
    let aggregate = es_server::aggregate_service::AggregateStoreService::new(
        server.shard_manager().clone(),
        &config,
    )
    .expect("创建 AggregateStore");
    let admin = es_raft::RaftAdminService::new(server.shard_manager().clone());
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AggregateStoreServer::new(aggregate))
            .add_service(RaftAdminServer::new(admin))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("gRPC 服务退出");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (address, task, server, directory)
}

fn esctl(endpoint: &str, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args(["--endpoints", endpoint, "-w", "json"])
        .args(arguments)
        .output()
        .expect("运行 esctl")
}

fn esctl_raw(endpoint: &str, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args(["--endpoints", endpoint])
        .args(arguments)
        .output()
        .expect("运行 esctl")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

async fn start_uninitialized(
    shard_count: u64,
) -> (
    String,
    tokio::task::JoinHandle<()>,
    Server,
    tempfile::TempDir,
) {
    let directory = tempfile::tempdir().expect("临时目录");
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: None,
            peers: Vec::new(),
        },
        storage: StorageConfig {
            data_dir: directory.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: (0..shard_count).collect(),
                replica: Vec::new(),
            }],
        },
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let server = Server::new(config.clone()).expect("创建服务器");
    server.init().await.expect("初始化服务器");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let address = format!("http://{}", listener.local_addr().expect("读取端口"));
    let aggregate = es_server::aggregate_service::AggregateStoreService::new(
        server.shard_manager().clone(),
        &config,
    )
    .expect("创建 AggregateStore");
    let admin = es_raft::RaftAdminService::new(server.shard_manager().clone());
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(AggregateStoreServer::new(aggregate))
            .add_service(RaftAdminServer::new(admin))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("gRPC 服务退出");
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    (address, task, server, directory)
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "esctl 失败：{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write_empty_snapshot(path: &std::path::Path, last_log: Option<(u64, u64)>) {
    let last_log_id = last_log.map(|(term, index)| {
        openraft::LogId::new(openraft::CommittedLeaderId::new(term, 1), index)
    });
    let metadata = openraft::SnapshotMeta {
        last_log_id,
        last_membership: openraft::StoredMembership::new(None, Default::default()),
        snapshot_id: "0-cli-e2e".into(),
    };
    let metadata_len = serde_json::to_vec(&metadata).expect("编码快照 meta").len() as u64;
    let mut file = std::fs::File::create(path).expect("创建快照文件");
    es_storage::snapshot::write_header(
        &mut file,
        &es_storage::snapshot::SnapshotHeader {
            version: es_storage::snapshot::SNAP_VERSION,
            compression: es_storage::snapshot::Compression::None,
            shard_id: 0,
            meta_len: metadata_len,
            payload_len: es_storage::snapshot::payload_len_for(&[]),
        },
        &metadata,
    )
    .expect("写快照头");
    let mut writer = es_storage::snapshot::Compression::None
        .writer(file)
        .expect("打开快照 payload");
    writer.write_all(&0u64.to_le_bytes()).expect("写记录数");
    es_storage::snapshot::write_end_marker(&mut writer).expect("写流尾");
    writer.finish().expect("完成快照");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_type_append_state_and_follow_roundtrip() {
    let (address, task, server, _directory) = start_server().await;

    let registered = esctl(
        &address,
        &["aggregate", "type", "register", "orders", "order"],
    );
    assert_success(&registered);
    assert!(String::from_utf8_lossy(&registered.stdout).contains("aggregate_type"));

    for arguments in [
        vec!["aggregate", "type", "list"],
        vec!["aggregate", "type", "get", "orders", "order"],
    ] {
        let output = esctl(&address, &arguments);
        assert_success(&output);
        assert!(String::from_utf8_lossy(&output.stdout).contains("order"));
    }

    let appended = esctl(
        &address,
        &[
            "aggregate",
            "append",
            "orders",
            "order",
            "order-1",
            "--event-type",
            "OrderOpened",
            "--data",
            "{}",
            "--expected-version",
            "no-aggregate",
        ],
    );
    assert_success(&appended);
    assert!(String::from_utf8_lossy(&appended.stdout).contains("aggregate_version"));

    let state = esctl(
        &address,
        &[
            "aggregate",
            "state",
            "put",
            "orders",
            "order",
            "order-1",
            "--data",
            r#"{"status":"open"}"#,
        ],
    );
    assert_success(&state);
    let state = esctl(
        &address,
        &["aggregate", "state", "get", "orders", "order", "order-1"],
    );
    assert_success(&state);
    assert!(String::from_utf8_lossy(&state.stdout).contains("open"));

    let follow = esctl(
        &address,
        &["aggregate", "follow", "orders", "order", "--once"],
    );
    assert_success(&follow);
    assert!(String::from_utf8_lossy(&follow.stdout).contains("order-1"));

    task.abort();
    let _ = task.await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_group_cursor_paging_and_settlement_roundtrip() {
    let (address, task, server, _directory) = start_server().await;
    assert_success(&esctl(
        &address,
        &["aggregate", "type", "register", "billing", "invoice"],
    ));
    for aggregate_id in ["invoice-1", "invoice-2"] {
        assert_success(&esctl(
            &address,
            &[
                "aggregate",
                "append",
                "billing",
                "invoice",
                aggregate_id,
                "--event-type",
                "InvoiceOpened",
                "--data",
                "{}",
                "--expected-version",
                "no-aggregate",
            ],
        ));
        assert_success(&esctl(
            &address,
            &[
                "aggregate",
                "state",
                "put",
                "billing",
                "invoice",
                aggregate_id,
                "--data",
                "{}",
            ],
        ));
    }

    let first_page = esctl(
        &address,
        &[
            "aggregate",
            "state",
            "list",
            "billing",
            "invoice",
            "--page-size",
            "1",
        ],
    );
    assert_success(&first_page);
    let first_json: serde_json::Value =
        serde_json::from_str(stdout(&first_page).trim()).expect("状态分页 JSON");
    let token = first_json["next_page_token"]
        .as_str()
        .expect("下一页 token")
        .to_string();
    assert_success(&esctl(
        &address,
        &[
            "aggregate",
            "state",
            "list",
            "billing",
            "invoice",
            "--page-size",
            "1",
            "--page-token",
            &token,
        ],
    ));

    let followed = esctl(
        &address,
        &["aggregate", "follow", "billing", "invoice", "--once"],
    );
    assert_success(&followed);
    let cursor = stdout(&followed)
        .lines()
        .last()
        .and_then(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .and_then(|value| value["cursor"].as_str().map(str::to_owned))
        .expect("caught-up cursor");
    assert_success(&esctl(
        &address,
        &[
            "aggregate",
            "follow",
            "billing",
            "invoice",
            "--cursor",
            &cursor,
            "--once",
        ],
    ));
    assert_success(&esctl(
        &address,
        &[
            "aggregate",
            "follow",
            "billing",
            "invoice",
            "--now",
            "--once",
        ],
    ));

    for action in ["ack", "retry", "park", "skip"] {
        let group = format!("workers-{action}");
        assert_success(&esctl(
            &address,
            &[
                "aggregate",
                "group",
                "create",
                "billing",
                "invoice",
                &group,
                "--ack-timeout-ms",
                "2000",
            ],
        ));
        let fetched = esctl(
            &address,
            &[
                "aggregate",
                "group",
                "fetch",
                "billing",
                "invoice",
                &group,
                "--consumer",
                "consumer-a",
                "--max-events",
                "1",
                "--wait-ms",
                "0",
            ],
        );
        assert_success(&fetched);
        let value: serde_json::Value =
            serde_json::from_str(stdout(&fetched).trim()).expect("Fetch JSON");
        let delivery = value["deliveries"][0]["delivery_id"]
            .as_str()
            .expect("delivery token");
        assert_success(&esctl(
            &address,
            &[
                "aggregate",
                "group",
                "settle",
                "billing",
                "invoice",
                &group,
                "--consumer",
                "consumer-a",
                "--delivery",
                delivery,
                "--action",
                action,
                "--reason",
                "coverage",
            ],
        ));
    }

    assert_success(&esctl(
        &address,
        &[
            "aggregate",
            "group",
            "create",
            "billing",
            "invoice",
            "reset-workers",
            "--now",
        ],
    ));
    for (revision, reset) in [("1", "--reset-beginning"), ("2", "--reset-now")] {
        assert_success(&esctl(
            &address,
            &[
                "aggregate",
                "group",
                "update",
                "billing",
                "invoice",
                "reset-workers",
                "--expected-revision",
                revision,
                reset,
            ],
        ));
    }
    assert_success(&esctl(
        &address,
        &["aggregate", "group", "list", "billing", "invoice"],
    ));
    assert_success(&esctl(
        &address,
        &[
            "aggregate",
            "group",
            "delete",
            "billing",
            "invoice",
            "reset-workers",
            "--expected-revision",
            "3",
        ],
    ));

    task.abort();
    let _ = task.await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_admin_formats_init_and_failure_paths() {
    let (address, task, server, _directory) = start_server().await;
    for format in ["simple", "table", "json"] {
        let status = esctl_raw(&address, &["-w", format, "status"]);
        assert_success(&status);
        let members = esctl_raw(&address, &["-w", format, "member", "list"]);
        assert_success(&members);
    }

    for (format, shard, member) in [
        ("simple", "0", "2@127.0.0.1:59992"),
        ("table", "1", "3@127.0.0.1:59993"),
        ("json", "0", "4@127.0.0.1:59994"),
    ] {
        let output = esctl_raw(
            &address,
            &[
                "-w",
                format,
                "member",
                "add",
                "--shard",
                shard,
                "--member",
                member,
                "--learner-only",
                "--no-blocking",
            ],
        );
        assert_success(&output);
    }
    let all_shards = esctl_raw(
        &address,
        &[
            "member",
            "add",
            "--all-shards",
            "--member",
            "5@127.0.0.1:59995",
            "--learner-only",
            "--no-blocking",
        ],
    );
    assert_success(&all_shards);
    let invalid_remove = esctl_raw(
        &address,
        &["member", "remove", "--shard", "0", "--node-id", "2"],
    );
    assert_eq!(invalid_remove.status.code(), Some(1));
    assert!(stderr(&invalid_remove).contains("不在其中"));

    let down = "http://127.0.0.1:59999";
    assert_eq!(esctl_raw(down, &["status"]).status.code(), Some(1));
    assert_eq!(esctl_raw(down, &["member", "list"]).status.code(), Some(1));

    task.abort();
    let _ = task.await;
    server.shutdown().await;

    let (address, task, server, _directory) = start_uninitialized(3).await;
    for format in ["simple", "table", "json"] {
        let uninitialized_members = esctl_raw(&address, &["-w", format, "member", "list"]);
        assert_success(&uninitialized_members);
    }
    for format in ["simple", "table", "json"] {
        let uninitialized_status = esctl_raw(&address, &["-w", format, "status"]);
        assert_success(&uninitialized_status);
    }
    let mixed_endpoints = format!("{address},http://127.0.0.1:59999");
    let mixed_status = esctl_raw(&mixed_endpoints, &["-w", "table", "status"]);
    assert_success(&mixed_status);
    assert!(stdout(&mixed_status).contains("no"));
    for (format, shard) in [("simple", "0"), ("table", "1"), ("json", "2")] {
        let output = esctl_raw(
            &address,
            &[
                "-w",
                format,
                "init",
                "--shard",
                shard,
                "--member",
                "1@127.0.0.1:50051",
            ],
        );
        assert_success(&output);
    }
    let repeated = esctl_raw(
        &address,
        &["init", "--shard", "0", "--member", "1@127.0.0.1:50051"],
    );
    assert_eq!(repeated.status.code(), Some(1));
    let all = esctl_raw(
        &address,
        &["init", "--all-shards", "--member", "1@127.0.0.1:50051"],
    );
    assert_eq!(all.status.code(), Some(1));

    task.abort();
    let _ = task.await;
    server.shutdown().await;
}

#[test]
fn snapshot_cli_lists_empty_and_corrupted_directories() {
    let empty = tempfile::tempdir().expect("空数据目录");
    let snapshots = empty.path().join("snapshots");
    std::fs::create_dir_all(&snapshots).expect("创建快照目录");
    for format in ["simple", "table", "json"] {
        let output = Command::new(env!("CARGO_BIN_EXE_esctl"))
            .args([
                "-w",
                format,
                "snapshot",
                "list",
                empty.path().to_str().expect("UTF-8 path"),
            ])
            .output()
            .expect("运行 snapshot list");
        assert_success(&output);
    }

    let valid = snapshots.join("snap-00000000-00000000000000000001-00000000000000000001.esnap");
    write_empty_snapshot(&valid, Some((1, 1)));
    write_empty_snapshot(
        &snapshots.join("snap-00000000-00000000000000000000-00000000000000000000.esnap"),
        None,
    );
    std::fs::write(
        snapshots.join("snap-00000000-00000000000000000001-00000000000000000001.esnap"),
        b"corrupted",
    )
    .expect("写损坏快照");
    let valid = snapshots.join("snap-00000000-00000000000000000002-00000000000000000002.esnap");
    write_empty_snapshot(&valid, Some((2, 2)));
    for format in ["simple", "table", "json"] {
        let listed = Command::new(env!("CARGO_BIN_EXE_esctl"))
            .args([
                "-w",
                format,
                "snapshot",
                "list",
                empty.path().to_str().expect("UTF-8 path"),
            ])
            .output()
            .expect("列有效与损坏快照");
        assert_success(&listed);
        assert!(stdout(&listed).contains("snap-00000000"));
    }

    for format in ["simple", "table", "json"] {
        let restore_data = tempfile::tempdir().expect("恢复数据目录");
        let restored = Command::new(env!("CARGO_BIN_EXE_esctl"))
            .args([
                "-w",
                format,
                "snapshot",
                "restore",
                restore_data.path().to_str().expect("UTF-8 path"),
                valid.to_str().expect("UTF-8 snapshot path"),
                "--yes",
            ])
            .output()
            .expect("恢复有效快照");
        assert_success(&restored);
    }

    let cancelled_data = tempfile::tempdir().expect("取消恢复数据目录");
    let mut child = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args([
            "snapshot",
            "restore",
            cancelled_data.path().to_str().expect("UTF-8 path"),
            valid.to_str().expect("UTF-8 snapshot path"),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("启动交互恢复");
    child
        .stdin
        .as_mut()
        .expect("恢复 stdin")
        .write_all(b"n\n")
        .expect("输入取消");
    assert_eq!(
        child
            .wait_with_output()
            .expect("等待取消恢复")
            .status
            .code(),
        Some(1)
    );

    let missing = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args(["snapshot", "list", "/path/that/does/not/exist"])
        .output()
        .expect("列不存在目录");
    assert_eq!(missing.status.code(), Some(1));
    let invalid_restore = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args([
            "snapshot",
            "restore",
            empty.path().to_str().expect("UTF-8 path"),
            "/path/that/does/not/exist.snap",
            "--yes",
        ])
        .output()
        .expect("恢复不存在快照");
    assert_eq!(invalid_restore.status.code(), Some(1));
}
