#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::time::Duration;

use es_proto::eventstore::aggregate_store_server::AggregateStoreServer;
use es_server::Server;
use es_server::config::{Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig};
use eventfs_fuse::backend::{EventFsBackend, EventSet, GrpcBackend};
use eventfs_fuse::codec::{EventEnvelope, ExpectedVersion, Settlement, SettlementAction};
use eventfs_fuse::fuse::{EventFs, MountIdentity};
use uuid::Uuid;

async fn wait_shard_leader(server: &Server, shard_id: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("读取测试 shard");
        if shard.raft.metrics().borrow().state.is_leader() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等待 shard {shard_id} leader 超时"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn start_server() -> (
    String,
    tokio::task::JoinHandle<()>,
    Server,
    tempfile::TempDir,
) {
    let data_dir = tempfile::tempdir().expect("创建服务端临时目录");
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: None,
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: data_dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: vec![0, 1],
                replica: vec![],
            }],
        },
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let server = Server::new(config.clone()).expect("创建测试服务端");
    server.init().await.expect("初始化测试服务端");
    for shard_id in 0..2 {
        server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("读取测试 shard")
            .raft
            .initialize(BTreeSet::from([1]))
            .await
            .expect("初始化单节点 Raft");
        wait_shard_leader(&server, shard_id).await;
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定测试端口");
    let endpoint = format!("http://{}", listener.local_addr().expect("读取测试端口"));
    let service = es_server::aggregate_service::AggregateStoreService::new(
        server.shard_manager().clone(),
        &config,
    )
    .expect("创建 AggregateStore 服务");
    let task = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(AggregateStoreServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (endpoint, task, server, data_dir)
}

fn event(aggregate_id: &str, expected_version: ExpectedVersion) -> EventEnvelope {
    EventEnvelope {
        aggregate_id: aggregate_id.into(),
        event_type: "order.changed".into(),
        data: br#"{"amount":1}"#.to_vec(),
        metadata: br#"{"source":"test"}"#.to_vec(),
        event_id: Uuid::new_v4(),
        expected_version,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_backend_roundtrips_all_aggregate_store_operations() {
    let (endpoint, server_task, server, _data_dir) = start_server().await;
    let backend = GrpcBackend::connect(vec![endpoint], None)
        .await
        .expect("连接 AggregateStore");
    let capabilities = backend.capabilities().await.expect("协商服务端能力");
    assert!(capabilities.max_event_bytes > 0);
    assert!(EventSet::new("bad/path", "order").is_err());

    let event_set = EventSet::new("orders", "order").expect("事件集身份");
    assert!(
        backend
            .list_event_sets()
            .await
            .expect("空 catalog")
            .is_empty()
    );
    backend
        .create_event_set(&event_set, Uuid::new_v4())
        .await
        .expect("创建事件集");
    assert_eq!(
        backend.list_event_sets().await.expect("列事件集"),
        vec![event_set.clone()]
    );

    assert_eq!(
        backend
            .append(&event_set, &event("order-1", ExpectedVersion::NoAggregate))
            .await
            .expect("NoAggregate 追加"),
        0
    );
    assert_eq!(
        backend
            .append(&event_set, &event("order-1", ExpectedVersion::Exists))
            .await
            .expect("Exists 追加"),
        1
    );
    assert_eq!(
        backend
            .append(&event_set, &event("order-1", ExpectedVersion::Exact(1)))
            .await
            .expect("Exact 追加"),
        2
    );
    for aggregate_id in ["order-2", "order-3", "order-4"] {
        backend
            .append(&event_set, &event(aggregate_id, ExpectedVersion::Any))
            .await
            .expect("Any 追加");
    }

    let mut follow = backend.follow(&event_set).await.expect("跟随事件");
    let mut event_frames = 0;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), follow.recv())
            .await
            .expect("等待 follow frame")
            .expect("follow 未提前关闭")
            .expect("follow frame 成功");
        let value: serde_json::Value = serde_json::from_slice(&frame).expect("JSONL frame");
        match value["kind"].as_str() {
            Some("event") => event_frames += 1,
            Some("caught_up") => break,
            other => panic!("意外 follow frame: {other:?}"),
        }
    }
    assert_eq!(event_frames, 6);
    drop(follow);

    assert!(
        backend
            .list_states(&event_set)
            .await
            .expect("空状态列表")
            .is_empty()
    );
    assert!(
        backend
            .get_state(&event_set, "order-missing")
            .await
            .expect("读取不存在状态")
            .is_none()
    );
    let first_state = backend
        .put_state(&event_set, "order-1", None, br#"{"balance":3}"#.to_vec())
        .await
        .expect("首次状态提交");
    assert_eq!(first_state.revision, 0);
    let second_state = backend
        .put_state(
            &event_set,
            "order-1",
            Some(first_state.revision),
            br#"{"balance":2}"#.to_vec(),
        )
        .await
        .expect("状态 CAS 更新");
    assert_eq!(second_state.revision, 1);
    assert_eq!(
        backend
            .get_state(&event_set, "order-1")
            .await
            .expect("读取状态")
            .expect("状态存在"),
        second_state
    );
    assert_eq!(
        backend.list_states(&event_set).await.expect("列状态"),
        vec!["order-1"]
    );

    backend
        .create_group(&event_set, "workers", Uuid::new_v4())
        .await
        .expect("创建消费者组");
    assert_eq!(
        backend.list_groups(&event_set).await.expect("列消费者组"),
        vec!["workers"]
    );
    let fetched = backend
        .fetch_group(&event_set, "workers", "consumer-a")
        .await
        .expect("Fetch delivery");
    assert_eq!(fetched.deliveries.len(), 4);
    let delivery_ids = fetched
        .deliveries
        .iter()
        .map(|delivery| delivery.delivery_id.clone())
        .collect::<Vec<_>>();
    let renewed = backend
        .renew_group(&event_set, "workers", "consumer-a", delivery_ids.clone())
        .await
        .expect("续租 delivery");
    assert_eq!(renewed.results.len(), 4);
    let actions = [
        SettlementAction::Ack,
        SettlementAction::Retry,
        SettlementAction::Park,
        SettlementAction::Skip,
    ];
    let settlements = delivery_ids
        .into_iter()
        .zip(actions)
        .map(|(delivery_id, action)| Settlement {
            delivery_id,
            action,
            reason: "backend e2e".into(),
        })
        .collect::<Vec<_>>();
    let settled = backend
        .settle_group(&event_set, "workers", "consumer-a", &settlements)
        .await
        .expect("结算 delivery");
    assert_eq!(settled.results.len(), 4);

    server_task.abort();
    let _ = server_task.await;
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "需要 Linux /dev/fuse、fusermount3 与允许 FUSE mount 的执行环境"]
async fn real_mount_appends_follows_and_commits_state() {
    assert!(std::path::Path::new("/dev/fuse").exists(), "缺少 /dev/fuse");
    let (endpoint, server_task, server, _data_dir) = start_server().await;
    let backend = Arc::new(
        GrpcBackend::connect(vec![endpoint], None)
            .await
            .expect("连接 AggregateStore"),
    );
    let capabilities = backend.capabilities().await.expect("协商服务端能力");
    let filesystem = EventFs::new(
        backend,
        tokio::runtime::Handle::current(),
        capabilities,
        MountIdentity {
            // SAFETY: libc 进程身份查询没有前置条件。
            uid: unsafe { libc::geteuid() },
            // SAFETY: libc 进程身份查询没有前置条件。
            gid: unsafe { libc::getegid() },
        },
    );
    let mount_dir = tempfile::tempdir().expect("创建挂载目录");
    let mut fuse_config = fuser::Config::default();
    fuse_config.mount_options.extend([
        fuser::MountOption::FSName("eventfs-test".into()),
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::RW,
    ]);
    let session =
        fuser::spawn_mount(filesystem, mount_dir.path(), &fuse_config).expect("挂载 eventfs-fuse");

    let event_set = mount_dir.path().join("orders/order");
    std::fs::create_dir_all(&event_set).expect("通过 FUSE 创建事件集");
    let events_path = event_set.join("events.jsonl");
    let mut event_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&events_path)
        .expect("打开事件文件");
    event_file
        .write_all(
            br#"{"spec_version":"1.0","aggregate_id":"order-1","event_type":"created","data":{"amount":100},"expected_version":{"kind":"no_aggregate"}}"#,
        )
        .expect("写事件");
    event_file.sync_all().expect("提交事件");
    drop(event_file);

    let events = std::fs::File::open(&events_path).expect("打开事件跟随流");
    let mut poll_fd = libc::pollfd {
        fd: events.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: poll_fd 指向一个有效元素，文件在 poll 调用期间保持打开。
    let ready = unsafe { libc::poll(&mut poll_fd, 1, 5_000) };
    assert_eq!(ready, 1, "事件跟随流在 5 秒内不可读");
    let mut line = String::new();
    BufReader::new(events)
        .read_line(&mut line)
        .expect("读取事件 frame");
    let frame: serde_json::Value = serde_json::from_str(&line).expect("事件 frame 是 JSON");
    assert_eq!(frame["kind"], "event");
    assert_eq!(frame["aggregate_id"], "order-1");
    assert_eq!(frame["data"]["amount"], 100);

    let state_path = event_set.join("states/order-1.json");
    let mut state = std::fs::File::create(&state_path).expect("创建状态文件");
    state.write_all(br#"{"balance":100}"#).expect("写状态");
    state.sync_all().expect("CAS 提交状态");
    drop(state);
    assert_eq!(
        std::fs::read_to_string(&state_path).expect("读回状态"),
        r#"{"balance":100}"#
    );

    session.umount_and_join().expect("卸载 eventfs-fuse");
    server_task.abort();
    server.shutdown().await;
}
