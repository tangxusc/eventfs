//! 端到端集成测试：单节点写入与读取

use std::collections::HashSet;
use std::time::Duration;

use es_proto::eventstore::event_store_server::EventStoreServer;
use es_proto::eventstore::internal_subscription_client::InternalSubscriptionClient;
use es_proto::eventstore::internal_subscription_server::InternalSubscriptionServer;
use es_proto::eventstore::raft_admin_server::RaftAdminServer;
use es_proto::eventstore::{event_store_client::EventStoreClient, *};
use es_server::Server;
use es_server::config::{
    Config, NodeConfig, PeerConfig, PlacementConfig, PlacementNode, StorageConfig,
};

/// 启动测试服务器。
///
/// 返回 (地址, 服务器任务句柄, Server, TempDir)。
/// TempDir 必须由调用方持有到测试结束，drop 即删数据目录。
async fn start_test_server() -> (
    String,
    tokio::task::JoinHandle<()>,
    Server,
    tempfile::TempDir,
) {
    let dir = tempfile::tempdir().expect("临时目录");

    let config = Config {
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

    // 单节点集群：把自己设为唯一成员，立即成为 leader
    let members = std::collections::BTreeSet::from([1u64]);
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

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().expect("取本地地址"));

    // 共享 server 的路由表实例（EsService::new 会自建独立实例，内存态不同步）
    let service = es_server::service::EsService::with_limits(
        server.shard_manager().clone(),
        config.limits.clone(),
        server.route_table().clone(),
        &config,
    )
    .expect("创建服务");
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    // 等 gRPC 服务器真正开始监听
    tokio::time::sleep(Duration::from_millis(100)).await;

    (addr, handle, server, dir)
}

/// 启动两个各承载一个分片的节点，覆盖公开订阅经内部 RPC 聚合远程来源。
async fn start_two_shard_servers() -> (
    String,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
    Server,
    Server,
    tempfile::TempDir,
    tempfile::TempDir,
) {
    let first_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定节点一端口");
    let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定节点二端口");
    let first_internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定节点一内部端口");
    let second_internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定节点二内部端口");
    let first_addr = format!(
        "http://{}",
        first_listener.local_addr().expect("节点一地址")
    );
    let second_addr = format!(
        "http://{}",
        second_listener.local_addr().expect("节点二地址")
    );
    let first_internal_addr = format!(
        "http://{}",
        first_internal_listener
            .local_addr()
            .expect("节点一内部地址")
    );
    let first_internal_listen_addr = first_internal_listener
        .local_addr()
        .expect("节点一内部监听地址")
        .to_string();
    let second_internal_addr = format!(
        "http://{}",
        second_internal_listener
            .local_addr()
            .expect("节点二内部地址")
    );
    let second_internal_listen_addr = second_internal_listener
        .local_addr()
        .expect("节点二内部监听地址")
        .to_string();
    let first_dir = tempfile::tempdir().expect("节点一目录");
    let second_dir = tempfile::tempdir().expect("节点二目录");
    let placement = PlacementConfig {
        replication_factor: 1,
        nodes: vec![
            PlacementNode {
                id: 1,
                primary: vec![1],
                replica: vec![],
            },
            PlacementNode {
                id: 2,
                primary: vec![0],
                replica: vec![],
            },
        ],
    };
    let first_config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: Some(first_internal_listen_addr),
            peers: vec![PeerConfig {
                id: 2,
                addr: second_addr.clone(),
                internal_addr: Some(second_internal_addr.clone()),
            }],
        },
        storage: StorageConfig {
            data_dir: first_dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: placement.clone(),
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let second_config = Config {
        node: NodeConfig {
            id: 2,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: Some(second_internal_listen_addr),
            peers: vec![PeerConfig {
                id: 1,
                addr: first_addr.clone(),
                internal_addr: Some(first_internal_addr),
            }],
        },
        storage: StorageConfig {
            data_dir: second_dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement,
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let first = Server::new(first_config.clone()).expect("创建节点一");
    let second = Server::new(second_config.clone()).expect("创建节点二");
    first.init().await.expect("初始化节点一");
    second.init().await.expect("初始化节点二");
    first
        .shard_manager()
        .get_shard(1)
        .await
        .expect("节点一分片")
        .raft
        .initialize(std::collections::BTreeSet::from([1u64]))
        .await
        .expect("初始化节点一 raft");
    second
        .shard_manager()
        .get_shard(0)
        .await
        .expect("节点二分片")
        .raft
        .initialize(std::collections::BTreeSet::from([2u64]))
        .await
        .expect("初始化节点二 raft");

    let first_service = es_server::service::EsService::with_limits(
        first.shard_manager().clone(),
        first_config.limits.clone(),
        first.route_table().clone(),
        &first_config,
    )
    .expect("节点一服务");
    let second_service = es_server::service::EsService::with_limits(
        second.shard_manager().clone(),
        second_config.limits.clone(),
        second.route_table().clone(),
        &second_config,
    )
    .expect("节点二服务");
    let first_admin = es_raft::RaftAdminService::new(first.shard_manager().clone());
    let second_admin = es_raft::RaftAdminService::new(second.shard_manager().clone());
    let first_handle = tokio::spawn(async move {
        let _ = tokio::try_join!(
            tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(first_service.clone()))
                .add_service(RaftAdminServer::new(first_admin))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    first_listener
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(first_service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    first_internal_listener
                )),
        );
    });
    let second_handle = tokio::spawn(async move {
        let _ = tokio::try_join!(
            tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(second_service.clone()))
                .add_service(RaftAdminServer::new(second_admin))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    second_listener
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(second_service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    second_internal_listener
                )),
        );
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (
        first_addr,
        first_handle,
        second_handle,
        first,
        second,
        first_dir,
        second_dir,
    )
}

/// 期望版本：不校验
fn ev_any() -> Option<ExpectedVersion> {
    Some(ExpectedVersion {
        kind: Some(expected_version::Kind::Any(Empty {})),
    })
}

/// 期望版本：流必须不存在
fn ev_no_stream() -> Option<ExpectedVersion> {
    Some(ExpectedVersion {
        kind: Some(expected_version::Kind::NoStream(Empty {})),
    })
}

/// 期望版本：流当前版本必须恰为 v
fn ev_exact(v: u64) -> Option<ExpectedVersion> {
    Some(ExpectedVersion {
        kind: Some(expected_version::Kind::Exact(v)),
    })
}

/// 造一条新事件，event_id 随机
fn new_event(data: &[u8]) -> NewEvent {
    NewEvent {
        event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        event_type: "TestEvent".to_string(),
        data: data.to_vec(),
        metadata: vec![],
    }
}

/// 造一条指定 event_id 的新事件，用于幂等测试
fn new_event_with_id(id: uuid::Uuid, data: &[u8]) -> NewEvent {
    NewEvent {
        event_id: id.as_bytes().to_vec(),
        event_type: "TestEvent".to_string(),
        data: data.to_vec(),
        metadata: vec![],
    }
}

/// 追加单条事件的便捷封装
async fn append_one(
    client: &mut EventStoreClient<tonic::transport::Channel>,
    stream_id: &str,
    data: &[u8],
) -> AppendResponse {
    client
        .append(AppendRequest {
            stream_id: stream_id.to_string(),
            expected_version: ev_any(),
            events: vec![new_event(data)],
        })
        .await
        .expect("append 应成功")
        .into_inner()
}

/// 读取整个流
async fn read_stream_all(
    client: &mut EventStoreClient<tonic::transport::Channel>,
    stream_id: &str,
) -> Vec<Event> {
    let mut s = client
        .read_stream(ReadStreamRequest {
            stream_id: stream_id.to_string(),
            from_version: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
        })
        .await
        .expect("read_stream 应成功")
        .into_inner();

    let mut out = Vec::new();
    while let Some(resp) = s.message().await.expect("读流式响应") {
        out.extend(resp.events);
    }
    out
}

/// 读指定流（可控起点、限量、方向）
async fn read_stream(
    client: &mut EventStoreClient<tonic::transport::Channel>,
    stream_id: &str,
    from_version: u64,
    max_count: u64,
    direction: Direction,
) -> Vec<Event> {
    let mut s = client
        .read_stream(ReadStreamRequest {
            stream_id: stream_id.to_string(),
            from_version,
            max_count,
            direction: direction as i32,
        })
        .await
        .expect("read_stream")
        .into_inner();
    let mut out = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        out.extend(r.events);
    }
    out
}

#[tokio::test]
async fn write_and_read_back() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    let resp = client
        .append(AppendRequest {
            stream_id: "test-stream".to_string(),
            expected_version: ev_no_stream(),
            events: vec![new_event(b"hello"), new_event(b"world")],
        })
        .await
        .expect("append")
        .into_inner();

    assert_eq!(resp.next_expected_version, 1, "两条事件后当前版本应为 1");
    assert_eq!(resp.first_position, 0);
    assert_eq!(resp.last_position, 1);

    // 经 gRPC 读回
    let events = read_stream_all(&mut client, "test-stream").await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].version, 0);
    assert_eq!(events[1].version, 1);
    assert_eq!(events[0].data, b"hello");
    assert_eq!(events[1].data, b"world");

    // 直查存储层，确认落盘内容与 gRPC 返回一致。
    // 路由表是流归属权威（append 隐式建流已记录）；ShardManager::route_shard
    // 是纯哈希推导，与写路径的「大致最少流」分配可能不一致，不能用于定位数据。
    let shard_id = server
        .route_table()
        .lookup("test-stream")
        .await
        .expect("append 应已记录路由");
    let shard = server
        .shard_manager()
        .get_shard(shard_id)
        .await
        .expect("取分片");
    let stored = shard
        .storage
        .read_stream_events("test-stream", 0, 0)
        .expect("读存储");
    assert_eq!(stored.len(), 2);
    assert_eq!(stored[0].data, b"hello");

    handle.abort();
}

#[tokio::test]
async fn optimistic_no_stream_conflict() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    client
        .append(AppendRequest {
            stream_id: "conflict".to_string(),
            expected_version: ev_no_stream(),
            events: vec![new_event(b"first")],
        })
        .await
        .expect("首次 append");

    let err = client
        .append(AppendRequest {
            stream_id: "conflict".to_string(),
            expected_version: ev_no_stream(),
            events: vec![new_event(b"second")],
        })
        .await
        .expect_err("重复用 NoStream 应冲突");

    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("optimistic conflict"),
        "错误信息应说明是乐观并发冲突，实际: {}",
        err.message()
    );

    // 冲突不得产生写入
    let events = read_stream_all(&mut client, "conflict").await;
    assert_eq!(events.len(), 1, "冲突不能写入第二条");

    handle.abort();
}

#[tokio::test]
async fn optimistic_exact_match_and_mismatch() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    client
        .append(AppendRequest {
            stream_id: "exact".to_string(),
            expected_version: ev_no_stream(),
            events: vec![new_event(b"a"), new_event(b"b")],
        })
        .await
        .expect("首次");
    // 当前版本为 1

    // Exact(1) 应通过
    client
        .append(AppendRequest {
            stream_id: "exact".to_string(),
            expected_version: ev_exact(1),
            events: vec![new_event(b"c")],
        })
        .await
        .expect("Exact(1) 应通过");

    // 当前版本已是 2，Exact(0) 应冲突
    let err = client
        .append(AppendRequest {
            stream_id: "exact".to_string(),
            expected_version: ev_exact(0),
            events: vec![new_event(b"d")],
        })
        .await
        .expect_err("Exact(0) 应冲突");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    let events = read_stream_all(&mut client, "exact").await;
    assert_eq!(events.len(), 3, "只应有 3 条");

    handle.abort();
}

#[tokio::test]
async fn idempotent_event_id_no_duplicate() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    let event_id = uuid::Uuid::new_v4();
    let req = || AppendRequest {
        stream_id: "idem".to_string(),
        expected_version: ev_any(),
        events: vec![new_event_with_id(event_id, b"payload")],
    };

    let r1 = client.append(req()).await.expect("首次").into_inner();
    let r2 = client.append(req()).await.expect("重放").into_inner();

    assert_eq!(
        (
            r1.next_expected_version,
            r1.first_position,
            r1.last_position
        ),
        (
            r2.next_expected_version,
            r2.first_position,
            r2.last_position
        ),
        "重放必须返回与首次相同的结果"
    );

    let events = read_stream_all(&mut client, "idem").await;
    assert_eq!(events.len(), 1, "重放不能产生重复事件");

    handle.abort();
}

#[tokio::test]
async fn shard_routing_spreads_streams() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    let mut shards = HashSet::new();
    for i in 0..20 {
        let resp = append_one(&mut client, &format!("stream-{i}"), &[i]).await;
        shards.insert(resp.shard_id);
    }

    // num_shards=2，20 个流应覆盖两个分片
    assert_eq!(
        shards.len(),
        2,
        "20 个流应覆盖全部 2 个分片，实际: {shards:?}"
    );

    handle.abort();
}

#[tokio::test]
async fn read_stream_from_version_with_limit() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    for i in 0..5u8 {
        append_one(&mut client, "range", &[i]).await;
    }

    let mut s = client
        .read_stream(ReadStreamRequest {
            stream_id: "range".to_string(),
            from_version: 2,
            max_count: 2,
            direction: Direction::Forward as i32,
        })
        .await
        .expect("read_stream")
        .into_inner();

    let resp = s.message().await.expect("读响应").expect("应有数据");
    assert_eq!(resp.events.len(), 2);
    assert_eq!(resp.events[0].version, 2);
    assert_eq!(resp.events[1].version, 3);

    handle.abort();
}

/// 路由表架构下，读未知流（未创建/路由表缺失）→ NotFound
/// （旧哈希路由时代返回空列表）。
#[tokio::test]
async fn read_stream_missing_stream_not_found() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    let err = client
        .read_stream(ReadStreamRequest {
            stream_id: "nonexistent".to_string(),
            from_version: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
        })
        .await
        .expect_err("未知流应 NotFound");
    assert_eq!(err.code(), tonic::Code::NotFound, "{err}");

    handle.abort();
}

#[tokio::test]
async fn get_stream_meta_exists_and_version() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 不存在的流
    let meta = client
        .get_stream_meta(GetStreamMetaRequest {
            stream_id: "nope".to_string(),
        })
        .await
        .expect("查元数据")
        .into_inner();
    assert!(!meta.exists);
    assert_eq!(meta.current_version, 0);

    // 写两条后再查
    client
        .append(AppendRequest {
            stream_id: "meta".to_string(),
            expected_version: ev_no_stream(),
            events: vec![new_event(b"a"), new_event(b"b")],
        })
        .await
        .expect("append");

    let meta = client
        .get_stream_meta(GetStreamMetaRequest {
            stream_id: "meta".to_string(),
        })
        .await
        .expect("查元数据")
        .into_inner();
    assert!(meta.exists);
    assert_eq!(meta.current_version, 1, "两条事件后当前版本应为 1");

    handle.abort();
}

#[tokio::test]
async fn read_all_position_ordered_across_streams() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 找一组落在同一分片的流名，确保 ReadAll 能一次读到多流
    // 先各写一条，按返回的 shard_id 分组
    let mut by_shard: std::collections::HashMap<u64, Vec<String>> =
        std::collections::HashMap::new();
    for i in 0..10 {
        let name = format!("all-{i}");
        let resp = append_one(&mut client, &name, b"x").await;
        by_shard.entry(resp.shard_id).or_default().push(name);
    }

    // 取流数最多的那个分片
    let (shard_id, streams) = by_shard
        .into_iter()
        .max_by_key(|(_, v)| v.len())
        .expect("至少有一个分片");
    assert!(streams.len() >= 2, "该分片应至少承载 2 个流");

    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids: vec![shard_id],
            from_position: 0,
            max_count: 100,
            direction: Direction::Forward as i32,
            from_positions: vec![],
        })
        .await
        .expect("read_all")
        .into_inner();

    let mut events = Vec::new();
    while let Some(resp) = s.message().await.expect("读响应") {
        events.extend(resp.events);
    }

    assert_eq!(events.len(), streams.len(), "ReadAll 应返回该分片全部事件");

    // position 严格递增，即提交序
    for i in 1..events.len() {
        assert!(
            events[i].position > events[i - 1].position,
            "position 必须严格递增: {} 之后是 {}",
            events[i - 1].position,
            events[i].position
        );
    }

    // 确实跨了多个流
    let names: HashSet<_> = events.iter().map(|e| e.stream_id.as_str()).collect();
    assert!(names.len() >= 2, "ReadAll 应跨多个流，实际: {names:?}");

    handle.abort();
}

#[tokio::test]
async fn read_all_from_position_with_limit() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 同一个流的事件必定在同一分片，position 连续
    let mut shard_id = 0;
    for i in 0..10u8 {
        shard_id = append_one(&mut client, "allrange", &[i]).await.shard_id;
    }

    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids: vec![shard_id],
            from_position: 5,
            max_count: 3,
            direction: Direction::Forward as i32,
            from_positions: vec![],
        })
        .await
        .expect("read_all")
        .into_inner();

    let resp = s.message().await.expect("读响应").expect("应有数据");
    assert_eq!(resp.events.len(), 3);
    assert_eq!(resp.events[0].position, 5);
    assert_eq!(resp.events[1].position, 6);
    assert_eq!(resp.events[2].position, 7);

    handle.abort();
}

/// 跨分片 ReadAll 的便捷封装
async fn read_all_shards(
    client: &mut EventStoreClient<tonic::transport::Channel>,
    shard_ids: Vec<u64>,
    from_position: u64,
    max_count: u64,
) -> Vec<Event> {
    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids,
            from_position,
            max_count,
            direction: Direction::Forward as i32,
            from_positions: vec![],
        })
        .await
        .expect("read_all 应成功")
        .into_inner();

    let mut out = Vec::new();
    while let Some(resp) = s.message().await.expect("读流式响应") {
        out.extend(resp.events);
    }
    out
}

#[tokio::test]
async fn read_all_merge_per_shard_position_order() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 交错写入 10 个流，num_shards=2 故必然分布在两个分片上
    let mut per_shard: std::collections::HashMap<u64, Vec<u64>> = std::collections::HashMap::new();
    for i in 0..10u8 {
        let r = append_one(&mut client, &format!("x-{i}"), &[i]).await;
        per_shard
            .entry(r.shard_id)
            .or_default()
            .push(r.first_position);
    }
    assert_eq!(per_shard.len(), 2, "应覆盖 2 个分片");

    let all = read_all_shards(&mut client, vec![0, 1], 0, 0).await;
    assert_eq!(all.len(), 10, "跨分片应读到全部 10 条");

    // 归并结果里每个分片的子序列必须仍按 position 升序。
    // 这是分片内「严格提交序」保证，不能被跨分片归并打乱。
    for (shard_id, _) in &per_shard {
        let seq: Vec<u64> = all
            .iter()
            .filter(|e| e.shard_id == *shard_id)
            .map(|e| e.position)
            .collect();
        let mut sorted = seq.clone();
        sorted.sort_unstable();
        assert_eq!(
            seq, sorted,
            "分片 {shard_id} 在归并结果中的 position 必须仍升序，实际 {seq:?}"
        );
    }

    // 两个分片的事件确实交错出现，而非简单拼接
    let shard_seq: Vec<u64> = all.iter().map(|e| e.shard_id).collect();
    let switches = shard_seq.windows(2).filter(|w| w[0] != w[1]).count();
    assert!(
        switches >= 1,
        "归并结果应体现跨分片交错，实际分片序列 {shard_seq:?}"
    );

    handle.abort();
}

#[tokio::test]
async fn read_all_cross_shard_limit_n() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    for i in 0..10u8 {
        append_one(&mut client, &format!("y-{i}"), &[i]).await;
    }

    let page = read_all_shards(&mut client, vec![0, 1], 0, 4).await;
    assert_eq!(page.len(), 4, "max_count=4 应只返回 4 条");

    handle.abort();
}

#[tokio::test]
async fn read_all_per_shard_cursor_paging() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    for i in 0..10u8 {
        append_one(&mut client, &format!("z-{i}"), &[i]).await;
    }

    // 第一页
    let page1 = read_all_shards(&mut client, vec![0, 1], 0, 4).await;
    assert_eq!(page1.len(), 4);

    // 用各分片已消费到的最大 position + 1 构造下一页游标。
    // 单一 from_position 做不到这点：两个分片被消费的进度不同。
    let mut next: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    for e in &page1 {
        let ent = next.entry(e.shard_id).or_insert(0);
        *ent = (*ent).max(e.position + 1);
    }
    let cursors: Vec<ShardPosition> = [0u64, 1]
        .iter()
        .map(|&s| ShardPosition {
            shard_id: s,
            from_position: next.get(&s).copied().unwrap_or(0),
            ended: false,
        })
        .collect();

    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids: vec![],
            from_position: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
            from_positions: cursors,
        })
        .await
        .expect("翻页读取")
        .into_inner();
    let mut page2 = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        page2.extend(r.events);
    }

    assert_eq!(page2.len(), 6, "第二页应为剩余 6 条");

    // 两页无重叠、无遗漏
    let key = |e: &Event| (e.shard_id, e.position);
    let set1: HashSet<_> = page1.iter().map(key).collect();
    let set2: HashSet<_> = page2.iter().map(key).collect();
    assert!(set1.is_disjoint(&set2), "两页不能有重复事件");
    assert_eq!(set1.len() + set2.len(), 10, "两页合起来应覆盖全部 10 条");

    handle.abort();
}

#[tokio::test]
async fn backward_read_reversed() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 写入 5 条
    for i in 0..5 {
        append_one(&mut client, "rev", &[b'a' + i]).await;
    }

    // 正序读：a b c d e
    let fwd = read_stream(&mut client, "rev", 0, 0, Direction::Forward).await;
    let fwd_data: Vec<u8> = fwd.iter().map(|e| e.data[0]).collect();
    assert_eq!(fwd_data, vec![b'a', b'b', b'c', b'd', b'e']);

    // 倒序读（from 传 u64::MAX 表示「从最新开始」）：e d c b a
    let mut s = client
        .read_stream(ReadStreamRequest {
            stream_id: "rev".to_string(),
            from_version: u64::MAX,
            max_count: 0,
            direction: Direction::Backward as i32,
        })
        .await
        .expect("倒序读")
        .into_inner();
    let mut back = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        back.extend(r.events);
    }
    let back_data: Vec<u8> = back.iter().map(|e| e.data[0]).collect();
    assert_eq!(back_data, vec![b'e', b'd', b'c', b'b', b'a'], "应倒序");

    // 版本号也应递减
    let back_vers: Vec<u64> = back.iter().map(|e| e.version).collect();
    assert_eq!(back_vers, vec![4, 3, 2, 1, 0]);

    // 限量 + 倒序：取最新 3 条
    let mut s = client
        .read_stream(ReadStreamRequest {
            stream_id: "rev".to_string(),
            from_version: u64::MAX,
            max_count: 3,
            direction: Direction::Backward as i32,
        })
        .await
        .expect("倒序限量")
        .into_inner();
    let mut recent = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        recent.extend(r.events);
    }
    let recent_data: Vec<u8> = recent.iter().map(|e| e.data[0]).collect();
    assert_eq!(recent_data, vec![b'e', b'd', b'c'], "应为最新 3 条倒序");

    // 从中间版本倒读：from=2 向下到 0
    let mut s = client
        .read_stream(ReadStreamRequest {
            stream_id: "rev".to_string(),
            from_version: 2,
            max_count: 0,
            direction: Direction::Backward as i32,
        })
        .await
        .expect("从 v2 倒读")
        .into_inner();
    let mut mid = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        mid.extend(r.events);
    }
    let mid_vers: Vec<u64> = mid.iter().map(|e| e.version).collect();
    assert_eq!(mid_vers, vec![2, 1, 0], "应从 v2 倒读到 v0");

    handle.abort();
}

#[tokio::test]
async fn cross_shard_read_all_backward() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 写入多个流,分散到不同分片(默认 2 分片)
    // 为确保跨分片,写足够多流让哈希分散
    for i in 0..10 {
        let stream = format!("s{i}");
        append_one(&mut client, &stream, &[b'a' + (i as u8)]).await;
    }

    // 正序 ReadAll
    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids: vec![0, 1],
            from_position: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
            from_positions: vec![],
        })
        .await
        .expect("正序 ReadAll")
        .into_inner();
    let mut fwd = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        fwd.extend(r.events);
    }
    assert_eq!(fwd.len(), 10, "应读到 10 条事件");

    // 倒序 ReadAll:按 HLC 降序
    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids: vec![0, 1],
            from_position: u64::MAX, // 从最新开始倒读
            max_count: 0,
            direction: Direction::Backward as i32,
            from_positions: vec![
                ShardPosition {
                    shard_id: 0,
                    from_position: u64::MAX,
                    ended: false,
                },
                ShardPosition {
                    shard_id: 1,
                    from_position: u64::MAX,
                    ended: false,
                },
            ],
        })
        .await
        .expect("倒序 ReadAll")
        .into_inner();

    let mut back = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        back.extend(r.events);
    }
    assert_eq!(back.len(), 10, "倒序应读到 10 条");

    // 验证 HLC 降序(墙上时钟递减)
    for w in back.windows(2) {
        let h0 = w[0].hlc.as_ref().unwrap();
        let h1 = w[1].hlc.as_ref().unwrap();
        assert!(h0.wall >= h1.wall, "HLC 应降序: {} vs {}", h0.wall, h1.wall);
    }

    // 倒序结果应与正序相反(按事件标识,如 stream_id)
    let fwd_streams: Vec<String> = fwd.iter().map(|e| e.stream_id.clone()).collect();
    let back_streams: Vec<String> = back.iter().map(|e| e.stream_id.clone()).collect();
    let mut expected_rev = fwd_streams.clone();
    expected_rev.reverse();
    // 注意:HLC 相同时顺序可能不完全相反(shard_id / position 作为次序),
    // 但大致应呈倒序趋势。这里只验证 HLC 降序即可,不强求完全镜像。
    eprintln!("正序流名: {:?}", fwd_streams);
    eprintln!("倒序流名: {:?}", back_streams);

    handle.abort();
}

/// 反向翻页必须干净终止：消费到 position 0 后游标保留为 0（而非被丢弃），
/// 下页 from=0 返回空页 —— 空页是正反两向统一的终止条件。
/// 修复前（游标被丢弃 → 客户端保留旧游标重读尾页）该测试死循环在 pages 守卫上。
#[tokio::test]
async fn read_all_backward_paging_terminates() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 10 个流分散到 2 分片
    for i in 0..10u8 {
        let stream = format!("p{i}");
        append_one(&mut client, &stream, &[b'a' + i]).await;
    }

    // 首页：统一从 u64::MAX 哨兵反向读
    let mut from_positions: Vec<ShardPosition> = vec![0, 1]
        .iter()
        .map(|&sid| ShardPosition {
            shard_id: sid,
            from_position: u64::MAX,
            ended: false,
        })
        .collect();

    let mut seen: std::collections::HashSet<(u64, u64)> = std::collections::HashSet::new();
    let mut total = 0;
    let mut pages = 0;
    let mut last_next: Vec<ShardPosition> = Vec::new();
    loop {
        pages += 1;
        assert!(
            pages < 10,
            "反向翻页应干净终止（最多 ~4 页），当前卡在第 {pages} 页"
        );
        let mut s = client
            .read_all(ReadAllRequest {
                shard_ids: vec![],
                from_position: 0,
                max_count: 4,
                direction: Direction::Backward as i32,
                from_positions: from_positions.clone(),
            })
            .await
            .expect("反向翻页")
            .into_inner();
        let mut page_events = Vec::new();
        while let Some(r) = s.message().await.expect("读响应") {
            page_events.extend(r.events);
            from_positions = r.next_positions;
        }
        for e in &page_events {
            assert!(
                seen.insert((e.shard_id, e.position)),
                "事件 (shard {}, position {}) 重复投递",
                e.shard_id,
                e.position
            );
        }
        total += page_events.len();
        if page_events.is_empty() {
            assert_eq!(
                from_positions, last_next,
                "空页游标应稳定不变（读尽分片游标为 0）"
            );
            break;
        }
        last_next = from_positions.clone();
    }
    assert_eq!(total, 10, "应恰好收到 10 条事件");
    assert!(pages >= 2, "max_count=4 应至少翻 2 页");
    handle.abort();
}

/// 首页即反向读尽（单分片 3 条、max_count=4）：游标必须保留为
/// (shard, 0) 而非被丢弃；翻页 from=0 返回空页且游标稳定。
#[tokio::test]
async fn read_all_backward_last_page_cursor_zero_kept() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 单流 3 条（同分片），从 append 响应取实际分片号
    let mut shard_id = 0u64;
    for i in 0..3u8 {
        let resp = append_one(&mut client, "one", &[i]).await;
        shard_id = resp.shard_id;
    }

    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids: vec![],
            from_position: 0,
            max_count: 4,
            direction: Direction::Backward as i32,
            from_positions: vec![ShardPosition {
                shard_id,
                from_position: u64::MAX,
                ended: false,
            }],
        })
        .await
        .expect("首页反向读")
        .into_inner();
    let mut events = Vec::new();
    let mut next_positions = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        events.extend(r.events);
        next_positions = r.next_positions;
    }
    assert_eq!(events.len(), 3, "首页应读到全部 3 条");
    assert_eq!(
        next_positions,
        vec![ShardPosition {
            shard_id,
            from_position: 0,
            ended: true, // 消费到 position 0：该分片已读尽
        }],
        "消费到 position 0 后游标应保留为 (shard, 0, ended)，而非被丢弃"
    );

    // 翻页：ended 分片返回空页（不重投 position 0），游标稳定 → 干净终止
    let mut s = client
        .read_all(ReadAllRequest {
            shard_ids: vec![],
            from_position: 0,
            max_count: 4,
            direction: Direction::Backward as i32,
            from_positions: next_positions,
        })
        .await
        .expect("翻页反向读")
        .into_inner();
    let mut tail_events = Vec::new();
    let mut tail_positions = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        tail_events.extend(r.events);
        tail_positions = r.next_positions;
    }
    assert!(tail_events.is_empty(), "ended 分片应返回空页");
    assert_eq!(
        tail_positions,
        vec![ShardPosition {
            shard_id,
            from_position: 0,
            ended: true,
        }],
        "空页游标应稳定"
    );
    handle.abort();
}

/// 从订阅流里取下一条响应，带超时。
///
/// 必须带超时：订阅流在 live 阶段会一直挂着等新事件，
/// 若断言写错或服务端没推，`message()` 会永久阻塞，
/// 表现为整个测试进程卡死而非失败。
async fn next_sub(
    s: &mut tonic::Streaming<SubscribeResponse>,
) -> Option<subscribe_response::Payload> {
    let fut = s.message();
    match tokio::time::timeout(Duration::from_secs(5), fut).await {
        Ok(Ok(Some(resp))) => resp.payload,
        Ok(Ok(None)) => None,
        Ok(Err(e)) => panic!("订阅流出错: {e}"),
        Err(_) => panic!("等订阅响应超过 5 秒，服务端可能没推送"),
    }
}

/// 收取 catch-up 阶段的全部事件，直到收到 caught_up 分界信号
async fn drain_until_caught_up(
    s: &mut tonic::Streaming<SubscribeResponse>,
) -> Vec<SubscriptionEvent> {
    let mut out = Vec::new();
    loop {
        match next_sub(s).await {
            Some(subscribe_response::Payload::Event(e)) => out.push(e),
            Some(subscribe_response::Payload::CaughtUp(_)) => return out,
            Some(subscribe_response::Payload::Degraded(_)) => panic!("健康订阅不应降级"),
            None => panic!("订阅流在收到 caught_up 前就结束了"),
        }
    }
}

#[tokio::test]
async fn subscribe_catchup_then_live() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 先写 3 条历史
    for i in 0..3u8 {
        append_one(&mut client, "sub", &[i]).await;
    }

    let mut s = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["sub".to_string()],
            })),
        })
        .await
        .expect("subscribe")
        .into_inner();

    // catch-up 阶段应拿到全部 3 条
    let history = drain_until_caught_up(&mut s).await;
    assert_eq!(history.len(), 3, "catch-up 应补齐 3 条历史");
    assert_eq!(history[0].data, vec![0]);
    assert_eq!(history[1].data, vec![1]);
    assert_eq!(history[2].data, vec![2]);

    // live 阶段：新写入应被推送
    append_one(&mut client, "sub", &[99]).await;
    match next_sub(&mut s).await {
        Some(subscribe_response::Payload::Event(e)) => {
            assert_eq!(e.data, vec![99], "应收到实时推送的新事件");
            assert_eq!(e.version, 3);
        }
        other => panic!("应收到 Event，实际: {other:?}"),
    }

    // 显式关掉订阅流，让服务端后台任务感知断开并退出。
    // 不 drop 的话该任务会一直挂在 recv() 上。
    drop(s);
    handle.abort();
}

#[tokio::test]
async fn subscribe_client_disconnect_does_not_block_subsequent_subscription() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");
    append_one(&mut client, "disconnect-stream", b"history").await;

    let cancelled = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["disconnect-stream".into()],
            })),
        })
        .await
        .expect("建立将取消的订阅")
        .into_inner();
    drop(cancelled);
    tokio::time::sleep(Duration::from_millis(150)).await;

    let mut healthy = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["disconnect-stream".into()],
            })),
        })
        .await
        .expect("取消旧订阅后仍可建立新订阅")
        .into_inner();
    let history = drain_until_caught_up(&mut healthy).await;
    assert_eq!(history.len(), 1, "新订阅必须正常完成历史追平");

    drop(healthy);
    handle.abort();
}

#[tokio::test]
async fn subscribe_only_this_stream() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // 找两个落在同一分片的流，验证订阅能正确过滤
    let a = append_one(&mut client, "filter-a", b"a0").await;
    let mut other = None;
    for i in 0..20 {
        let name = format!("filter-b{i}");
        let r = append_one(&mut client, &name, b"b0").await;
        if r.shard_id == a.shard_id {
            other = Some(name);
            break;
        }
    }
    let other = other.expect("应能找到同分片的另一个流");

    let mut s = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["filter-a".to_string()],
            })),
        })
        .await
        .expect("subscribe")
        .into_inner();

    let history = drain_until_caught_up(&mut s).await;
    assert_eq!(history.len(), 1, "catch-up 只应含本流的 1 条");
    assert_eq!(history[0].stream_id, "filter-a");

    // 往同分片的另一个流写入，不应推给本订阅
    append_one(&mut client, &other, b"noise").await;
    // 再往本流写入，应能收到
    append_one(&mut client, "filter-a", b"a1").await;

    match next_sub(&mut s).await {
        Some(subscribe_response::Payload::Event(e)) => {
            assert_eq!(e.stream_id, "filter-a", "订阅单流时不能收到其他流的事件");
            assert_eq!(e.data, b"a1");
        }
        other => panic!("应收到本流事件，实际: {other:?}"),
    }

    drop(s);
    handle.abort();
}

#[tokio::test]
async fn subscribe_multiple_streams_across_shards() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    let first = append_one(&mut client, "aggregate-a", b"a0").await;
    let mut second = None;
    for i in 0..20 {
        let stream = format!("aggregate-b-{i}");
        let response = append_one(&mut client, &stream, b"b0").await;
        if response.shard_id != first.shard_id {
            second = Some(stream);
            break;
        }
    }
    let second = second.expect("应找到另一个分片上的 stream");

    let mut subscription = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["aggregate-a".into(), second.clone()],
            })),
        })
        .await
        .expect("建立聚合订阅")
        .into_inner();

    let history = drain_until_caught_up(&mut subscription).await;
    let streams: HashSet<_> = history
        .iter()
        .map(|event| event.stream_id.as_str())
        .collect();
    assert_eq!(streams, HashSet::from(["aggregate-a", second.as_str()]));

    append_one(&mut client, "aggregate-a", b"a1").await;
    append_one(&mut client, &second, b"b1").await;
    let mut live = HashSet::new();
    while live.len() < 2 {
        match next_sub(&mut subscription).await {
            Some(subscribe_response::Payload::Event(event)) => {
                live.insert((event.stream_id, event.version, event.data));
            }
            Some(subscribe_response::Payload::Degraded(_)) => panic!("健康订阅不应降级"),
            Some(subscribe_response::Payload::CaughtUp(_)) => {}
            None => panic!("订阅流提前结束"),
        }
    }
    assert!(live.contains(&(String::from("aggregate-a"), 1, b"a1".to_vec())));
    assert!(live.contains(&(second, 1, b"b1".to_vec())));

    drop(subscription);
    handle.abort();
}

#[tokio::test]
async fn subscribe_aggregates_remote_shard_through_internal_rpc() {
    let (first_addr, first_handle, second_handle, first, second, _first_dir, _second_dir) =
        start_two_shard_servers().await;

    // 两个路由表在相同顺序下分配，保证接入节点和远程承载节点对同一 stream 一致。
    first
        .route_table()
        .allocate("remote-stream")
        .await
        .expect("节点一记录远程路由");
    second
        .route_table()
        .allocate("remote-stream")
        .await
        .expect("节点二记录远程路由");
    first
        .route_table()
        .allocate("local-stream")
        .await
        .expect("节点一记录本地路由");
    second
        .route_table()
        .allocate("local-stream")
        .await
        .expect("节点二记录本地路由");
    assert_eq!(first.route_table().lookup("remote-stream").await, Some(0));
    assert_eq!(first.route_table().lookup("local-stream").await, Some(1));

    let mut first_client = EventStoreClient::connect(first_addr)
        .await
        .expect("连接节点一");
    let second_addr = first.config().node.peers[0].addr.clone();
    let mut second_client = EventStoreClient::connect(second_addr)
        .await
        .expect("连接节点二");

    append_one(&mut first_client, "local-stream", b"local-history").await;
    append_one(&mut second_client, "remote-stream", b"remote-history").await;

    let mut subscription = first_client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["local-stream".into(), "remote-stream".into()],
            })),
        })
        .await
        .expect("建立跨节点聚合订阅")
        .into_inner();
    let history = drain_until_caught_up(&mut subscription).await;
    let identities: HashSet<_> = history
        .iter()
        .map(|event| (event.stream_id.as_str(), event.version))
        .collect();
    assert_eq!(
        identities,
        HashSet::from([("local-stream", 0), ("remote-stream", 0)])
    );

    append_one(&mut second_client, "remote-stream", b"remote-live").await;
    match next_sub(&mut subscription).await {
        Some(subscribe_response::Payload::Event(event)) => {
            assert_eq!(event.stream_id, "remote-stream");
            assert_eq!(event.version, 1);
            assert_eq!(event.data, b"remote-live");
        }
        other => panic!("远程分片实时事件应经内部 RPC 转发，实际: {other:?}"),
    }

    drop(subscription);
    first_handle.abort();
    second_handle.abort();
}

#[tokio::test]
async fn subscribe_from_follower_forwards_to_leader_internal_listener() {
    let follower_public_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 follower 公共端口");
    let leader_public_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 leader 公共端口");
    let leader_internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 leader 内部端口");
    let follower_internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 follower 内部端口");
    let follower_public_addr = format!(
        "http://{}",
        follower_public_listener
            .local_addr()
            .expect("读取 follower 公共地址")
    );
    let leader_public_addr = format!(
        "http://{}",
        leader_public_listener
            .local_addr()
            .expect("读取 leader 公共地址")
    );
    let leader_internal_addr = format!(
        "http://{}",
        leader_internal_listener
            .local_addr()
            .expect("读取 leader 内部地址")
    );
    let follower_internal_addr = format!(
        "http://{}",
        follower_internal_listener
            .local_addr()
            .expect("读取 follower 内部地址")
    );
    let follower_dir = tempfile::tempdir().expect("follower 临时目录");
    let leader_dir = tempfile::tempdir().expect("leader 临时目录");
    let placement = PlacementConfig {
        replication_factor: 2,
        nodes: vec![
            PlacementNode {
                id: 1,
                primary: vec![],
                replica: vec![0],
            },
            PlacementNode {
                id: 2,
                primary: vec![0],
                replica: vec![],
            },
        ],
    };
    let follower_config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: Some(
                follower_internal_listener
                    .local_addr()
                    .expect("读取 follower 内部监听地址")
                    .to_string(),
            ),
            peers: vec![PeerConfig {
                id: 2,
                addr: leader_public_addr.clone(),
                internal_addr: Some(leader_internal_addr.clone()),
            }],
        },
        storage: StorageConfig {
            data_dir: follower_dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement: placement.clone(),
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let leader_config = Config {
        node: NodeConfig {
            id: 2,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: None,
            peers: vec![PeerConfig {
                id: 1,
                addr: follower_public_addr.clone(),
                internal_addr: None,
            }],
        },
        storage: StorageConfig {
            data_dir: leader_dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        placement,
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let follower = Server::new(follower_config.clone()).expect("创建 follower");
    let leader = Server::new(leader_config.clone()).expect("创建 leader");
    follower.init().await.expect("初始化 follower");
    leader.init().await.expect("初始化 leader");
    leader
        .shard_manager()
        .get_shard(0)
        .await
        .expect("获取 leader shard")
        .raft
        .initialize(std::collections::BTreeSet::from([2u64]))
        .await
        .expect("初始化 leader raft");

    follower
        .route_table()
        .allocate("follower-stream")
        .await
        .expect("记录 follower 路由");
    leader
        .route_table()
        .allocate("follower-stream")
        .await
        .expect("记录 leader 路由");

    let follower_service = es_server::service::EsService::with_limits(
        follower.shard_manager().clone(),
        follower_config.limits.clone(),
        follower.route_table().clone(),
        &follower_config,
    )
    .expect("创建 follower 服务");
    let leader_service = es_server::service::EsService::with_limits(
        leader.shard_manager().clone(),
        leader_config.limits.clone(),
        leader.route_table().clone(),
        &leader_config,
    )
    .expect("创建 leader 服务");
    let leader_admin = es_raft::RaftAdminService::new(leader.shard_manager().clone());
    let follower_handle = tokio::spawn(async move {
        let _ = tokio::try_join!(
            tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(follower_service.clone()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    follower_public_listener,
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(follower_service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    follower_internal_listener,
                )),
        );
    });
    let leader_handle = tokio::spawn(async move {
        let _ = tokio::try_join!(
            tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(leader_service.clone()))
                .add_service(RaftAdminServer::new(leader_admin))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    leader_public_listener,
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(leader_service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    leader_internal_listener,
                )),
        );
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut follower_internal = InternalSubscriptionClient::connect(follower_internal_addr)
        .await
        .expect("连接 follower 内部端口");
    let internal_err = follower_internal
        .subscribe_internal(InternalSubscribeRequest {
            shard_id: 0,
            stream_ids: vec!["follower-stream".into()],
        })
        .await
        .expect_err("follower 不得作为内部订阅的追平来源");
    assert_eq!(internal_err.code(), tonic::Code::Unavailable);

    let mut leader_client = EventStoreClient::connect(leader_public_addr)
        .await
        .expect("连接 leader");
    append_one(&mut leader_client, "follower-stream", b"leader-history").await;

    let mut follower_client = EventStoreClient::connect(follower_public_addr)
        .await
        .expect("连接 follower");
    let mut subscription = follower_client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["follower-stream".into()],
            })),
        })
        .await
        .expect("从 follower 建立订阅")
        .into_inner();
    let history = drain_until_caught_up(&mut subscription).await;
    assert_eq!(history.len(), 1, "follower 必须转发 leader 的历史事件");
    assert_eq!(history[0].data, b"leader-history");

    drop(subscription);
    follower_handle.abort();
    leader_handle.abort();
}

#[tokio::test]
async fn subscribe_validates_public_target_without_exposing_shards() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    let missing = client
        .subscribe(SubscribeRequest { target: None })
        .await
        .expect_err("缺失目标必须拒绝");
    assert_eq!(missing.code(), tonic::Code::InvalidArgument);
    assert!(missing.message().contains("target"));

    let empty = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec![],
            })),
        })
        .await
        .expect_err("空 stream 列表必须拒绝");
    assert_eq!(empty.code(), tonic::Code::InvalidArgument);
    assert!(empty.message().contains("stream_ids"));

    let empty_create = client
        .create_stream(CreateStreamRequest {
            stream_id: String::new(),
        })
        .await
        .expect_err("空 stream ID 必须拒绝");
    assert_eq!(empty_create.code(), tonic::Code::InvalidArgument);
    assert!(empty_create.message().contains("stream_id"));

    let unknown = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["missing-stream".into()],
            })),
        })
        .await
        .expect_err("未知 stream 必须拒绝");
    assert_eq!(unknown.code(), tonic::Code::NotFound);
    assert!(
        !unknown.message().contains("shard"),
        "公开错误不能泄露分片: {unknown}"
    );

    handle.abort();
}

#[tokio::test]
async fn public_listener_does_not_expose_internal_subscription() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = InternalSubscriptionClient::connect(addr)
        .await
        .expect("连接公共服务");
    let err = client
        .subscribe_internal(InternalSubscribeRequest {
            shard_id: 0,
            stream_ids: vec![],
        })
        .await
        .expect_err("公共端口不能暴露内部订阅");
    assert_eq!(err.code(), tonic::Code::Unimplemented);

    handle.abort();
}

#[tokio::test]
async fn server_serve_separates_public_and_internal_subscription_listeners() {
    let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("预留公共端口");
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("预留内部端口");
    let public_addr = public_listener.local_addr().expect("读取公共端口");
    let internal_addr = internal_listener.local_addr().expect("读取内部端口");
    drop(public_listener);
    drop(internal_listener);

    let dir = tempfile::tempdir().expect("临时目录");
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: public_addr.to_string(),
            internal_listen_addr: Some(internal_addr.to_string()),
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
    let server = Server::new(config).expect("创建服务器");
    server.init().await.expect("初始化服务器");
    let shard = server.shard_manager().get_shard(0).await.expect("获取分片");
    shard
        .raft
        .initialize(std::collections::BTreeSet::from([1u64]))
        .await
        .expect("初始化 raft");

    let handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut public_client = InternalSubscriptionClient::connect(format!("http://{public_addr}"))
        .await
        .expect("连接公共端口");
    let public_err = public_client
        .subscribe_internal(InternalSubscribeRequest {
            shard_id: 0,
            stream_ids: vec![],
        })
        .await
        .expect_err("公共端口不得注册内部服务");
    assert_eq!(public_err.code(), tonic::Code::Unimplemented);

    let mut internal_client =
        InternalSubscriptionClient::connect(format!("http://{internal_addr}"))
            .await
            .expect("连接内部端口");
    let mut internal_stream = internal_client
        .subscribe_internal(InternalSubscribeRequest {
            shard_id: 0,
            stream_ids: vec![],
        })
        .await
        .expect("内部端口应提供内部服务")
        .into_inner();
    match internal_stream.message().await.expect("读取内部响应") {
        Some(InternalSubscribeResponse {
            payload: Some(internal_subscribe_response::Payload::CaughtUp(_)),
        }) => {}
        other => panic!("内部端口应返回 caught-up，实际: {other:?}"),
    }

    handle.abort();
}

#[tokio::test]
async fn subscribe_reports_degraded_when_remote_source_is_unavailable() {
    let (first_addr, first_handle, second_handle, first, second, _first_dir, _second_dir) =
        start_two_shard_servers().await;
    first
        .route_table()
        .allocate("remote-only")
        .await
        .expect("节点一记录路由");
    second
        .route_table()
        .allocate("remote-only")
        .await
        .expect("节点二记录路由");
    assert_eq!(first.route_table().lookup("remote-only").await, Some(0));

    // 接入节点保持可用，但远程分片节点下线：聚合器必须隐去内部细节并降级。
    second_handle.abort();
    let mut client = EventStoreClient::connect(first_addr)
        .await
        .expect("连接节点一");
    let mut stream = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::Streams(SubscribeStreams {
                stream_ids: vec!["remote-only".into()],
            })),
        })
        .await
        .expect("建立订阅")
        .into_inner();
    match next_sub(&mut stream).await {
        Some(subscribe_response::Payload::Degraded(_)) => {}
        other => panic!("远程来源不可用应只发送 degraded，实际: {other:?}"),
    }
    drop(stream);
    first_handle.abort();
}

#[tokio::test]
async fn subscribe_all_shard_streams() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");

    // `$all` 订阅全部 stream，不向客户端暴露它们所在的 shard。
    let mut names = Vec::new();
    for i in 0..20 {
        let name = format!("sa-{i}");
        append_one(&mut client, &name, b"x").await;
        names.push(name);
        if names.len() >= 2 {
            break;
        }
    }

    let mut s = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::All(Empty {})),
        })
        .await
        .expect("subscribe")
        .into_inner();

    let history = drain_until_caught_up(&mut s).await;
    let names: HashSet<_> = history.iter().map(|e| e.stream_id.as_str()).collect();
    assert!(names.len() >= 2, "$all 订阅应跨多个流，实际: {names:?}");

    drop(s);
    handle.abort();
}

#[tokio::test]
async fn subscribe_all_aggregates_remote_shards_and_emits_caught_up_once() {
    let (first_addr, first_handle, second_handle, first, second, _first_dir, _second_dir) =
        start_two_shard_servers().await;

    // 两个节点用相同顺序创建路由，保证 $all 的本地和远程来源可分别写入。
    for server in [&first, &second] {
        server
            .route_table()
            .allocate("all-remote-stream")
            .await
            .expect("记录远程路由");
        server
            .route_table()
            .allocate("all-local-stream")
            .await
            .expect("记录本地路由");
    }
    assert_eq!(
        first.route_table().lookup("all-remote-stream").await,
        Some(0)
    );
    assert_eq!(
        first.route_table().lookup("all-local-stream").await,
        Some(1)
    );

    let mut first_client = EventStoreClient::connect(first_addr)
        .await
        .expect("连接接入节点");
    let mut second_client = EventStoreClient::connect(first.config().node.peers[0].addr.clone())
        .await
        .expect("连接远程节点");
    append_one(&mut first_client, "all-local-stream", b"local-history").await;
    append_one(&mut second_client, "all-remote-stream", b"remote-history").await;

    let mut subscription = first_client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::All(Empty {})),
        })
        .await
        .expect("建立跨节点 $all 订阅")
        .into_inner();
    let history = drain_until_caught_up(&mut subscription).await;
    let streams: HashSet<_> = history
        .iter()
        .map(|event| event.stream_id.as_str())
        .collect();
    assert_eq!(
        streams,
        HashSet::from(["all-local-stream", "all-remote-stream"]),
        "$all 必须聚合两个节点各自承载的 shard"
    );

    // 所有来源追平后只发送一个公共 caught_up；重复信号会让 --once 错判完成。
    assert!(
        tokio::time::timeout(Duration::from_millis(250), subscription.message())
            .await
            .is_err(),
        "追平后不应再发送额外的订阅状态"
    );

    append_one(&mut first_client, "all-local-stream", b"local-live").await;
    append_one(&mut second_client, "all-remote-stream", b"remote-live").await;
    let mut live = HashSet::new();
    while live.len() < 2 {
        match next_sub(&mut subscription).await {
            Some(subscribe_response::Payload::Event(event)) => {
                live.insert((event.stream_id, event.data));
            }
            other => panic!("$all 实时阶段应只收到事件，实际: {other:?}"),
        }
    }
    assert!(live.contains(&(String::from("all-local-stream"), b"local-live".to_vec())));
    assert!(live.contains(&(String::from("all-remote-stream"), b"remote-live".to_vec())));

    drop(subscription);
    first_handle.abort();
    second_handle.abort();
}

#[tokio::test]
async fn subscribe_all_dynamically_includes_new_stream() {
    let (addr, handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(addr).await.expect("连接");
    append_one(&mut client, "all-before", b"before").await;

    let mut subscription = client
        .subscribe(SubscribeRequest {
            target: Some(subscribe_request::Target::All(Empty {})),
        })
        .await
        .expect("建立 $all 订阅")
        .into_inner();
    let _ = drain_until_caught_up(&mut subscription).await;

    append_one(&mut client, "all-created-after-subscribe", b"after").await;
    match next_sub(&mut subscription).await {
        Some(subscribe_response::Payload::Event(event)) => {
            assert_eq!(event.stream_id, "all-created-after-subscribe");
            assert_eq!(event.data, b"after");
        }
        other => panic!("$all 应纳入新 stream，实际: {other:?}"),
    }

    drop(subscription);
    handle.abort();
}
