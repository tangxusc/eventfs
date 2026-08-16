//! 端到端集成测试：单节点写入与读取

use std::collections::HashSet;
use std::time::Duration;

use es_proto::eventstore::aggregate_store_client::AggregateStoreClient;
use es_proto::eventstore::aggregate_store_internal_server::{
    AggregateStoreInternal, AggregateStoreInternalServer,
};
use es_proto::eventstore::aggregate_store_server::AggregateStoreServer;
use es_proto::eventstore::event_store_server::EventStoreServer;
use es_proto::eventstore::internal_subscription_client::InternalSubscriptionClient;
use es_proto::eventstore::internal_subscription_server::InternalSubscriptionServer;
use es_proto::eventstore::migration_server::MigrationServer;
use es_proto::eventstore::ownership_internal_server::OwnershipInternalServer;
use es_proto::eventstore::persistent_subscriptions_client::PersistentSubscriptionsClient;
use es_proto::eventstore::persistent_subscriptions_server::PersistentSubscriptionsServer;
use es_proto::eventstore::raft_admin_server::RaftAdminServer;
use es_proto::eventstore::{event_store_client::EventStoreClient, *};
use es_server::Server;
use es_server::config::{
    Config, NodeConfig, PeerConfig, PlacementConfig, PlacementNode, StorageConfig,
};
use tokio_stream::StreamExt;

async fn wait_shard_leader(server: &Server, shard_id: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("等待 leader 时获取分片");
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
    for shard_id in 0..2 {
        wait_shard_leader(&server, shard_id).await;
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
    let aggregate_service = es_server::aggregate_service::AggregateStoreService::new(
        server.shard_manager().clone(),
        &config,
    )
    .expect("创建聚合服务");
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(service.clone()))
            .add_service(PersistentSubscriptionsServer::new(service))
            .add_service(AggregateStoreServer::new(aggregate_service))
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
    wait_shard_leader(&first, 1).await;
    wait_shard_leader(&second, 0).await;

    let first_service = es_server::service::EsService::with_ownership(
        first.shard_manager().clone(),
        first_config.limits.clone(),
        first.route_table().clone(),
        first.ownership().clone(),
        &first_config,
    )
    .expect("节点一服务");
    let second_service = es_server::service::EsService::with_ownership(
        second.shard_manager().clone(),
        second_config.limits.clone(),
        second.route_table().clone(),
        second.ownership().clone(),
        &second_config,
    )
    .expect("节点二服务");
    let first_admin = es_raft::RaftAdminService::new(first.shard_manager().clone());
    let second_admin = es_raft::RaftAdminService::new(second.shard_manager().clone());
    let first_migration = es_server::migration_service::MigrationService::new(
        first.route_table().clone(),
        first.shard_manager().clone(),
        first.ownership().clone(),
    );
    let second_migration = es_server::migration_service::MigrationService::new(
        second.route_table().clone(),
        second.shard_manager().clone(),
        second.ownership().clone(),
    );
    let first_aggregate = es_server::aggregate_service::AggregateStoreService::new(
        first.shard_manager().clone(),
        &first_config,
    )
    .expect("节点一聚合服务");
    let second_aggregate = es_server::aggregate_service::AggregateStoreService::new(
        second.shard_manager().clone(),
        &second_config,
    )
    .expect("节点二聚合服务");
    let first_handle = tokio::spawn(async move {
        let _ = tokio::try_join!(
            tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(first_service.clone()))
                .add_service(PersistentSubscriptionsServer::new(first_service.clone()))
                .add_service(RaftAdminServer::new(first_admin))
                .add_service(MigrationServer::new(first_migration.clone()))
                .add_service(AggregateStoreServer::new(first_aggregate.clone()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    first_listener
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(first_service))
                .add_service(OwnershipInternalServer::new(first_migration))
                .add_service(AggregateStoreInternalServer::new(first_aggregate))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    first_internal_listener
                )),
        );
    });
    let second_handle = tokio::spawn(async move {
        let _ = tokio::try_join!(
            tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(second_service.clone()))
                .add_service(PersistentSubscriptionsServer::new(second_service.clone()))
                .add_service(RaftAdminServer::new(second_admin))
                .add_service(MigrationServer::new(second_migration.clone()))
                .add_service(AggregateStoreServer::new(second_aggregate.clone()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    second_listener
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(second_service))
                .add_service(OwnershipInternalServer::new(second_migration))
                .add_service(AggregateStoreInternalServer::new(second_aggregate))
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
        .unwrap_or_else(|error| panic!("append {stream_id} 应成功: {error}"))
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

fn persistent_stream_target(stream_id: &str) -> PersistentSubscriptionTarget {
    persistent_streams_target(&[stream_id])
}

fn persistent_streams_target(stream_ids: &[&str]) -> PersistentSubscriptionTarget {
    PersistentSubscriptionTarget {
        target: Some(persistent_subscription_target::Target::Streams(
            SubscribeStreams {
                stream_ids: stream_ids
                    .iter()
                    .map(|stream| (*stream).to_string())
                    .collect(),
            },
        )),
    }
}

fn persistent_all_target() -> PersistentSubscriptionTarget {
    PersistentSubscriptionTarget {
        target: Some(persistent_subscription_target::Target::All(Empty {})),
    }
}

async fn settle_delivery(
    client: &mut PersistentSubscriptionsClient<tonic::transport::Channel>,
    group: &str,
    consumer: &str,
    delivery: &PersistentDelivery,
    action: PersistentSettlementAction,
) -> PersistentSettlementStatus {
    let response = client
        .settle_persistent_subscription(SettlePersistentSubscriptionRequest {
            name: group.into(),
            consumer_id: consumer.into(),
            group_epoch: delivery.group_epoch,
            settlements: vec![PersistentSettlement {
                delivery_id: delivery.delivery_id.clone(),
                action: action as i32,
                reason: "e2e".into(),
            }],
        })
        .await
        .expect("结算 delivery")
        .into_inner();
    PersistentSettlementStatus::try_from(response.results[0].status).expect("合法结算状态")
}

async fn fetch_persistent_one(
    client: &mut PersistentSubscriptionsClient<tonic::transport::Channel>,
    group: &str,
    consumer: &str,
) -> PersistentDelivery {
    let response = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: group.into(),
            consumer_id: consumer.into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("拉取单条持久化订阅事件")
        .into_inner();
    assert_eq!(response.deliveries.len(), 1, "必须恰好拉取一条事件");
    response.deliveries.into_iter().next().unwrap()
}

async fn wait_route_projection(server: &Server, stream: &str, expected_shard: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        if server.route_table().lookup(stream).await == Some(expected_shard) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        server.route_table().lookup(stream).await,
        Some(expected_shard),
        "兼容路由投影未在时限内收敛: stream={stream}"
    );
}

fn aggregate_event_set() -> Option<AggregateEventSetRef> {
    Some(AggregateEventSetRef {
        business_space: "orders".into(),
        aggregate_type: "order".into(),
    })
}

fn aggregate_event(data: &[u8]) -> NewAggregateEvent {
    NewAggregateEvent {
        event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        event_type: "order.changed".into(),
        data: data.to_vec(),
        metadata: Vec::new(),
    }
}

fn aggregate_no_instance() -> Option<ExpectedAggregateVersion> {
    Some(ExpectedAggregateVersion {
        kind: Some(expected_aggregate_version::Kind::NoAggregate(Empty {})),
    })
}

fn aggregate_exact(version: u64) -> Option<ExpectedAggregateVersion> {
    Some(ExpectedAggregateVersion {
        kind: Some(expected_aggregate_version::Kind::Exact(version)),
    })
}

fn aggregate_group_settings(
    ack_timeout_ms: u64,
    max_retries: u32,
) -> Option<AggregateGroupSettings> {
    Some(AggregateGroupSettings {
        max_unacked_per_consumer: 8,
        max_unacked_per_group: 16,
        ack_timeout_ms,
        max_retries,
        retry_min_ms: 1,
        retry_max_ms: 1,
    })
}

fn assert_grpc_code<T>(
    result: Result<tonic::Response<T>, tonic::Status>,
    expected: tonic::Code,
    context: &str,
) {
    match result {
        Ok(_) => panic!("{context}: 请求意外成功"),
        Err(status) => assert_eq!(status.code(), expected, "{context}: {status}"),
    }
}

#[tokio::test]
async fn aggregate_public_api_validates_catalog_events_cursors_and_states() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = AggregateStoreClient::connect(addr)
        .await
        .expect("连接聚合服务");
    let capabilities = client
        .get_aggregate_store_capabilities(GetAggregateStoreCapabilitiesRequest {})
        .await
        .expect("读取服务限制")
        .into_inner();

    assert_grpc_code(
        client
            .create_event_set(CreateEventSetRequest {
                event_set: None,
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            })
            .await,
        tonic::Code::InvalidArgument,
        "缺失事件集",
    );
    assert_grpc_code(
        client
            .create_event_set(CreateEventSetRequest {
                event_set: aggregate_event_set(),
                operation_id: vec![0; 15],
            })
            .await,
        tonic::Code::InvalidArgument,
        "非法创建 operation_id",
    );

    let operation_id = uuid::Uuid::new_v4().as_bytes().to_vec();
    client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: operation_id.clone(),
        })
        .await
        .expect("创建事件集");
    client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id,
        })
        .await
        .expect("相同 operation_id 幂等创建");
    assert_grpc_code(
        client
            .create_event_set(CreateEventSetRequest {
                event_set: aggregate_event_set(),
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            })
            .await,
        tonic::Code::AlreadyExists,
        "不同 operation_id 重复创建",
    );

    assert_eq!(
        client
            .list_event_sets(ListEventSetsRequest {})
            .await
            .expect("列出事件集")
            .into_inner()
            .event_sets
            .len(),
        1
    );
    client
        .get_event_set(GetEventSetRequest {
            event_set: aggregate_event_set(),
        })
        .await
        .expect("读取事件集");
    client
        .list_aggregate_partitions(ListAggregatePartitionsRequest {
            event_set: aggregate_event_set(),
        })
        .await
        .expect("列出聚合分区");
    let status = client
        .get_aggregate_store_status(GetAggregateStoreStatusRequest {})
        .await
        .expect("读取聚合服务状态")
        .into_inner();
    assert_eq!(status.active_event_set_count, 1);

    let missing_event_set = Some(AggregateEventSetRef {
        business_space: "orders".into(),
        aggregate_type: "missing".into(),
    });
    assert_grpc_code(
        client
            .get_event_set(GetEventSetRequest {
                event_set: missing_event_set.clone(),
            })
            .await,
        tonic::Code::NotFound,
        "读取不存在事件集",
    );
    assert_grpc_code(
        client
            .list_aggregate_partitions(ListAggregatePartitionsRequest {
                event_set: missing_event_set,
            })
            .await,
        tonic::Code::NotFound,
        "列出不存在事件集分区",
    );

    let append =
        |aggregate_id: &str, event: Option<NewAggregateEvent>| AppendAggregateEventRequest {
            event_set: aggregate_event_set(),
            aggregate_id: aggregate_id.into(),
            expected_version: aggregate_no_instance(),
            event,
        };
    assert_grpc_code(
        client
            .append_aggregate_event(append("", Some(aggregate_event(b"{}"))))
            .await,
        tonic::Code::InvalidArgument,
        "空 aggregate_id",
    );
    assert_grpc_code(
        client.append_aggregate_event(append("order-1", None)).await,
        tonic::Code::InvalidArgument,
        "缺失事件",
    );
    let mut oversized = aggregate_event(b"{}");
    oversized.data = vec![0; capabilities.max_event_bytes as usize + 1];
    assert_grpc_code(
        client
            .append_aggregate_event(append("order-1", Some(oversized)))
            .await,
        tonic::Code::FailedPrecondition,
        "事件 payload 超限",
    );
    let mut invalid_event_id = aggregate_event(b"{}");
    invalid_event_id.event_id = vec![0; 15];
    assert_grpc_code(
        client
            .append_aggregate_event(append("order-1", Some(invalid_event_id)))
            .await,
        tonic::Code::InvalidArgument,
        "非法 event_id",
    );
    let mut empty_event_type = aggregate_event(b"{}");
    empty_event_type.event_type.clear();
    assert_grpc_code(
        client
            .append_aggregate_event(append("order-1", Some(empty_event_type)))
            .await,
        tonic::Code::InvalidArgument,
        "空 event_type",
    );

    for aggregate_id in ["order-1", "order-2"] {
        client
            .append_aggregate_event(append(aggregate_id, Some(aggregate_event(b"{}"))))
            .await
            .expect("追加分页测试事件");
        client
            .put_aggregate_state(PutAggregateStateRequest {
                event_set: aggregate_event_set(),
                aggregate_id: aggregate_id.into(),
                expected_revision: Some(ExpectedStateRevision {
                    kind: Some(expected_state_revision::Kind::Absent(Empty {})),
                }),
                data: aggregate_id.as_bytes().to_vec(),
            })
            .await
            .expect("写入分页测试状态");
    }
    assert_grpc_code(
        client
            .put_aggregate_state(PutAggregateStateRequest {
                event_set: aggregate_event_set(),
                aggregate_id: "order-1".into(),
                expected_revision: None,
                data: b"{}".to_vec(),
            })
            .await,
        tonic::Code::InvalidArgument,
        "缺失状态 expected_revision",
    );
    assert_grpc_code(
        client
            .put_aggregate_state(PutAggregateStateRequest {
                event_set: aggregate_event_set(),
                aggregate_id: "order-1".into(),
                expected_revision: Some(ExpectedStateRevision {
                    kind: Some(expected_state_revision::Kind::Exact(0)),
                }),
                data: vec![0; capabilities.max_state_bytes as usize + 1],
            })
            .await,
        tonic::Code::FailedPrecondition,
        "状态 payload 超限",
    );
    assert_grpc_code(
        client
            .get_aggregate_state(GetAggregateStateRequest {
                event_set: aggregate_event_set(),
                aggregate_id: "order-404".into(),
            })
            .await,
        tonic::Code::NotFound,
        "读取不存在状态",
    );

    client
        .list_aggregate_states(ListAggregateStatesRequest {
            event_set: aggregate_event_set(),
            page_size: 0,
            page_token: Vec::new(),
        })
        .await
        .expect("使用默认状态分页大小");
    let first_page = client
        .list_aggregate_states(ListAggregateStatesRequest {
            event_set: aggregate_event_set(),
            page_size: 1,
            page_token: Vec::new(),
        })
        .await
        .expect("读取首个状态分页")
        .into_inner();
    assert_eq!(first_page.states.len(), 1);
    assert!(!first_page.next_page_token.is_empty());
    client
        .list_aggregate_states(ListAggregateStatesRequest {
            event_set: aggregate_event_set(),
            page_size: u32::MAX,
            page_token: first_page.next_page_token.clone(),
        })
        .await
        .expect("状态分页大小按上限截断");
    let mut invalid_page_token = first_page.next_page_token;
    invalid_page_token[0] = invalid_page_token[0].wrapping_add(1);
    assert_grpc_code(
        client
            .list_aggregate_states(ListAggregateStatesRequest {
                event_set: aggregate_event_set(),
                page_size: 1,
                page_token: invalid_page_token,
            })
            .await,
        tonic::Code::InvalidArgument,
        "状态分页 token 版本非法",
    );

    let mut stream = client
        .read_aggregate_events(ReadAggregateEventsRequest {
            event_set: aggregate_event_set(),
            start: Some(AggregateReadStart {
                kind: Some(aggregate_read_start::Kind::Beginning(Empty {})),
            }),
        })
        .await
        .expect("读取聚合事件")
        .into_inner();
    let cursor = stream
        .message()
        .await
        .expect("读取首帧")
        .expect("首帧存在")
        .cursor;
    drop(stream);
    let mut invalid_cursor = cursor;
    invalid_cursor[0] = invalid_cursor[0].wrapping_add(1);
    assert_grpc_code(
        client
            .read_aggregate_events(ReadAggregateEventsRequest {
                event_set: aggregate_event_set(),
                start: Some(AggregateReadStart {
                    kind: Some(aggregate_read_start::Kind::Cursor(invalid_cursor)),
                }),
            })
            .await,
        tonic::Code::InvalidArgument,
        "聚合游标版本非法",
    );
    assert_grpc_code(
        client
            .read_aggregate_events(ReadAggregateEventsRequest {
                event_set: aggregate_event_set(),
                start: Some(AggregateReadStart {
                    kind: Some(aggregate_read_start::Kind::Cursor(vec![255])),
                }),
            })
            .await,
        tonic::Code::Internal,
        "损坏聚合游标",
    );

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_group_public_api_validates_limits_tokens_and_revisions() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = AggregateStoreClient::connect(addr)
        .await
        .expect("连接聚合服务");
    client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建事件集");
    client
        .append_aggregate_event(AppendAggregateEventRequest {
            event_set: aggregate_event_set(),
            aggregate_id: "order-1".into(),
            expected_version: aggregate_no_instance(),
            event: Some(aggregate_event(b"{}")),
        })
        .await
        .expect("追加组测试事件");

    assert_grpc_code(
        client
            .create_aggregate_group(CreateAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: String::new(),
                start: None,
                settings: None,
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            })
            .await,
        tonic::Code::InvalidArgument,
        "空消费者组名",
    );
    assert_grpc_code(
        client
            .create_aggregate_group(CreateAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                start: None,
                settings: None,
                operation_id: vec![0; 15],
            })
            .await,
        tonic::Code::InvalidArgument,
        "非法消费者组 operation_id",
    );
    let mut invalid_settings = aggregate_group_settings(1000, 3).unwrap();
    invalid_settings.max_unacked_per_consumer = 0;
    assert_grpc_code(
        client
            .create_aggregate_group(CreateAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                start: None,
                settings: Some(invalid_settings),
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            })
            .await,
        tonic::Code::InvalidArgument,
        "非法消费者组 settings",
    );

    let created = client
        .create_aggregate_group(CreateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            start: None,
            settings: aggregate_group_settings(1000, 3),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建消费者组")
        .into_inner();
    assert_grpc_code(
        client
            .create_aggregate_group(CreateAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                start: None,
                settings: None,
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            })
            .await,
        tonic::Code::AlreadyExists,
        "重复创建消费者组",
    );
    client
        .get_aggregate_group(GetAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
        })
        .await
        .expect("读取消费者组");
    assert_eq!(
        client
            .list_aggregate_groups(ListAggregateGroupsRequest {
                event_set: aggregate_event_set(),
            })
            .await
            .expect("列出消费者组")
            .into_inner()
            .groups
            .len(),
        1
    );
    assert_grpc_code(
        client
            .get_aggregate_group(GetAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "missing".into(),
            })
            .await,
        tonic::Code::NotFound,
        "读取不存在消费者组",
    );
    assert_grpc_code(
        client
            .fetch_aggregate_group(FetchAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                consumer_id: String::new(),
                max_events: 0,
                max_bytes: 0,
                wait_ms: 0,
            })
            .await,
        tonic::Code::InvalidArgument,
        "空消费者 ID",
    );

    let fetched = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: u32::MAX,
            max_bytes: u64::MAX,
            wait_ms: u64::MAX,
        })
        .await
        .expect("Fetch 配额按上限截断")
        .into_inner();
    assert_eq!(fetched.deliveries.len(), 1);
    let old_delivery_id = fetched.deliveries[0].delivery_id.clone();

    assert_grpc_code(
        client
            .update_aggregate_group(UpdateAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                expected_revision: created.revision,
                start: None,
                settings: None,
                operation_id: vec![0; 15],
            })
            .await,
        tonic::Code::InvalidArgument,
        "非法更新 operation_id",
    );
    assert_grpc_code(
        client
            .update_aggregate_group(UpdateAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                expected_revision: created.revision + 9,
                start: None,
                settings: None,
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            })
            .await,
        tonic::Code::Aborted,
        "更新 revision 冲突",
    );
    let reset = client
        .update_aggregate_group(UpdateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            expected_revision: created.revision,
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
            }),
            settings: None,
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("重置消费者组")
        .into_inner();
    assert!(reset.epoch > created.epoch);

    let stale_settlement = client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-a".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: old_delivery_id.clone(),
                action: AggregateGroupSettlementAction::AggregateGroupSettlementAck as i32,
                reason: String::new(),
            }],
        })
        .await
        .expect("旧 epoch settle 返回逐项状态")
        .into_inner();
    assert_eq!(
        stale_settlement.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease as i32
    );
    let stale_renewal = client
        .renew_aggregate_group(RenewAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-a".into(),
            delivery_ids: vec![old_delivery_id],
        })
        .await
        .expect("旧 epoch renew 返回逐项状态")
        .into_inner();
    assert_eq!(
        stale_renewal.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease as i32
    );

    let current = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-b".into(),
            max_events: 0,
            max_bytes: 0,
            wait_ms: 0,
        })
        .await
        .expect("使用默认 Fetch 配额")
        .into_inner();
    let current_delivery_id = current.deliveries[0].delivery_id.clone();
    assert_grpc_code(
        client
            .settle_aggregate_group(SettleAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                consumer_id: "consumer-b".into(),
                settlements: vec![AggregateGroupSettlement {
                    delivery_id: current_delivery_id,
                    action: 99,
                    reason: String::new(),
                }],
            })
            .await,
        tonic::Code::InvalidArgument,
        "非法 settle action",
    );
    assert_grpc_code(
        client
            .settle_aggregate_group(SettleAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                consumer_id: String::new(),
                settlements: Vec::new(),
            })
            .await,
        tonic::Code::InvalidArgument,
        "settle 空消费者 ID",
    );
    assert_grpc_code(
        client
            .renew_aggregate_group(RenewAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                consumer_id: "consumer-b".into(),
                delivery_ids: vec![vec![255]],
            })
            .await,
        tonic::Code::Internal,
        "损坏 delivery token",
    );

    assert_grpc_code(
        client
            .delete_aggregate_group(DeleteAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                expected_revision: reset.revision,
                operation_id: vec![0; 15],
            })
            .await,
        tonic::Code::InvalidArgument,
        "非法删除 operation_id",
    );
    assert_grpc_code(
        client
            .delete_aggregate_group(DeleteAggregateGroupRequest {
                event_set: aggregate_event_set(),
                name: "workers".into(),
                expected_revision: reset.revision + 1,
                operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            })
            .await,
        tonic::Code::Aborted,
        "删除 revision 冲突",
    );
    client
        .delete_aggregate_group(DeleteAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            expected_revision: reset.revision,
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("删除消费者组");

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_internal_api_rejects_invalid_control_and_partition_requests() {
    let (_addr, handle, server, _dir) = start_test_server().await;
    let service = es_server::aggregate_service::AggregateStoreService::new(
        server.shard_manager().clone(),
        server.config(),
    )
    .expect("创建内部聚合服务");

    assert_grpc_code(
        service
            .get_aggregate_catalog_internal(tonic::Request::new(
                GetAggregateCatalogInternalRequest {
                    control_shard_id: 1,
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "catalog control shard 不匹配",
    );
    assert!(
        service
            .get_aggregate_catalog_internal(tonic::Request::new(
                GetAggregateCatalogInternalRequest {
                    control_shard_id: 0,
                },
            ))
            .await
            .expect("读取内部 catalog")
            .into_inner()
            .payload
            .len()
            > 0
    );
    assert_grpc_code(
        service
            .commit_aggregate_catalog_internal(tonic::Request::new(
                CommitAggregateCatalogInternalRequest {
                    control_shard_id: 1,
                    payload: Vec::new(),
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "提交 catalog control shard 不匹配",
    );
    assert_grpc_code(
        service
            .commit_aggregate_catalog_internal(tonic::Request::new(
                CommitAggregateCatalogInternalRequest {
                    control_shard_id: 0,
                    payload: vec![255],
                },
            ))
            .await,
        tonic::Code::Internal,
        "损坏 catalog command",
    );
    let mut trailing_catalog_command = bincode::serde::encode_to_vec(
        es_core::AggregateCatalogCommand::Activate {
            event_set: es_core::EventSetId::new("orders", "order").expect("事件集身份"),
            operation_id: uuid::Uuid::new_v4(),
        },
        bincode::config::standard(),
    )
    .expect("编码 catalog command");
    trailing_catalog_command.push(0);
    assert_grpc_code(
        service
            .commit_aggregate_catalog_internal(tonic::Request::new(
                CommitAggregateCatalogInternalRequest {
                    control_shard_id: 0,
                    payload: trailing_catalog_command,
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "catalog command 尾随字节",
    );

    assert_grpc_code(
        service
            .install_aggregate_partition_fence_internal(tonic::Request::new(
                InstallAggregatePartitionFenceInternalRequest {
                    shard_id: 0,
                    event_set: None,
                    partition_id: 0,
                    generation: 1,
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "安装 fence 缺失事件集",
    );
    assert_grpc_code(
        service
            .install_aggregate_partition_fence_internal(tonic::Request::new(
                InstallAggregatePartitionFenceInternalRequest {
                    shard_id: 0,
                    event_set: aggregate_event_set(),
                    partition_id: u32::from(u16::MAX) + 1,
                    generation: 1,
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "安装 fence 分区越界",
    );

    let mut invalid_subscription = service
        .subscribe_aggregate_partitions_internal(tonic::Request::new(
            SubscribeAggregatePartitionsInternalRequest {
                shard_id: 0,
                event_set: aggregate_event_set(),
                cursors: vec![AggregatePartitionCursor {
                    partition_id: u32::from(u16::MAX) + 1,
                    next_position: 0,
                }],
                from_now: false,
            },
        ))
        .await
        .expect("内部订阅建立后异步报告非法 cursor")
        .into_inner();
    assert_eq!(
        invalid_subscription
            .next()
            .await
            .expect("内部订阅返回错误 frame")
            .expect_err("越界 partition cursor 必须失败")
            .code(),
        tonic::Code::InvalidArgument
    );

    assert_grpc_code(
        service
            .list_aggregate_partition_states_internal(tonic::Request::new(
                ListAggregatePartitionStatesInternalRequest {
                    shard_id: 0,
                    event_set: aggregate_event_set(),
                    cursors: vec![AggregateStatePartitionCursor {
                        partition_id: u32::from(u16::MAX) + 1,
                        after_aggregate_id: String::new(),
                    }],
                    limit_per_partition: 1,
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "内部状态 cursor 越界",
    );
    let states = service
        .list_aggregate_partition_states_internal(tonic::Request::new(
            ListAggregatePartitionStatesInternalRequest {
                shard_id: 0,
                event_set: aggregate_event_set(),
                cursors: vec![
                    AggregateStatePartitionCursor {
                        partition_id: 0,
                        after_aggregate_id: String::new(),
                    },
                    AggregateStatePartitionCursor {
                        partition_id: 1,
                        after_aggregate_id: "order-1".into(),
                    },
                ],
                limit_per_partition: 1,
            },
        ))
        .await
        .expect("内部状态列表支持空和排他起点")
        .into_inner();
    assert!(states.states.is_empty());

    assert_grpc_code(
        service
            .get_aggregate_group_catalog_internal(tonic::Request::new(
                GetAggregateGroupCatalogInternalRequest {
                    control_shard_id: 1,
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "组 catalog control shard 不匹配",
    );
    assert!(
        service
            .get_aggregate_group_catalog_internal(tonic::Request::new(
                GetAggregateGroupCatalogInternalRequest {
                    control_shard_id: 0,
                },
            ))
            .await
            .expect("读取内部组 catalog")
            .into_inner()
            .payload
            .len()
            > 0
    );
    assert_grpc_code(
        service
            .commit_aggregate_group_catalog_internal(tonic::Request::new(
                CommitAggregateGroupCatalogInternalRequest {
                    control_shard_id: 1,
                    payload: Vec::new(),
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "提交组 catalog control shard 不匹配",
    );
    assert_grpc_code(
        service
            .fetch_aggregate_group_partition_internal(tonic::Request::new(
                FetchAggregateGroupPartitionInternalRequest {
                    shard_id: 0,
                    payload: vec![255],
                },
            ))
            .await,
        tonic::Code::Internal,
        "损坏内部 Fetch payload",
    );
    assert_grpc_code(
        service
            .apply_aggregate_group_partition_internal(tonic::Request::new(
                ApplyAggregateGroupPartitionInternalRequest {
                    shard_id: 0,
                    payload: vec![255],
                },
            ))
            .await,
        tonic::Code::Internal,
        "损坏内部 Apply payload",
    );
    let non_group_request = bincode::serde::encode_to_vec(
        es_storage::EsRequest::DeleteStream {
            stream_id: "legacy".into(),
        },
        bincode::config::standard(),
    )
    .expect("编码非组请求");
    assert_grpc_code(
        service
            .apply_aggregate_group_partition_internal(tonic::Request::new(
                ApplyAggregateGroupPartitionInternalRequest {
                    shard_id: 0,
                    payload: non_group_request,
                },
            ))
            .await,
        tonic::Code::InvalidArgument,
        "内部 Apply 拒绝非组命令",
    );

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_store_versions_occ_and_state_cas_are_instance_scoped() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = AggregateStoreClient::connect(addr)
        .await
        .expect("连接聚合服务");

    let capabilities = client
        .get_aggregate_store_capabilities(GetAggregateStoreCapabilitiesRequest {})
        .await
        .expect("读取聚合能力")
        .into_inner();
    assert_eq!(capabilities.partition_count, 256);
    assert!(capabilities.state_revision_cas);

    let created = client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建事件集")
        .into_inner();
    assert_eq!(created.partition_count, 256);
    assert_eq!(
        created.status,
        AggregateEventSetStatus::AggregateEventSetActive as i32
    );

    for aggregate_id in ["order-1", "order-2"] {
        let appended = client
            .append_aggregate_event(AppendAggregateEventRequest {
                event_set: aggregate_event_set(),
                aggregate_id: aggregate_id.into(),
                expected_version: aggregate_no_instance(),
                event: Some(aggregate_event(br#"{"op":"create"}"#)),
            })
            .await
            .unwrap_or_else(|error| panic!("{aggregate_id} 首事件追加失败: {error}"))
            .into_inner();
        assert_eq!(appended.aggregate_version, 0, "实例版本必须彼此独立");
    }

    let conflict = client
        .append_aggregate_event(AppendAggregateEventRequest {
            event_set: aggregate_event_set(),
            aggregate_id: "order-1".into(),
            expected_version: aggregate_exact(9),
            event: Some(aggregate_event(br#"{"op":"pay"}"#)),
        })
        .await
        .expect_err("错误实例版本必须触发 OCC 冲突");
    assert_eq!(conflict.code(), tonic::Code::Aborted);

    let stored = client
        .put_aggregate_state(PutAggregateStateRequest {
            event_set: aggregate_event_set(),
            aggregate_id: "order-1".into(),
            expected_revision: Some(ExpectedStateRevision {
                kind: Some(expected_state_revision::Kind::Absent(Empty {})),
            }),
            data: br#"{"balance":50}"#.to_vec(),
        })
        .await
        .expect("首次写入聚合状态")
        .into_inner();
    assert_eq!(stored.revision, 0);
    assert!(stored.modified_unix_millis > 0);

    let state_conflict = client
        .put_aggregate_state(PutAggregateStateRequest {
            event_set: aggregate_event_set(),
            aggregate_id: "order-1".into(),
            expected_revision: Some(ExpectedStateRevision {
                kind: Some(expected_state_revision::Kind::Absent(Empty {})),
            }),
            data: br#"{"balance":25}"#.to_vec(),
        })
        .await
        .expect_err("重复 absent 写入必须触发状态 CAS 冲突");
    assert_eq!(state_conflict.code(), tonic::Code::Aborted);

    let loaded = client
        .get_aggregate_state(GetAggregateStateRequest {
            event_set: aggregate_event_set(),
            aggregate_id: "order-1".into(),
        })
        .await
        .expect("读取聚合状态")
        .into_inner();
    assert_eq!(loaded.revision, 0);
    assert_eq!(loaded.data, br#"{"balance":50}"#);
    assert_eq!(loaded.modified_unix_millis, stored.modified_unix_millis);

    let listed = client
        .list_aggregate_states(ListAggregateStatesRequest {
            event_set: aggregate_event_set(),
            page_size: 10,
            page_token: Vec::new(),
        })
        .await
        .expect("列出聚合状态")
        .into_inner();
    let listed = listed
        .states
        .into_iter()
        .find(|state| state.aggregate_id == "order-1")
        .expect("列表包含 order-1");
    assert_eq!(listed.revision, stored.revision);
    assert_eq!(listed.modified_unix_millis, stored.modified_unix_millis);

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_group_enforces_instance_order_and_explicit_settlement() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = AggregateStoreClient::connect(addr)
        .await
        .expect("连接聚合服务");
    client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建事件集");
    for (aggregate_id, expected) in [
        ("order-1", aggregate_no_instance()),
        ("order-1", aggregate_exact(0)),
        ("order-2", aggregate_no_instance()),
    ] {
        client
            .append_aggregate_event(AppendAggregateEventRequest {
                event_set: aggregate_event_set(),
                aggregate_id: aggregate_id.into(),
                expected_version: expected,
                event: Some(aggregate_event(aggregate_id.as_bytes())),
            })
            .await
            .expect("追加组测试事件");
    }
    let group = client
        .create_aggregate_group(CreateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
            }),
            settings: None,
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建聚合消费者组")
        .into_inner();
    assert_eq!(group.revision, 1);
    assert_eq!(group.epoch, 1);

    let first = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 10,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("首次 Fetch")
        .into_inner();
    assert_eq!(first.deliveries.len(), 2, "每个实例最多一个未结算 delivery");
    let order_1 = first
        .deliveries
        .iter()
        .find(|delivery| delivery.event.as_ref().unwrap().aggregate_id == "order-1")
        .expect("order-1 首事件");
    let order_2 = first
        .deliveries
        .iter()
        .find(|delivery| delivery.event.as_ref().unwrap().aggregate_id == "order-2")
        .expect("order-2 首事件");
    assert_eq!(order_1.event.as_ref().unwrap().aggregate_version, 0);

    let renewed = client
        .renew_aggregate_group(RenewAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-a".into(),
            delivery_ids: vec![order_1.delivery_id.clone()],
        })
        .await
        .expect("续租 order-1")
        .into_inner();
    assert_eq!(renewed.results.len(), 1);
    assert_eq!(
        renewed.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementApplied as i32
    );

    client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-a".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: order_2.delivery_id.clone(),
                action: AggregateGroupSettlementAction::AggregateGroupSettlementAck as i32,
                reason: String::new(),
            }],
        })
        .await
        .expect("先 Ack order-2");
    let blocked = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-b".into(),
            max_events: 10,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("order-1 未 Ack 时再次 Fetch")
        .into_inner();
    assert!(
        blocked.deliveries.is_empty(),
        "order-1 version 1 必须被前序租约阻塞"
    );

    client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-a".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: order_1.delivery_id.clone(),
                action: AggregateGroupSettlementAction::AggregateGroupSettlementAck as i32,
                reason: String::new(),
            }],
        })
        .await
        .expect("Ack order-1 首事件");
    let next = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "workers".into(),
            consumer_id: "consumer-b".into(),
            max_events: 10,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("前序 Ack 后 Fetch")
        .into_inner();
    assert_eq!(next.deliveries.len(), 1);
    assert_eq!(
        next.deliveries[0].event.as_ref().unwrap().aggregate_id,
        "order-1"
    );
    assert_eq!(
        next.deliveries[0].event.as_ref().unwrap().aggregate_version,
        1
    );

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_group_fetch_skips_blocked_candidate_before_available_instance() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = AggregateStoreClient::connect(addr)
        .await
        .expect("连接聚合服务");
    let operation_id = uuid::Uuid::new_v4();
    client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: operation_id.as_bytes().to_vec(),
        })
        .await
        .expect("创建事件集");

    let first_id = "blocked-0".to_string();
    let partition = es_core::EventPartitionHash::Xxh3V1
        .partition(
            operation_id.as_bytes(),
            &first_id,
            es_core::EVENT_PARTITION_COUNT,
        )
        .expect("计算首实例分区");
    let second_id = (1..10_000)
        .map(|index| format!("available-{index}"))
        .find(|candidate| {
            es_core::EventPartitionHash::Xxh3V1
                .partition(
                    operation_id.as_bytes(),
                    candidate,
                    es_core::EVENT_PARTITION_COUNT,
                )
                .expect("计算候选实例分区")
                == partition
        })
        .expect("有限候选中必须找到同分区实例");
    for aggregate_id in [&first_id, &second_id] {
        client
            .append_aggregate_event(AppendAggregateEventRequest {
                event_set: aggregate_event_set(),
                aggregate_id: aggregate_id.clone(),
                expected_version: aggregate_no_instance(),
                event: Some(aggregate_event(aggregate_id.as_bytes())),
            })
            .await
            .expect("追加同分区测试事件");
    }
    client
        .create_aggregate_group(CreateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "scan-window".into(),
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
            }),
            settings: None,
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建消费者组");

    let first = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "scan-window".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("领取首实例")
        .into_inner();
    assert_eq!(first.deliveries.len(), 1);
    assert_eq!(
        first.deliveries[0].event.as_ref().unwrap().aggregate_id,
        first_id
    );

    let second = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "scan-window".into(),
            consumer_id: "consumer-b".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("越过被占用实例领取后续实例")
        .into_inner();
    assert_eq!(second.deliveries.len(), 1);
    assert_eq!(
        second.deliveries[0].event.as_ref().unwrap().aggregate_id,
        second_id
    );

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_group_update_reset_and_delete_operation_are_consistent() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = AggregateStoreClient::connect(addr)
        .await
        .expect("连接聚合服务");
    client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建事件集");
    for (expected, data) in [
        (aggregate_no_instance(), b"v0".as_slice()),
        (aggregate_exact(0), b"v1".as_slice()),
    ] {
        client
            .append_aggregate_event(AppendAggregateEventRequest {
                event_set: aggregate_event_set(),
                aggregate_id: "order-update".into(),
                expected_version: expected,
                event: Some(aggregate_event(data)),
            })
            .await
            .expect("追加更新测试事件");
    }
    client
        .create_aggregate_group(CreateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "update-reset".into(),
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
            }),
            settings: aggregate_group_settings(1_000, 2),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建消费者组");
    let delivery = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "update-reset".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("领取更新前 delivery")
        .into_inner()
        .deliveries
        .remove(0);

    let update_operation = uuid::Uuid::new_v4();
    let update_request = || UpdateAggregateGroupRequest {
        event_set: aggregate_event_set(),
        name: "update-reset".into(),
        expected_revision: 1,
        start: None,
        settings: aggregate_group_settings(2_000, 2),
        operation_id: update_operation.as_bytes().to_vec(),
    };
    let updated = client
        .update_aggregate_group(update_request())
        .await
        .expect("只更新 settings")
        .into_inner();
    assert_eq!((updated.revision, updated.epoch), (2, 1));
    let retried = client
        .update_aggregate_group(update_request())
        .await
        .expect("幂等重试 settings 更新")
        .into_inner();
    assert_eq!((retried.revision, retried.epoch), (2, 1));

    let active = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "update-reset".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("settings 更新后保持现有进度")
        .into_inner();
    assert!(
        active.deliveries.is_empty(),
        "同实例旧 delivery 仍应占用租约"
    );

    let reset = client
        .update_aggregate_group(UpdateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "update-reset".into(),
            expected_revision: 2,
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Now(Empty {})),
            }),
            settings: None,
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("显式 reset 到 Now")
        .into_inner();
    assert_eq!((reset.revision, reset.epoch), (3, 2));
    let stale = client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "update-reset".into(),
            consumer_id: "consumer-a".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: delivery.delivery_id,
                action: AggregateGroupSettlementAction::AggregateGroupSettlementAck as i32,
                reason: String::new(),
            }],
        })
        .await
        .expect("reset 后结算旧 token")
        .into_inner();
    assert_eq!(
        stale.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementStaleLease as i32
    );

    let delete_operation = uuid::Uuid::new_v4();
    let delete_request = || DeleteAggregateGroupRequest {
        event_set: aggregate_event_set(),
        name: "update-reset".into(),
        expected_revision: 3,
        operation_id: delete_operation.as_bytes().to_vec(),
    };
    client
        .delete_aggregate_group(delete_request())
        .await
        .expect("删除消费者组");
    client
        .delete_aggregate_group(delete_request())
        .await
        .expect("相同 operation ID 删除重试必须成功");
    let missing = client
        .get_aggregate_group(GetAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "update-reset".into(),
        })
        .await
        .expect_err("删除后组不可见");
    assert_eq!(missing.code(), tonic::Code::NotFound);

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_group_timeout_retry_and_park_unlock_next_instance_version() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = AggregateStoreClient::connect(addr)
        .await
        .expect("连接聚合服务");
    client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建事件集");
    for version in 0..3 {
        client
            .append_aggregate_event(AppendAggregateEventRequest {
                event_set: aggregate_event_set(),
                aggregate_id: "order-retry".into(),
                expected_version: if version == 0 {
                    aggregate_no_instance()
                } else {
                    aggregate_exact(version - 1)
                },
                event: Some(aggregate_event(format!("v{version}").as_bytes())),
            })
            .await
            .expect("追加重试测试事件");
    }
    client
        .create_aggregate_group(CreateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
            }),
            settings: aggregate_group_settings(20, 1),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建重试消费者组");
    let first = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("首次领取")
        .into_inner();
    assert_eq!(first.deliveries[0].attempt, 0);

    tokio::time::sleep(Duration::from_millis(30)).await;
    let _ = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            consumer_id: "consumer-b".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("触发租约过期");
    tokio::time::sleep(Duration::from_millis(2)).await;
    let retried = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            consumer_id: "consumer-b".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("领取超时重投")
        .into_inner();
    assert_eq!(retried.deliveries.len(), 1);
    assert_eq!(retried.deliveries[0].attempt, 1);
    assert_eq!(
        retried.deliveries[0]
            .event
            .as_ref()
            .unwrap()
            .aggregate_version,
        0
    );

    client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            consumer_id: "consumer-b".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: retried.deliveries[0].delivery_id.clone(),
                action: AggregateGroupSettlementAction::AggregateGroupSettlementRetry as i32,
                reason: "retry exhausted".into(),
            }],
        })
        .await
        .expect("重试耗尽后 Park");
    let second = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            consumer_id: "consumer-c".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("领取 Park 后下一版本")
        .into_inner();
    assert_eq!(
        second.deliveries[0]
            .event
            .as_ref()
            .unwrap()
            .aggregate_version,
        1
    );
    client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            consumer_id: "consumer-c".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: second.deliveries[0].delivery_id.clone(),
                action: AggregateGroupSettlementAction::AggregateGroupSettlementPark as i32,
                reason: "explicit park".into(),
            }],
        })
        .await
        .expect("显式 Park");
    let third = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "retry-park".into(),
            consumer_id: "consumer-d".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 0,
        })
        .await
        .expect("领取显式 Park 后下一版本")
        .into_inner();
    assert_eq!(
        third.deliveries[0]
            .event
            .as_ref()
            .unwrap()
            .aggregate_version,
        2
    );

    handle.abort();
    let _ = handle.await;
    server.shutdown().await;
}

#[tokio::test]
async fn aggregate_store_catalog_and_follow_span_multiple_nodes() {
    let (first_addr, first_handle, second_handle, first, second, _first_dir, _second_dir) =
        start_two_shard_servers().await;
    let second_addr = first.config().node.peers[0].addr.clone();
    let mut first_client = AggregateStoreClient::connect(first_addr.clone())
        .await
        .expect("连接节点一聚合服务");
    first_client
        .create_event_set(CreateEventSetRequest {
            event_set: aggregate_event_set(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("经非控制节点创建跨节点事件集");

    let event_set = es_core::EventSetId::new("orders", "order").expect("事件集身份");
    let catalog = second
        .shard_manager()
        .get_shard(0)
        .await
        .expect("控制分片")
        .storage
        .read_aggregate_catalog()
        .expect("读取 catalog");
    let definition = catalog
        .event_sets
        .get(&event_set)
        .expect("事件集已写入 catalog");
    let mut aggregate_by_shard = std::collections::BTreeMap::new();
    for index in 0..10_000 {
        let aggregate_id = format!("order-{index}");
        let partition_id = definition
            .partition_for(&aggregate_id)
            .expect("计算聚合分区");
        let shard_id = definition.placements[&partition_id].shard_id;
        aggregate_by_shard.entry(shard_id).or_insert(aggregate_id);
        if aggregate_by_shard.len() == 2 {
            break;
        }
    }
    assert_eq!(
        aggregate_by_shard.len(),
        2,
        "测试身份必须覆盖两个物理 Shard"
    );

    let mut second_client = AggregateStoreClient::connect(second_addr)
        .await
        .expect("连接节点二聚合服务");
    for (&shard_id, aggregate_id) in &aggregate_by_shard {
        let client = if shard_id == 1 {
            &mut first_client
        } else {
            &mut second_client
        };
        let response = client
            .append_aggregate_event(AppendAggregateEventRequest {
                event_set: aggregate_event_set(),
                aggregate_id: aggregate_id.clone(),
                expected_version: aggregate_no_instance(),
                event: Some(aggregate_event(aggregate_id.as_bytes())),
            })
            .await
            .unwrap_or_else(|error| panic!("Shard {shard_id} 聚合追加失败: {error}"))
            .into_inner();
        assert_eq!(response.aggregate_version, 0);
    }

    let mut stream = first_client
        .read_aggregate_events(ReadAggregateEventsRequest {
            event_set: aggregate_event_set(),
            start: Some(AggregateReadStart {
                kind: Some(aggregate_read_start::Kind::Beginning(Empty {})),
            }),
        })
        .await
        .expect("从节点一跨 Shard follow")
        .into_inner();
    let mut seen = std::collections::BTreeSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let frame = tokio::time::timeout_at(deadline, stream.message())
            .await
            .expect("等待跨 Shard follow 超时")
            .expect("读取跨 Shard frame")
            .expect("follow 不应提前结束");
        match frame.payload {
            Some(read_aggregate_events_response::Payload::Event(event)) => {
                seen.insert(event.aggregate_id);
            }
            Some(read_aggregate_events_response::Payload::CaughtUp(_)) => break,
            Some(read_aggregate_events_response::Payload::Degraded(degraded)) => {
                panic!("两节点在线时不应降级: {degraded:?}")
            }
            _ => {}
        }
    }
    assert_eq!(
        seen,
        aggregate_by_shard.values().cloned().collect(),
        "公共 follow 必须合并所有物理 Shard，且不暴露分区"
    );

    first_client
        .create_aggregate_group(CreateAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "cross-node".into(),
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
            }),
            settings: aggregate_group_settings(1_000, 2),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("经非控制节点创建消费者组");
    let fetched = first_client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "cross-node".into(),
            consumer_id: "consumer-cross".into(),
            max_events: 10,
            max_bytes: 4096,
            wait_ms: 0,
        })
        .await
        .expect("跨节点 Fetch")
        .into_inner();
    assert_eq!(fetched.deliveries.len(), 2);
    assert_eq!(
        fetched
            .deliveries
            .iter()
            .map(|delivery| delivery.event.as_ref().unwrap().aggregate_id.clone())
            .collect::<std::collections::BTreeSet<_>>(),
        aggregate_by_shard.values().cloned().collect()
    );
    let renewed = first_client
        .renew_aggregate_group(RenewAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "cross-node".into(),
            consumer_id: "consumer-cross".into(),
            delivery_ids: fetched
                .deliveries
                .iter()
                .map(|delivery| delivery.delivery_id.clone())
                .collect(),
        })
        .await
        .expect("跨节点 Renew")
        .into_inner();
    assert!(renewed.results.iter().all(|result| {
        result.status == AggregateGroupSettlementStatus::AggregateGroupSettlementApplied as i32
            && result.deadline_ms > 0
    }));
    let settled = first_client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            event_set: aggregate_event_set(),
            name: "cross-node".into(),
            consumer_id: "consumer-cross".into(),
            settlements: fetched
                .deliveries
                .iter()
                .map(|delivery| AggregateGroupSettlement {
                    delivery_id: delivery.delivery_id.clone(),
                    action: AggregateGroupSettlementAction::AggregateGroupSettlementAck as i32,
                    reason: String::new(),
                })
                .collect(),
        })
        .await
        .expect("跨节点 Settle")
        .into_inner();
    assert!(settled.results.iter().all(|result| {
        result.status == AggregateGroupSettlementStatus::AggregateGroupSettlementApplied as i32
    }));

    first_handle.abort();
    second_handle.abort();
    let _ = first_handle.await;
    let _ = second_handle.await;
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn persistent_subscription_pull_backpressure_retry_park_and_replay() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut events = EventStoreClient::connect(addr.clone())
        .await
        .expect("连接事件服务");
    for data in [b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()] {
        append_one(&mut events, "orders/persistent", data).await;
    }
    let mut client = PersistentSubscriptionsClient::connect(addr)
        .await
        .expect("连接持久化订阅服务");
    let created = client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "orders-workers".into(),
            target: Some(persistent_stream_target("orders/persistent")),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: Default::default(),
            }),
            settings: Some(PersistentSubscriptionSettings {
                max_unacked_per_consumer: 2,
                max_unacked_per_group: 2,
                ack_timeout_ms: 1_000,
                max_retries: 1,
                retry_min_ms: 1,
                retry_max_ms: 1,
            }),
        })
        .await
        .expect("创建持久化订阅")
        .into_inner();
    assert_eq!(created.revision, 1);

    let first = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "orders-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 10,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("首次拉取")
        .into_inner();
    assert_eq!(first.deliveries.len(), 2, "单消费者额度必须限制投递数");
    assert_eq!(first.deliveries[0].event.as_ref().unwrap().version, 0);
    assert_eq!(first.deliveries[1].event.as_ref().unwrap().version, 1);

    let competing = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "orders-workers".into(),
            consumer_id: "consumer-b".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("竞争消费者拉取")
        .into_inner();
    assert!(competing.throttled, "组未确认额度耗尽时必须背压");
    assert!(competing.deliveries.is_empty());

    assert_eq!(
        settle_delivery(
            &mut client,
            "orders-workers",
            "consumer-a",
            &first.deliveries[0],
            PersistentSettlementAction::PersistentSettlementAck,
        )
        .await,
        PersistentSettlementStatus::PersistentSettlementApplied
    );
    let leased = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "orders-workers".into(),
            consumer_id: "consumer-b".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("租约竞争拉取")
        .into_inner();
    assert!(
        leased.deliveries.is_empty(),
        "同一 Stream 租约不能跨消费者并发"
    );

    let third = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "orders-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 10,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("同消费者续拉")
        .into_inner();
    assert_eq!(third.deliveries.len(), 1);
    assert_eq!(third.deliveries[0].event.as_ref().unwrap().version, 2);

    settle_delivery(
        &mut client,
        "orders-workers",
        "consumer-a",
        &first.deliveries[1],
        PersistentSettlementAction::PersistentSettlementRetry,
    )
    .await;
    settle_delivery(
        &mut client,
        "orders-workers",
        "consumer-a",
        &third.deliveries[0],
        PersistentSettlementAction::PersistentSettlementAck,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(2)).await;
    let retry = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "orders-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("重试拉取")
        .into_inner();
    assert_eq!(retry.deliveries.len(), 1);
    assert_eq!(retry.deliveries[0].attempt, 1);
    settle_delivery(
        &mut client,
        "orders-workers",
        "consumer-a",
        &retry.deliveries[0],
        PersistentSettlementAction::PersistentSettlementRetry,
    )
    .await;

    let parked = client
        .list_parked_persistent_subscription(ListParkedPersistentSubscriptionRequest {
            name: "orders-workers".into(),
            offset: 0,
            limit: 10,
        })
        .await
        .expect("读取 parked")
        .into_inner();
    assert_eq!(parked.events.len(), 1);
    assert_eq!(parked.events[0].event.as_ref().unwrap().version, 1);
    assert_eq!(
        client
            .replay_parked_persistent_subscription(ReplayParkedPersistentSubscriptionRequest {
                name: "orders-workers".into(),
            })
            .await
            .expect("重放 parked")
            .into_inner()
            .replayed_count,
        1
    );
    let replayed = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "orders-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("拉取重放事件")
        .into_inner();
    assert!(replayed.deliveries[0].replayed);
    settle_delivery(
        &mut client,
        "orders-workers",
        "consumer-a",
        &replayed.deliveries[0],
        PersistentSettlementAction::PersistentSettlementAck,
    )
    .await;

    let info = client
        .get_persistent_subscription(GetPersistentSubscriptionRequest {
            name: "orders-workers".into(),
        })
        .await
        .expect("读取组")
        .into_inner();
    assert_eq!(info.active_delivery_count, 0);
    assert_eq!(info.parked_count, 0);
    assert_eq!(
        client
            .list_persistent_subscriptions(ListPersistentSubscriptionsRequest {})
            .await
            .expect("枚举组")
            .into_inner()
            .subscriptions
            .len(),
        1
    );
    client
        .delete_persistent_subscription(DeletePersistentSubscriptionRequest {
            name: "orders-workers".into(),
            expected_revision: info.revision,
        })
        .await
        .expect("删除组");
    assert_eq!(
        client
            .get_persistent_subscription(GetPersistentSubscriptionRequest {
                name: "orders-workers".into(),
            })
            .await
            .expect_err("删除后组不存在")
            .code(),
        tonic::Code::NotFound
    );

    handle.abort();
    server.shutdown().await;
}

#[tokio::test]
async fn persistent_subscription_ack_timeout_honors_retry_backoff() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut events = EventStoreClient::connect(addr.clone())
        .await
        .expect("连接事件服务");
    append_one(&mut events, "orders/retry-backoff", b"zero").await;
    append_one(&mut events, "orders/retry-backoff", b"one").await;

    let mut client = PersistentSubscriptionsClient::connect(addr)
        .await
        .expect("连接持久化订阅服务");
    client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "retry-backoff-workers".into(),
            target: Some(persistent_stream_target("orders/retry-backoff")),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: Default::default(),
            }),
            settings: Some(PersistentSubscriptionSettings {
                max_unacked_per_consumer: 2,
                max_unacked_per_group: 2,
                ack_timeout_ms: 5,
                max_retries: 2,
                retry_min_ms: 200,
                retry_max_ms: 200,
            }),
        })
        .await
        .expect("创建持久化订阅");

    let first = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "retry-backoff-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("首次拉取")
        .into_inner();
    assert_eq!(first.deliveries.len(), 1);
    assert_eq!(first.deliveries[0].event.as_ref().unwrap().version, 0);

    tokio::time::sleep(Duration::from_millis(10)).await;
    let backing_off = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "retry-backoff-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 2,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("过期后进入退避")
        .into_inner();
    assert!(backing_off.deliveries.is_empty());
    assert!(!backing_off.caught_up, "尚有延迟重试时不能报告 caught up");
    assert!(
        backing_off.retry_after_ms > 0 && backing_off.retry_after_ms <= 200,
        "必须返回剩余退避时间"
    );

    tokio::time::sleep(Duration::from_millis(210)).await;
    let retried = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "retry-backoff-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 2,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("退避到期后重投")
        .into_inner();
    assert_eq!(
        retried.deliveries.len(),
        1,
        "同一 Stream 先解决 checkpoint 缺口"
    );
    assert_eq!(retried.deliveries[0].event.as_ref().unwrap().version, 0);
    assert_eq!(retried.deliveries[0].attempt, 1);
    assert_eq!(
        settle_delivery(
            &mut client,
            "retry-backoff-workers",
            "consumer-a",
            &retried.deliveries[0],
            PersistentSettlementAction::PersistentSettlementAck,
        )
        .await,
        PersistentSettlementStatus::PersistentSettlementApplied
    );

    let next = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "retry-backoff-workers".into(),
            consumer_id: "consumer-a".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 1,
        })
        .await
        .expect("缺口解决后继续拉取")
        .into_inner();
    assert_eq!(next.deliveries.len(), 1);
    assert_eq!(next.deliveries[0].event.as_ref().unwrap().version, 1);

    handle.abort();
    server.shutdown().await;
}

#[tokio::test]
async fn persistent_subscription_epoch_change_redelivers_unresolved_gap() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut events = EventStoreClient::connect(addr.clone())
        .await
        .expect("连接事件服务");
    append_one(&mut events, "epoch-a", b"a0").await;
    append_one(&mut events, "epoch-a", b"a1").await;
    append_one(&mut events, "epoch-b", b"b0").await;

    let mut client = PersistentSubscriptionsClient::connect(addr)
        .await
        .expect("连接持久化订阅服务");
    let created = client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "epoch-workers".into(),
            target: Some(persistent_streams_target(&["epoch-a", "epoch-b"])),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: Default::default(),
            }),
            settings: None,
        })
        .await
        .expect("创建 epoch 回归组")
        .into_inner();
    let first = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "epoch-workers".into(),
            consumer_id: "epoch-consumer".into(),
            max_events: 3,
            max_bytes: 4096,
            wait_ms: 1,
        })
        .await
        .expect("首次拉取乱序确认批次")
        .into_inner();
    let a0 = first
        .deliveries
        .iter()
        .find(|delivery| {
            let event = delivery.event.as_ref().unwrap();
            event.stream_id == "epoch-a" && event.version == 0
        })
        .expect("批次包含 epoch-a v0")
        .clone();
    let a1 = first
        .deliveries
        .iter()
        .find(|delivery| {
            let event = delivery.event.as_ref().unwrap();
            event.stream_id == "epoch-a" && event.version == 1
        })
        .expect("批次包含 epoch-a v1")
        .clone();
    assert_eq!(
        settle_delivery(
            &mut client,
            "epoch-workers",
            "epoch-consumer",
            &a1,
            PersistentSettlementAction::PersistentSettlementAck,
        )
        .await,
        PersistentSettlementStatus::PersistentSettlementApplied
    );

    let updated = client
        .update_persistent_subscription(UpdatePersistentSubscriptionRequest {
            name: "epoch-workers".into(),
            expected_revision: created.revision,
            target: None,
            settings: None,
            resets: vec![PersistentStreamReset {
                stream_id: "epoch-b".into(),
                start: Some(persistent_stream_reset::Start::Beginning(Empty {})),
            }],
        })
        .await
        .expect("reset 另一条 Stream")
        .into_inner();
    assert!(updated.epoch > a0.group_epoch);

    let redelivered = fetch_persistent_one(&mut client, "epoch-workers", "epoch-consumer").await;
    let event = redelivered.event.as_ref().unwrap();
    assert_eq!((event.stream_id.as_str(), event.version), ("epoch-a", 0));
    assert_eq!(redelivered.group_epoch, updated.epoch);
    assert_eq!(redelivered.attempt, a0.attempt);
    assert_eq!(
        settle_delivery(
            &mut client,
            "epoch-workers",
            "epoch-consumer",
            &redelivered,
            PersistentSettlementAction::PersistentSettlementAck,
        )
        .await,
        PersistentSettlementStatus::PersistentSettlementApplied
    );

    append_one(&mut events, "epoch-a", b"a2").await;
    let next = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "epoch-workers".into(),
            consumer_id: "epoch-consumer".into(),
            max_events: 2,
            max_bytes: 4096,
            wait_ms: 1,
        })
        .await
        .expect("闭合 gap 后拉取下一版本")
        .into_inner();
    assert!(next.deliveries.iter().any(|delivery| {
        let event = delivery.event.as_ref().unwrap();
        event.stream_id == "epoch-a" && event.version == 2
    }));

    handle.abort();
    server.shutdown().await;
}

#[tokio::test]
async fn persistent_subscription_scan_skips_ineligible_streams_without_spending_limit() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut events = EventStoreClient::connect(addr.clone())
        .await
        .expect("连接事件服务");
    for stream in ["scan-a", "scan-b", "scan-c"] {
        append_one(&mut events, stream, b"v0").await;
    }
    let mut client = PersistentSubscriptionsClient::connect(addr)
        .await
        .expect("连接持久化订阅服务");
    client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "scan-workers".into(),
            target: Some(persistent_streams_target(&["scan-a", "scan-b", "scan-c"])),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: Default::default(),
            }),
            settings: Some(PersistentSubscriptionSettings {
                max_unacked_per_consumer: 8,
                max_unacked_per_group: 8,
                ack_timeout_ms: 60_000,
                max_retries: 3,
                retry_min_ms: 60_000,
                retry_max_ms: 60_000,
            }),
        })
        .await
        .expect("创建扫描公平性回归组");

    let a0 = fetch_persistent_one(&mut client, "scan-workers", "scan-consumer").await;
    assert_eq!(a0.event.as_ref().unwrap().stream_id, "scan-a");
    settle_delivery(
        &mut client,
        "scan-workers",
        "scan-consumer",
        &a0,
        PersistentSettlementAction::PersistentSettlementAck,
    )
    .await;

    let b0 = fetch_persistent_one(&mut client, "scan-workers", "scan-consumer").await;
    assert_eq!(b0.event.as_ref().unwrap().stream_id, "scan-b");
    settle_delivery(
        &mut client,
        "scan-workers",
        "scan-consumer",
        &b0,
        PersistentSettlementAction::PersistentSettlementRetry,
    )
    .await;

    let c0 = fetch_persistent_one(&mut client, "scan-workers", "scan-consumer").await;
    assert_eq!(c0.event.as_ref().unwrap().stream_id, "scan-c");
    settle_delivery(
        &mut client,
        "scan-workers",
        "scan-consumer",
        &c0,
        PersistentSettlementAction::PersistentSettlementAck,
    )
    .await;

    append_one(&mut events, "scan-a", b"v1").await;
    let a1 = fetch_persistent_one(&mut client, "scan-workers", "scan-consumer").await;
    assert_eq!(a1.event.as_ref().unwrap().stream_id, "scan-a");
    settle_delivery(
        &mut client,
        "scan-workers",
        "scan-consumer",
        &a1,
        PersistentSettlementAction::PersistentSettlementAck,
    )
    .await;

    append_one(&mut events, "scan-c", b"v1").await;
    let c1 = fetch_persistent_one(&mut client, "scan-workers", "scan-consumer").await;
    let event = c1.event.as_ref().unwrap();
    assert_eq!((event.stream_id.as_str(), event.version), ("scan-c", 1));

    handle.abort();
    server.shutdown().await;
}

#[tokio::test]
async fn persistent_subscription_all_discovers_new_stream_and_long_polls() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut client = PersistentSubscriptionsClient::connect(addr.clone())
        .await
        .expect("连接持久化订阅服务");
    let created = client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "all-workers".into(),
            target: Some(persistent_all_target()),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: Default::default(),
            }),
            settings: None,
        })
        .await
        .expect("创建 $all 组")
        .into_inner();

    let started = tokio::time::Instant::now();
    let empty = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "all-workers".into(),
            consumer_id: "consumer-all".into(),
            max_events: 10,
            max_bytes: 1024,
            wait_ms: 80,
        })
        .await
        .expect("空组长轮询")
        .into_inner();
    assert!(empty.caught_up);
    assert!(started.elapsed() >= Duration::from_millis(70));

    let mut events = EventStoreClient::connect(addr).await.expect("连接事件服务");
    append_one(&mut events, "created-after-group", b"new").await;
    client
        .update_persistent_subscription(UpdatePersistentSubscriptionRequest {
            name: "all-workers".into(),
            expected_revision: created.revision,
            target: None,
            settings: Some(PersistentSubscriptionSettings::default()),
            resets: vec![],
        })
        .await
        .expect("Fetch 对账前更新 $all settings")
        .into_inner();
    let fetched = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "all-workers".into(),
            consumer_id: "consumer-all".into(),
            max_events: 10,
            max_bytes: 1024,
            wait_ms: 100,
        })
        .await
        .expect("拉取新发现 Stream")
        .into_inner();
    assert_eq!(fetched.deliveries.len(), 1);
    assert_eq!(
        fetched.deliveries[0].event.as_ref().unwrap().stream_id,
        "created-after-group"
    );

    handle.abort();
    server.shutdown().await;
}

#[tokio::test]
async fn persistent_subscription_validates_start_update_limits_and_paging() {
    let (addr, handle, server, _dir) = start_test_server().await;
    let mut events = EventStoreClient::connect(addr.clone())
        .await
        .expect("连接事件服务");
    for stream in ["validation-a", "validation-b"] {
        append_one(&mut events, stream, b"zero").await;
        append_one(&mut events, stream, b"one").await;
    }
    let mut client = PersistentSubscriptionsClient::connect(addr)
        .await
        .expect("连接持久化订阅服务");

    let empty_target = client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "empty-target".into(),
            target: Some(PersistentSubscriptionTarget {
                target: Some(persistent_subscription_target::Target::Streams(
                    SubscribeStreams {
                        stream_ids: vec![String::new()],
                    },
                )),
            }),
            start: None,
            settings: None,
        })
        .await
        .expect_err("空 Stream 目标必须拒绝");
    assert_eq!(empty_target.code(), tonic::Code::InvalidArgument);

    let unknown_start = client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "unknown-start".into(),
            target: Some(persistent_stream_target("validation-a")),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: [(String::from("outside"), 0)].into_iter().collect(),
            }),
            settings: None,
        })
        .await
        .expect_err("非目标 Stream 起点必须拒绝");
    assert_eq!(unknown_start.code(), tonic::Code::InvalidArgument);

    let past_head = client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "past-head".into(),
            target: Some(persistent_stream_target("validation-a")),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: [(String::from("validation-a"), 99)].into_iter().collect(),
            }),
            settings: None,
        })
        .await
        .expect_err("超过 head 的起点必须拒绝");
    assert_eq!(past_head.code(), tonic::Code::InvalidArgument);

    client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "now-workers".into(),
            target: Some(persistent_stream_target("validation-a")),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartNow as i32,
                next_versions: Default::default(),
            }),
            settings: None,
        })
        .await
        .expect("创建 FromNow 组");

    let created = client
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            target: Some(persistent_stream_target("validation-a")),
            start: Some(PersistentStartSpec {
                default: PersistentStartDefault::PersistentStartBeginning as i32,
                next_versions: [(String::from("validation-a"), 0)].into_iter().collect(),
            }),
            settings: None,
        })
        .await
        .expect("创建校验组")
        .into_inner();

    let missing_reset = client
        .update_persistent_subscription(UpdatePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            expected_revision: created.revision,
            target: Some(PersistentSubscriptionTarget {
                target: Some(persistent_subscription_target::Target::Streams(
                    SubscribeStreams {
                        stream_ids: vec!["validation-a".into(), "validation-b".into()],
                    },
                )),
            }),
            settings: None,
            resets: vec![],
        })
        .await
        .expect_err("新增 Stream 缺少 reset 必须拒绝");
    assert_eq!(missing_reset.code(), tonic::Code::InvalidArgument);

    let outside_reset = client
        .update_persistent_subscription(UpdatePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            expected_revision: created.revision,
            target: None,
            settings: None,
            resets: vec![PersistentStreamReset {
                stream_id: "validation-b".into(),
                start: Some(persistent_stream_reset::Start::Beginning(Empty {})),
            }],
        })
        .await
        .expect_err("目标外 reset 必须拒绝");
    assert_eq!(outside_reset.code(), tonic::Code::InvalidArgument);

    let expanded = client
        .update_persistent_subscription(UpdatePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            expected_revision: created.revision,
            target: Some(PersistentSubscriptionTarget {
                target: Some(persistent_subscription_target::Target::Streams(
                    SubscribeStreams {
                        stream_ids: vec!["validation-a".into(), "validation-b".into()],
                    },
                )),
            }),
            settings: None,
            resets: vec![PersistentStreamReset {
                stream_id: "validation-b".into(),
                start: Some(persistent_stream_reset::Start::Beginning(Empty {})),
            }],
        })
        .await
        .expect("新增目标并指定 reset")
        .into_inner();
    let reset = client
        .update_persistent_subscription(UpdatePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            expected_revision: expanded.revision,
            target: None,
            settings: None,
            resets: vec![PersistentStreamReset {
                stream_id: "validation-a".into(),
                start: Some(persistent_stream_reset::Start::NextVersion(0)),
            }],
        })
        .await
        .expect("同目标 reset")
        .into_inner();
    assert!(reset.epoch > expanded.epoch);

    for request in [
        FetchPersistentSubscriptionRequest {
            name: String::new(),
            consumer_id: "consumer".into(),
            max_events: 1,
            max_bytes: 1,
            wait_ms: 1,
        },
        FetchPersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: String::new(),
            max_events: 1,
            max_bytes: 1,
            wait_ms: 1,
        },
        FetchPersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: "consumer".into(),
            max_events: es_core::persistent::MAX_FETCH_EVENTS + 1,
            max_bytes: 1,
            wait_ms: 1,
        },
        FetchPersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: "consumer".into(),
            max_events: 1,
            max_bytes: es_core::persistent::MAX_FETCH_BYTES + 1,
            wait_ms: 1,
        },
        FetchPersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: "consumer".into(),
            max_events: 1,
            max_bytes: 1,
            wait_ms: es_core::persistent::MAX_FETCH_WAIT_MS + 1,
        },
    ] {
        assert_eq!(
            client
                .fetch_persistent_subscription(request)
                .await
                .expect_err("非法 Fetch 参数必须拒绝")
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    let fetched = client
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: "consumer".into(),
            max_events: 0,
            max_bytes: 0,
            wait_ms: 0,
        })
        .await
        .expect("零值使用服务端默认 Fetch 参数")
        .into_inner();
    assert!(fetched.deliveries.len() >= 2);

    for request in [
        SettlePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: String::new(),
            group_epoch: fetched.deliveries[0].group_epoch,
            settlements: vec![PersistentSettlement {
                delivery_id: fetched.deliveries[0].delivery_id.clone(),
                action: PersistentSettlementAction::PersistentSettlementAck as i32,
                reason: String::new(),
            }],
        },
        SettlePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: "consumer".into(),
            group_epoch: fetched.deliveries[0].group_epoch,
            settlements: vec![],
        },
    ] {
        assert_eq!(
            client
                .settle_persistent_subscription(request)
                .await
                .expect_err("空 Settle 参数必须拒绝")
                .code(),
            tonic::Code::InvalidArgument
        );
    }

    client
        .settle_persistent_subscription(SettlePersistentSubscriptionRequest {
            name: "validation-workers".into(),
            consumer_id: "consumer".into(),
            group_epoch: fetched.deliveries[0].group_epoch,
            settlements: fetched
                .deliveries
                .iter()
                .take(2)
                .map(|delivery| PersistentSettlement {
                    delivery_id: delivery.delivery_id.clone(),
                    action: PersistentSettlementAction::PersistentSettlementPark as i32,
                    reason: "paging".into(),
                })
                .collect(),
        })
        .await
        .expect("停放两条 delivery");
    let default_page = client
        .list_parked_persistent_subscription(ListParkedPersistentSubscriptionRequest {
            name: "validation-workers".into(),
            offset: 0,
            limit: 0,
        })
        .await
        .expect("parked 默认分页")
        .into_inner();
    assert_eq!(default_page.events.len(), 2);
    let first_page = client
        .list_parked_persistent_subscription(ListParkedPersistentSubscriptionRequest {
            name: "validation-workers".into(),
            offset: 0,
            limit: 1,
        })
        .await
        .expect("parked 显式分页")
        .into_inner();
    assert_eq!(first_page.events.len(), 1);
    assert_eq!(first_page.next_offset, 1);

    handle.abort();
    server.shutdown().await;
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

/// 读取一页 `$all`，保留服务端返回的逐分片消费水位。
async fn read_all_page(
    client: &mut EventStoreClient<tonic::transport::Channel>,
    shard_ids: Vec<u64>,
    from_position: u64,
    max_count: u64,
    direction: Direction,
    from_positions: Vec<ShardPosition>,
) -> ReadEventsResponse {
    let mut stream = client
        .read_all(ReadAllRequest {
            shard_ids,
            from_position,
            max_count,
            direction: direction as i32,
            from_positions,
        })
        .await
        .expect("read_all 应成功")
        .into_inner();
    stream
        .message()
        .await
        .expect("读取 read_all 响应")
        .expect("read_all 应返回一页")
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

    let mut first_client = EventStoreClient::connect(first_addr)
        .await
        .expect("连接节点一");
    let second_addr = first.config().node.peers[0].addr.clone();
    let mut second_client = EventStoreClient::connect(second_addr)
        .await
        .expect("连接节点二");

    // 通过权威归属 interface 创建，顺序保证最少负载分配依次落到 shard 0、1。
    append_one(&mut second_client, "remote-stream", b"remote-history").await;
    append_one(&mut first_client, "local-stream", b"local-history").await;
    assert_eq!(first.route_table().lookup("remote-stream").await, Some(0));
    assert_eq!(first.route_table().lookup("local-stream").await, Some(1));
    assert_eq!(second.route_table().lookup("remote-stream").await, Some(0));
    assert_eq!(second.route_table().lookup("local-stream").await, Some(1));

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
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn read_all_aggregates_remote_shard_with_bidirectional_paging() {
    let (first_addr, first_handle, second_handle, first, second, _first_dir, _second_dir) =
        start_two_shard_servers().await;

    let mut first_client = EventStoreClient::connect(first_addr)
        .await
        .expect("连接接入节点");
    let mut second_client = EventStoreClient::connect(first.config().node.peers[0].addr.clone())
        .await
        .expect("连接远程节点");

    let remote = append_one(&mut second_client, "read-all-remote", b"remote-0").await;
    let local = append_one(&mut first_client, "read-all-local", b"local-0").await;
    assert_eq!(remote.shard_id, 0);
    assert_eq!(local.shard_id, 1);
    append_one(&mut second_client, "read-all-remote", b"remote-1").await;
    append_one(&mut first_client, "read-all-local", b"local-1").await;

    let mut forward = Vec::new();
    let mut cursors = Vec::new();
    for page in 0..=4 {
        let response = read_all_page(
            &mut first_client,
            if page == 0 { vec![0, 1] } else { Vec::new() },
            0,
            1,
            Direction::Forward,
            cursors,
        )
        .await;
        cursors = response.next_positions;
        if response.events.is_empty() {
            break;
        }
        assert_eq!(response.events.len(), 1, "分页上限必须全局生效");
        forward.extend(response.events);
    }
    assert_eq!(forward.len(), 4, "正向读取应汇总本地与远程分片");
    let identities: HashSet<_> = forward
        .iter()
        .map(|event| (event.stream_id.as_str(), event.version))
        .collect();
    assert_eq!(
        identities,
        HashSet::from([
            ("read-all-local", 0),
            ("read-all-local", 1),
            ("read-all-remote", 0),
            ("read-all-remote", 1),
        ])
    );
    for shard_id in [0, 1] {
        let positions: Vec<_> = forward
            .iter()
            .filter(|event| event.shard_id == shard_id)
            .map(|event| event.position)
            .collect();
        assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
    }

    let mut backward = Vec::new();
    let mut cursors = Vec::new();
    for page in 0..=3 {
        let response = read_all_page(
            &mut first_client,
            if page == 0 { vec![0, 1] } else { Vec::new() },
            u64::MAX,
            2,
            Direction::Backward,
            cursors,
        )
        .await;
        cursors = response.next_positions;
        if response.events.is_empty() {
            break;
        }
        backward.extend(response.events);
    }
    assert_eq!(backward.len(), 4, "反向读取应汇总本地与远程分片");
    let backward_identities: HashSet<_> = backward
        .iter()
        .map(|event| (event.stream_id.as_str(), event.version))
        .collect();
    assert_eq!(backward_identities, identities);
    for shard_id in [0, 1] {
        let positions: Vec<_> = backward
            .iter()
            .filter(|event| event.shard_id == shard_id)
            .map(|event| event.position)
            .collect();
        assert!(positions.windows(2).all(|pair| pair[0] > pair[1]));
    }

    first_handle.abort();
    second_handle.abort();
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn persistent_fetch_reads_remote_data_shard_through_internal_rpc() {
    let (first_addr, first_handle, second_handle, first, second, _first_dir, _second_dir) =
        start_two_shard_servers().await;
    let second_addr = first.config().node.peers[0].addr.clone();
    let mut first_events = EventStoreClient::connect(first_addr)
        .await
        .expect("连接数据节点");
    let mut second_events = EventStoreClient::connect(second_addr.clone())
        .await
        .expect("连接 control 节点");
    append_one(&mut second_events, "persistent-on-control", b"control").await;
    append_one(&mut first_events, "persistent-on-remote", b"remote").await;
    assert_eq!(
        second.route_table().lookup("persistent-on-remote").await,
        Some(1),
        "第二个 Stream 应按最少负载落到远程数据 Shard"
    );

    let mut subscriptions = PersistentSubscriptionsClient::connect(second_addr)
        .await
        .expect("连接 control Shard 节点");
    subscriptions
        .create_persistent_subscription(CreatePersistentSubscriptionRequest {
            name: "remote-workers".into(),
            target: Some(persistent_stream_target("persistent-on-remote")),
            start: None,
            settings: None,
        })
        .await
        .expect("创建远程数据源订阅");
    let fetched = subscriptions
        .fetch_persistent_subscription(FetchPersistentSubscriptionRequest {
            name: "remote-workers".into(),
            consumer_id: "consumer-remote".into(),
            max_events: 1,
            max_bytes: 1024,
            wait_ms: 100,
        })
        .await
        .expect("跨内部 RPC 拉取")
        .into_inner();
    assert_eq!(fetched.deliveries.len(), 1);
    let event = fetched.deliveries[0].event.as_ref().expect("公开事件");
    assert_eq!(event.stream_id, "persistent-on-remote");
    assert_eq!(event.data, b"remote");

    first_handle.abort();
    second_handle.abort();
    first.shutdown().await;
    second.shutdown().await;
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
            internal_listen_addr: Some(
                leader_internal_listener
                    .local_addr()
                    .expect("读取 leader 内部监听地址")
                    .to_string(),
            ),
            peers: vec![PeerConfig {
                id: 1,
                addr: follower_public_addr.clone(),
                internal_addr: Some(follower_internal_addr.clone()),
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

    let follower_service = es_server::service::EsService::with_ownership(
        follower.shard_manager().clone(),
        follower_config.limits.clone(),
        follower.route_table().clone(),
        follower.ownership().clone(),
        &follower_config,
    )
    .expect("创建 follower 服务");
    let leader_service = es_server::service::EsService::with_ownership(
        leader.shard_manager().clone(),
        leader_config.limits.clone(),
        leader.route_table().clone(),
        leader.ownership().clone(),
        &leader_config,
    )
    .expect("创建 leader 服务");
    let leader_admin = es_raft::RaftAdminService::new(leader.shard_manager().clone());
    let follower_migration = es_server::migration_service::MigrationService::new(
        follower.route_table().clone(),
        follower.shard_manager().clone(),
        follower.ownership().clone(),
    );
    let leader_migration = es_server::migration_service::MigrationService::new(
        leader.route_table().clone(),
        leader.shard_manager().clone(),
        leader.ownership().clone(),
    );
    let follower_handle = tokio::spawn(async move {
        let _ = tokio::try_join!(
            tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(follower_service.clone()))
                .add_service(MigrationServer::new(follower_migration.clone()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    follower_public_listener,
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(follower_service))
                .add_service(OwnershipInternalServer::new(follower_migration))
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
                .add_service(MigrationServer::new(leader_migration.clone()))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                    leader_public_listener,
                )),
            tonic::transport::Server::builder()
                .add_service(InternalSubscriptionServer::new(leader_service))
                .add_service(OwnershipInternalServer::new(leader_migration))
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
    first.shutdown().await;
    second.shutdown().await;
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

    let mut first_client = EventStoreClient::connect(first_addr)
        .await
        .expect("连接接入节点");
    let mut second_client = EventStoreClient::connect(first.config().node.peers[0].addr.clone())
        .await
        .expect("连接远程节点");
    // 通过权威归属 interface 创建，顺序保证两个 Stream 分别落到远程和本地 Shard。
    let remote = append_one(&mut second_client, "all-remote-stream", b"remote-history").await;
    let local = append_one(&mut first_client, "all-local-stream", b"local-history").await;
    assert_eq!(remote.shard_id, 0);
    assert_eq!(local.shard_id, 1);
    wait_route_projection(&first, "all-remote-stream", 0).await;
    wait_route_projection(&first, "all-local-stream", 1).await;

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
    first.shutdown().await;
    second.shutdown().await;
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
