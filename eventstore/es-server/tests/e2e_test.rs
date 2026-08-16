//! AggregateStore 真实 gRPC 端到端测试。

use std::collections::BTreeSet;
use std::time::Duration;

use es_proto::eventstore::aggregate_store_client::AggregateStoreClient;
use es_proto::eventstore::aggregate_store_server::AggregateStoreServer;
use es_proto::eventstore::*;
use es_server::Server;
use es_server::config::{Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig};

struct TestServer {
    address: String,
    task: tokio::task::JoinHandle<()>,
    server: Server,
    _directory: tempfile::TempDir,
}

impl TestServer {
    async fn start() -> Self {
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
            server
                .shard_manager()
                .get_shard(shard_id)
                .await
                .expect("读取 Shard")
                .raft
                .initialize(BTreeSet::from([1]))
                .await
                .expect("初始化单节点 Raft");
            wait_leader(&server, shard_id).await;
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定端口");
        let address = format!("http://{}", listener.local_addr().expect("读取端口"));
        let service = es_server::aggregate_service::AggregateStoreService::new(
            server.shard_manager().clone(),
            &config,
        )
        .expect("创建 AggregateStore 服务");
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(AggregateStoreServer::new(service))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .expect("gRPC 服务退出");
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        Self {
            address,
            task,
            server,
            _directory: directory,
        }
    }

    async fn stop(self) {
        self.task.abort();
        let _ = self.task.await;
        self.server.shutdown().await;
    }
}

async fn wait_leader(server: &Server, shard_id: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("读取 Shard");
        if shard.raft.metrics().borrow().state.is_leader() {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "等待 leader 超时");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn aggregate_type() -> Option<AggregateTypeRef> {
    Some(AggregateTypeRef {
        business_space: "orders".into(),
        aggregate_type: "order".into(),
    })
}

fn event(id: uuid::Uuid, data: &[u8]) -> NewAggregateEvent {
    NewAggregateEvent {
        event_id: id.as_bytes().to_vec(),
        event_type: "OrderChanged".into(),
        data: data.to_vec(),
        metadata: b"{}".to_vec(),
    }
}

fn expected(kind: expected_aggregate_version::Kind) -> Option<ExpectedAggregateVersion> {
    Some(ExpectedAggregateVersion { kind: Some(kind) })
}

#[tokio::test]
async fn catalog_append_follow_and_state_roundtrip() {
    let test = TestServer::start().await;
    let mut client = AggregateStoreClient::connect(test.address.clone())
        .await
        .expect("连接 AggregateStore");

    let operation_id = uuid::Uuid::new_v4().as_bytes().to_vec();
    let registered = client
        .register_aggregate_type(RegisterAggregateTypeRequest {
            aggregate_type: aggregate_type(),
            operation_id: operation_id.clone(),
        })
        .await
        .expect("注册 AggregateType")
        .into_inner();
    assert_eq!(registered.partition_count, 256);
    client
        .register_aggregate_type(RegisterAggregateTypeRequest {
            aggregate_type: aggregate_type(),
            operation_id,
        })
        .await
        .expect("注册请求幂等");
    assert_eq!(
        client
            .list_aggregate_types(ListAggregateTypesRequest {})
            .await
            .expect("枚举 AggregateType")
            .into_inner()
            .aggregate_types
            .len(),
        1
    );
    client
        .get_aggregate_type(GetAggregateTypeRequest {
            aggregate_type: aggregate_type(),
        })
        .await
        .expect("查询 AggregateType");

    let event_id = uuid::Uuid::new_v4();
    let first = client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-1".into(),
            expected_version: expected(expected_aggregate_version::Kind::NoAggregate(Empty {})),
            event: Some(event(event_id, br#"{"step":1}"#)),
        })
        .await
        .expect("追加首事件")
        .into_inner();
    assert_eq!(first.aggregate_version, 0);
    let repeated = client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-1".into(),
            expected_version: expected(expected_aggregate_version::Kind::NoAggregate(Empty {})),
            event: Some(event(event_id, br#"{"step":1}"#)),
        })
        .await
        .expect("同一 event_id 幂等重试")
        .into_inner();
    assert_eq!(repeated.aggregate_version, 0);
    let conflict = client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-1".into(),
            expected_version: expected(expected_aggregate_version::Kind::NoAggregate(Empty {})),
            event: Some(event(uuid::Uuid::new_v4(), br#"{"step":2}"#)),
        })
        .await
        .expect_err("OCC 冲突必须失败");
    assert_eq!(conflict.code(), tonic::Code::FailedPrecondition);

    let state = client
        .put_aggregate_state(PutAggregateStateRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-1".into(),
            expected_revision: Some(ExpectedStateRevision {
                kind: Some(expected_state_revision::Kind::Absent(Empty {})),
            }),
            data: br#"{"status":"open"}"#.to_vec(),
        })
        .await
        .expect("写入状态")
        .into_inner();
    assert_eq!(state.revision, 0);
    let state_conflict = client
        .put_aggregate_state(PutAggregateStateRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-1".into(),
            expected_revision: Some(ExpectedStateRevision {
                kind: Some(expected_state_revision::Kind::Absent(Empty {})),
            }),
            data: Vec::new(),
        })
        .await
        .expect_err("状态 CAS 冲突必须失败");
    assert_eq!(state_conflict.code(), tonic::Code::FailedPrecondition);

    let mut feed = client
        .follow_aggregate_type_events(FollowAggregateTypeEventsRequest {
            aggregate_type: aggregate_type(),
            start: Some(AggregateFollowStart {
                kind: Some(aggregate_follow_start::Kind::Beginning(Empty {})),
            }),
        })
        .await
        .expect("跟随 AggregateType feed")
        .into_inner();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut events = Vec::new();
    loop {
        let frame = tokio::time::timeout_at(deadline, feed.message())
            .await
            .expect("等待 feed 超时")
            .expect("读取 feed")
            .expect("feed 不应提前结束");
        match frame.payload {
            Some(follow_aggregate_type_events_response::Payload::Event(event)) => {
                events.push(event)
            }
            Some(follow_aggregate_type_events_response::Payload::CaughtUp(_)) => break,
            Some(follow_aggregate_type_events_response::Payload::Degraded(value)) => {
                panic!("单节点 feed 不应降级：{value:?}")
            }
            Some(follow_aggregate_type_events_response::Payload::Recovered(_)) => {}
            None => {}
        }
    }
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].aggregate_id, "order-1");
    assert_eq!(events[0].aggregate_version, 0);
    test.stop().await;
}

#[tokio::test]
async fn aggregate_group_fetch_and_settle_roundtrip() {
    let test = TestServer::start().await;
    let mut client = AggregateStoreClient::connect(test.address.clone())
        .await
        .expect("连接 AggregateStore");
    client
        .register_aggregate_type(RegisterAggregateTypeRequest {
            aggregate_type: aggregate_type(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("注册 AggregateType");
    client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-group".into(),
            expected_version: expected(expected_aggregate_version::Kind::NoAggregate(Empty {})),
            event: Some(event(uuid::Uuid::new_v4(), b"{}")),
        })
        .await
        .expect("追加待消费事件");
    client
        .create_aggregate_group(CreateAggregateGroupRequest {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            start: Some(AggregateGroupStart {
                kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
            }),
            settings: Some(AggregateGroupSettings {
                max_unacked_per_consumer: 4,
                max_unacked_per_group: 8,
                ack_timeout_ms: 5_000,
                max_retries: 3,
                retry_min_ms: 10,
                retry_max_ms: 100,
            }),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("创建消费者组");
    let fetched = client
        .fetch_aggregate_group(FetchAggregateGroupRequest {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            consumer_id: "worker-1".into(),
            max_events: 10,
            max_bytes: 4096,
            wait_ms: 0,
        })
        .await
        .expect("拉取消费事件")
        .into_inner();
    assert_eq!(fetched.deliveries.len(), 1);
    let wrong_consumer = client
        .renew_aggregate_group(RenewAggregateGroupRequest {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            consumer_id: "worker-2".into(),
            delivery_ids: vec![fetched.deliveries[0].delivery_id.clone()],
        })
        .await
        .expect("错误消费者续租返回逐项状态")
        .into_inner();
    assert_eq!(
        wrong_consumer.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementWrongConsumer as i32
    );
    let renewed = client
        .renew_aggregate_group(RenewAggregateGroupRequest {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            consumer_id: "worker-1".into(),
            delivery_ids: vec![fetched.deliveries[0].delivery_id.clone()],
        })
        .await
        .expect("续租消费事件")
        .into_inner();
    assert_eq!(
        renewed.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementApplied as i32
    );
    assert!(renewed.results[0].deadline_ms > 0);
    let invalid_action = client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            consumer_id: "worker-1".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: fetched.deliveries[0].delivery_id.clone(),
                action: i32::MAX,
                reason: String::new(),
            }],
        })
        .await
        .expect_err("非法结算动作必须拒绝");
    assert_eq!(invalid_action.code(), tonic::Code::InvalidArgument);
    let settled = client
        .settle_aggregate_group(SettleAggregateGroupRequest {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            consumer_id: "worker-1".into(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: fetched.deliveries[0].delivery_id.clone(),
                action: AggregateGroupSettlementAction::AggregateGroupSettlementAck as i32,
                reason: String::new(),
            }],
        })
        .await
        .expect("确认消费事件")
        .into_inner();
    assert_eq!(
        settled.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementApplied as i32
    );
    let settled_again = client
        .renew_aggregate_group(RenewAggregateGroupRequest {
            aggregate_type: aggregate_type(),
            name: "workers".into(),
            consumer_id: "worker-1".into(),
            delivery_ids: vec![fetched.deliveries[0].delivery_id.clone()],
        })
        .await
        .expect("已结算投递续租返回逐项状态")
        .into_inner();
    assert_eq!(
        settled_again.results[0].status,
        AggregateGroupSettlementStatus::AggregateGroupSettlementAlreadySettled as i32
    );
    test.stop().await;
}

#[tokio::test]
async fn grpc_rejects_invalid_aggregate_requests_before_raft() {
    let test = TestServer::start().await;
    let mut client = AggregateStoreClient::connect(test.address.clone())
        .await
        .expect("连接 AggregateStore");
    client
        .register_aggregate_type(RegisterAggregateTypeRequest {
            aggregate_type: aggregate_type(),
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .await
        .expect("注册 AggregateType");

    let missing_event = client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-invalid".into(),
            expected_version: expected(expected_aggregate_version::Kind::Any(Empty {})),
            event: None,
        })
        .await
        .expect_err("缺少 event 必须拒绝");
    assert_eq!(missing_event.code(), tonic::Code::InvalidArgument);

    let mut invalid_uuid = event(uuid::Uuid::new_v4(), b"{}");
    invalid_uuid.event_id = vec![1, 2, 3];
    let invalid_uuid = client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-invalid".into(),
            expected_version: expected(expected_aggregate_version::Kind::Any(Empty {})),
            event: Some(invalid_uuid),
        })
        .await
        .expect_err("非法 UUID 必须拒绝");
    assert_eq!(invalid_uuid.code(), tonic::Code::InvalidArgument);

    let mut empty_event_type = event(uuid::Uuid::new_v4(), b"{}");
    empty_event_type.event_type.clear();
    let empty_event_type = client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-invalid".into(),
            expected_version: expected(expected_aggregate_version::Kind::Any(Empty {})),
            event: Some(empty_event_type),
        })
        .await
        .expect_err("空事件类型必须拒绝");
    assert_eq!(empty_event_type.code(), tonic::Code::InvalidArgument);

    let oversized = client
        .append_aggregate_event(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-invalid".into(),
            expected_version: expected(expected_aggregate_version::Kind::Any(Empty {})),
            event: Some(event(
                uuid::Uuid::new_v4(),
                &vec![0; es_core::limits::MAX_EVENT_PAYLOAD_BYTES + 1],
            )),
        })
        .await
        .expect_err("超限事件必须拒绝");
    assert_eq!(oversized.code(), tonic::Code::FailedPrecondition);

    let oversized_state = client
        .put_aggregate_state(PutAggregateStateRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-invalid".into(),
            expected_revision: Some(ExpectedStateRevision {
                kind: Some(expected_state_revision::Kind::Absent(Empty {})),
            }),
            data: vec![0; es_core::limits::MAX_EVENT_PAYLOAD_BYTES + 1],
        })
        .await
        .expect_err("超限状态必须拒绝");
    assert_eq!(oversized_state.code(), tonic::Code::FailedPrecondition);

    for page_size in [0, u32::MAX] {
        client
            .list_aggregate_states(ListAggregateStatesRequest {
                aggregate_type: aggregate_type(),
                page_size,
                page_token: Vec::new(),
            })
            .await
            .expect("默认值与上限分页均有效");
    }
    let missing_type = client
        .get_aggregate_type(GetAggregateTypeRequest {
            aggregate_type: Some(AggregateTypeRef {
                business_space: "orders".into(),
                aggregate_type: "missing".into(),
            }),
        })
        .await
        .expect_err("不存在类型必须返回 NotFound");
    assert_eq!(missing_type.code(), tonic::Code::NotFound);
    test.stop().await;
}
