//! AggregateStore SDK 的 leader 重定向与 cursor 自动续读测试。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use es_client::AggregateStoreClient;
use es_proto::eventstore::aggregate_store_server::{AggregateStore, AggregateStoreServer};
use es_proto::eventstore::*;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct StubState {
    append_calls: usize,
    append_queue: VecDeque<Result<AppendAggregateEventResponse, Status>>,
    read_requests: Vec<FollowAggregateTypeEventsRequest>,
    read_streams: VecDeque<Vec<Result<FollowAggregateTypeEventsResponse, Status>>>,
    put_state_calls: usize,
    put_state_queue: VecDeque<Result<PutAggregateStateResponse, Status>>,
    fetch_calls: usize,
    fetch_queue: VecDeque<Result<FetchAggregateGroupResponse, Status>>,
}

#[derive(Clone)]
struct StubServer {
    state: Arc<Mutex<StubState>>,
}

#[tonic::async_trait]
impl AggregateStore for StubServer {
    type FollowAggregateTypeEventsStream =
        ReceiverStream<Result<FollowAggregateTypeEventsResponse, Status>>;

    async fn get_aggregate_store_capabilities(
        &self,
        _request: Request<GetAggregateStoreCapabilitiesRequest>,
    ) -> Result<Response<AggregateStoreCapabilities>, Status> {
        Ok(Response::new(AggregateStoreCapabilities {
            api_version: "1.0".into(),
            partition_count: 256,
            max_event_bytes: 1024,
            max_state_bytes: 2048,
            state_revision_cas: true,
            explicit_group_settlement: true,
            state_modified_time: true,
        }))
    }

    async fn register_aggregate_type(
        &self,
        _request: Request<RegisterAggregateTypeRequest>,
    ) -> Result<Response<AggregateTypeInfo>, Status> {
        Ok(Response::new(AggregateTypeInfo::default()))
    }

    async fn list_aggregate_types(
        &self,
        _request: Request<ListAggregateTypesRequest>,
    ) -> Result<Response<ListAggregateTypesResponse>, Status> {
        Ok(Response::new(ListAggregateTypesResponse {
            aggregate_types: vec![AggregateTypeInfo::default()],
        }))
    }

    async fn get_aggregate_type(
        &self,
        _request: Request<GetAggregateTypeRequest>,
    ) -> Result<Response<AggregateTypeInfo>, Status> {
        Ok(Response::new(AggregateTypeInfo::default()))
    }

    async fn append_aggregate_event(
        &self,
        _request: Request<AppendAggregateEventRequest>,
    ) -> Result<Response<AppendAggregateEventResponse>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.append_calls += 1;
        match state.append_queue.pop_front() {
            Some(Ok(response)) => Ok(Response::new(response)),
            Some(Err(status)) => Err(status),
            None => Ok(Response::new(AppendAggregateEventResponse {
                aggregate_version: 7,
            })),
        }
    }

    async fn follow_aggregate_type_events(
        &self,
        request: Request<FollowAggregateTypeEventsRequest>,
    ) -> Result<Response<Self::FollowAggregateTypeEventsStream>, Status> {
        let items = {
            let mut state = self.state.lock().expect("stub 锁");
            state.read_requests.push(request.into_inner());
            state.read_streams.pop_front().unwrap_or_default()
        };
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tokio::spawn(async move {
            for item in items {
                if tx.send(item).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn list_aggregate_states(
        &self,
        _request: Request<ListAggregateStatesRequest>,
    ) -> Result<Response<ListAggregateStatesResponse>, Status> {
        Ok(Response::new(ListAggregateStatesResponse {
            states: vec![AggregateStateInfo {
                aggregate_id: "order-1".into(),
                revision: 3,
                modified_unix_millis: 1_000,
            }],
            next_page_token: Vec::new(),
        }))
    }

    async fn get_aggregate_state(
        &self,
        _request: Request<GetAggregateStateRequest>,
    ) -> Result<Response<GetAggregateStateResponse>, Status> {
        Ok(Response::new(GetAggregateStateResponse {
            revision: 3,
            data: b"{}".to_vec(),
            modified_unix_millis: 1_000,
        }))
    }

    async fn put_aggregate_state(
        &self,
        _request: Request<PutAggregateStateRequest>,
    ) -> Result<Response<PutAggregateStateResponse>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.put_state_calls += 1;
        match state.put_state_queue.pop_front() {
            Some(Ok(response)) => Ok(Response::new(response)),
            Some(Err(status)) => Err(status),
            None => Ok(Response::new(PutAggregateStateResponse {
                revision: 4,
                modified_unix_millis: 2_000,
            })),
        }
    }

    async fn get_aggregate_store_status(
        &self,
        _request: Request<GetAggregateStoreStatusRequest>,
    ) -> Result<Response<AggregateStoreStatus>, Status> {
        Ok(Response::new(AggregateStoreStatus {
            catalog_revision: 1,
            aggregate_type_count: 1,
            registering_aggregate_type_count: 0,
            active_aggregate_type_count: 1,
        }))
    }

    async fn list_aggregate_partitions(
        &self,
        _request: Request<ListAggregatePartitionsRequest>,
    ) -> Result<Response<ListAggregatePartitionsResponse>, Status> {
        Ok(Response::new(ListAggregatePartitionsResponse {
            partitions: vec![AggregatePartitionInfo::default()],
        }))
    }

    async fn create_aggregate_group(
        &self,
        _request: Request<CreateAggregateGroupRequest>,
    ) -> Result<Response<AggregateGroupInfo>, Status> {
        Ok(Response::new(AggregateGroupInfo::default()))
    }

    async fn update_aggregate_group(
        &self,
        _request: Request<UpdateAggregateGroupRequest>,
    ) -> Result<Response<AggregateGroupInfo>, Status> {
        Ok(Response::new(AggregateGroupInfo::default()))
    }

    async fn delete_aggregate_group(
        &self,
        _request: Request<DeleteAggregateGroupRequest>,
    ) -> Result<Response<Empty>, Status> {
        Ok(Response::new(Empty {}))
    }

    async fn get_aggregate_group(
        &self,
        _request: Request<GetAggregateGroupRequest>,
    ) -> Result<Response<AggregateGroupInfo>, Status> {
        Ok(Response::new(AggregateGroupInfo::default()))
    }

    async fn list_aggregate_groups(
        &self,
        _request: Request<ListAggregateGroupsRequest>,
    ) -> Result<Response<ListAggregateGroupsResponse>, Status> {
        Ok(Response::new(ListAggregateGroupsResponse {
            groups: vec![AggregateGroupInfo::default()],
        }))
    }

    async fn fetch_aggregate_group(
        &self,
        _request: Request<FetchAggregateGroupRequest>,
    ) -> Result<Response<FetchAggregateGroupResponse>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.fetch_calls += 1;
        match state.fetch_queue.pop_front() {
            Some(Ok(response)) => Ok(Response::new(response)),
            Some(Err(status)) => Err(status),
            None => Ok(Response::new(FetchAggregateGroupResponse::default())),
        }
    }

    async fn settle_aggregate_group(
        &self,
        _request: Request<SettleAggregateGroupRequest>,
    ) -> Result<Response<SettleAggregateGroupResponse>, Status> {
        Ok(Response::new(SettleAggregateGroupResponse::default()))
    }

    async fn renew_aggregate_group(
        &self,
        _request: Request<RenewAggregateGroupRequest>,
    ) -> Result<Response<RenewAggregateGroupResponse>, Status> {
        Ok(Response::new(RenewAggregateGroupResponse::default()))
    }
}

async fn start_stub() -> (String, Arc<Mutex<StubState>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定 stub 端口");
    let address = format!("http://{}", listener.local_addr().expect("stub 地址"));
    let state = Arc::new(Mutex::new(StubState::default()));
    let server = StubServer {
        state: state.clone(),
    };
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(AggregateStoreServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    (address, state)
}

fn aggregate_type() -> Option<AggregateTypeRef> {
    Some(AggregateTypeRef {
        business_space: "orders".into(),
        aggregate_type: "order".into(),
    })
}

#[tokio::test]
async fn all_unary_methods_roundtrip_and_rotate_after_connect_failure() {
    assert!(AggregateStoreClient::connect(Vec::new()).await.is_err());
    assert!(
        AggregateStoreClient::connect(vec!["not a valid endpoint".into()])
            .await
            .is_err()
    );

    let (address, _) = start_stub().await;
    let mut client = AggregateStoreClient::connect(vec![address, "http://127.0.0.1:1".into()])
        .await
        .expect("连接 stub");
    assert_eq!(
        client.capabilities().await.expect("能力").api_version,
        "1.0"
    );
    assert_eq!(
        client
            .list_aggregate_types()
            .await
            .expect("连接失败后轮换节点")
            .len(),
        1
    );
    client
        .register_aggregate_type(RegisterAggregateTypeRequest::default())
        .await
        .expect("注册聚合类型");
    client
        .get_aggregate_type(aggregate_type().unwrap())
        .await
        .expect("获取聚合类型");
    assert_eq!(
        client
            .list_states(ListAggregateStatesRequest::default())
            .await
            .expect("列状态")
            .states
            .len(),
        1
    );
    assert_eq!(
        client
            .get_state(GetAggregateStateRequest::default())
            .await
            .expect("读状态")
            .revision,
        3
    );
    assert_eq!(
        client
            .put_state(PutAggregateStateRequest::default())
            .await
            .expect("写状态")
            .revision,
        4
    );
    assert_eq!(
        client
            .status()
            .await
            .expect("服务状态")
            .aggregate_type_count,
        1
    );
    client
        .create_group(CreateAggregateGroupRequest::default())
        .await
        .expect("创建组");
    client
        .update_group(UpdateAggregateGroupRequest::default())
        .await
        .expect("更新组");
    client
        .get_group(GetAggregateGroupRequest::default())
        .await
        .expect("获取组");
    assert_eq!(
        client
            .list_groups(aggregate_type().unwrap())
            .await
            .expect("列组")
            .len(),
        1
    );
    client
        .fetch_group(FetchAggregateGroupRequest::default())
        .await
        .expect("Fetch 组");
    client
        .settle_group(SettleAggregateGroupRequest::default())
        .await
        .expect("结算组");
    client
        .renew_group(RenewAggregateGroupRequest::default())
        .await
        .expect("续租组");
    assert_eq!(
        client
            .list_partitions(aggregate_type().unwrap())
            .await
            .expect("列分区")
            .len(),
        1
    );
    client
        .delete_group(DeleteAggregateGroupRequest::default())
        .await
        .expect("删除组");
}

#[tokio::test]
async fn unary_retry_without_hint_handles_retryable_and_permanent_statuses() {
    let (address, state) = start_stub().await;
    state.lock().expect("stub 锁").append_queue.extend([
        Err(Status::unavailable("leader election")),
        Err(Status::deadline_exceeded("slow election")),
        Ok(AppendAggregateEventResponse {
            aggregate_version: 9,
        }),
        Err(Status::invalid_argument("bad request")),
    ]);
    let mut client = AggregateStoreClient::connect(vec![address])
        .await
        .expect("连接 stub");
    let request = AppendAggregateEventRequest::default();
    assert_eq!(
        client
            .append(request.clone())
            .await
            .expect("可重试错误后成功")
            .aggregate_version,
        9
    );
    let error = client
        .append(request)
        .await
        .expect_err("永久错误必须直接返回");
    assert!(matches!(
        error,
        es_client::ClientError::RpcFailed {
            code: tonic::Code::InvalidArgument,
            ..
        }
    ));
}

#[tokio::test]
async fn append_follows_leader_hint_to_uncached_node() {
    let (leader, leader_state) = start_stub().await;
    let (follower, follower_state) = start_stub().await;
    follower_state
        .lock()
        .expect("stub 锁")
        .append_queue
        .push_back(Err(Status::unavailable(format!(
            "not leader; leader_id=2 leader_addr={leader}"
        ))));

    let mut client = AggregateStoreClient::connect(vec![follower])
        .await
        .expect("连接 follower");
    let response = client
        .append(AppendAggregateEventRequest {
            aggregate_type: aggregate_type(),
            aggregate_id: "order-1".into(),
            expected_version: None,
            event: Some(NewAggregateEvent {
                event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                event_type: "created".into(),
                data: b"{}".to_vec(),
                metadata: Vec::new(),
            }),
        })
        .await
        .expect("应重定向到 leader");
    assert_eq!(response.aggregate_version, 7);
    assert_eq!(follower_state.lock().expect("stub 锁").append_calls, 1);
    assert_eq!(leader_state.lock().expect("stub 锁").append_calls, 1);
}

#[tokio::test]
async fn ambiguous_put_state_and_fetch_do_not_replay_transient_failures() {
    let (address, state) = start_stub().await;
    {
        let mut state = state.lock().expect("stub 锁");
        state
            .put_state_queue
            .push_back(Err(Status::unavailable("response lost")));
        state
            .fetch_queue
            .push_back(Err(Status::deadline_exceeded("response lost")));
    }
    let mut client = AggregateStoreClient::connect(vec![address])
        .await
        .expect("连接 stub");
    assert!(
        client
            .put_state(PutAggregateStateRequest::default())
            .await
            .is_err()
    );
    assert!(
        client
            .fetch_group(FetchAggregateGroupRequest::default())
            .await
            .is_err()
    );
    let state = state.lock().expect("stub 锁");
    assert_eq!(state.put_state_calls, 1);
    assert_eq!(state.fetch_calls, 1);
}

#[tokio::test]
async fn ambiguous_put_state_still_follows_explicit_leader_hint() {
    let (leader, leader_state) = start_stub().await;
    let (follower, follower_state) = start_stub().await;
    follower_state
        .lock()
        .expect("stub 锁")
        .put_state_queue
        .push_back(Err(Status::unavailable(format!(
            "not leader; leader_id=2 leader_addr={leader}"
        ))));
    let mut client = AggregateStoreClient::connect(vec![follower])
        .await
        .expect("连接 follower");
    assert_eq!(
        client
            .put_state(PutAggregateStateRequest::default())
            .await
            .expect("明确 leader hint 应允许重定向")
            .revision,
        4
    );
    assert_eq!(follower_state.lock().expect("stub 锁").put_state_calls, 1);
    assert_eq!(leader_state.lock().expect("stub 锁").put_state_calls, 1);
}

#[tokio::test]
async fn follow_reconnects_from_last_opaque_cursor() {
    let (address, state) = start_stub().await;
    state.lock().expect("stub 锁").read_streams.extend([
        vec![
            Ok(FollowAggregateTypeEventsResponse {
                payload: Some(follow_aggregate_type_events_response::Payload::Event(
                    AggregateEvent {
                        aggregate_id: "order-1".into(),
                        aggregate_version: 0,
                        ..Default::default()
                    },
                )),
                cursor: vec![1, 2, 3],
            }),
            Err(Status::unavailable("connection dropped")),
        ],
        vec![Ok(FollowAggregateTypeEventsResponse {
            payload: Some(follow_aggregate_type_events_response::Payload::CaughtUp(
                Empty {},
            )),
            cursor: vec![4, 5, 6],
        })],
    ]);

    let mut client = AggregateStoreClient::connect(vec![address])
        .await
        .expect("连接 stub");
    let mut stream = client
        .follow(FollowAggregateTypeEventsRequest {
            aggregate_type: aggregate_type(),
            start: Some(AggregateFollowStart {
                kind: Some(aggregate_follow_start::Kind::Beginning(Empty {})),
            }),
        })
        .await
        .expect("建立 follow");

    let first = stream.next().await.expect("第一帧").expect("第一帧成功");
    assert!(matches!(
        first.payload,
        Some(follow_aggregate_type_events_response::Payload::Event(_))
    ));
    let resumed = tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
        .await
        .expect("等待续读超时")
        .expect("续读帧")
        .expect("续读成功");
    assert!(matches!(
        resumed.payload,
        Some(follow_aggregate_type_events_response::Payload::CaughtUp(_))
    ));

    let requests = &state.lock().expect("stub 锁").read_requests;
    assert_eq!(requests.len(), 2);
    assert!(matches!(
        requests[1].start.as_ref().and_then(|start| start.kind.as_ref()),
        Some(aggregate_follow_start::Kind::Cursor(cursor)) if cursor == &vec![1, 2, 3]
    ));
}
