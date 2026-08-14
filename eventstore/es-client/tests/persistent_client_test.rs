//! 持久化订阅 SDK 的 leader 重定向、退避与失败汇总测试。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use es_client::ClientError;
use es_proto::eventstore::persistent_subscriptions_server::{
    PersistentSubscriptions, PersistentSubscriptionsServer,
};
use es_proto::eventstore::*;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct StubState {
    get_calls: usize,
    get_queue: VecDeque<Result<PersistentSubscriptionInfo, Status>>,
}

#[derive(Clone)]
struct StubServer {
    state: Arc<Mutex<StubState>>,
}

#[tonic::async_trait]
impl PersistentSubscriptions for StubServer {
    async fn create_persistent_subscription(
        &self,
        _request: Request<CreatePersistentSubscriptionRequest>,
    ) -> Result<Response<PersistentSubscriptionInfo>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn update_persistent_subscription(
        &self,
        _request: Request<UpdatePersistentSubscriptionRequest>,
    ) -> Result<Response<PersistentSubscriptionInfo>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn delete_persistent_subscription(
        &self,
        _request: Request<DeletePersistentSubscriptionRequest>,
    ) -> Result<Response<Empty>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn get_persistent_subscription(
        &self,
        request: Request<GetPersistentSubscriptionRequest>,
    ) -> Result<Response<PersistentSubscriptionInfo>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.get_calls += 1;
        match state.get_queue.pop_front() {
            Some(Ok(info)) => Ok(Response::new(info)),
            Some(Err(status)) => Err(status),
            None => Ok(Response::new(PersistentSubscriptionInfo {
                name: request.into_inner().name,
                ..Default::default()
            })),
        }
    }

    async fn list_persistent_subscriptions(
        &self,
        _request: Request<ListPersistentSubscriptionsRequest>,
    ) -> Result<Response<ListPersistentSubscriptionsResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn fetch_persistent_subscription(
        &self,
        _request: Request<FetchPersistentSubscriptionRequest>,
    ) -> Result<Response<FetchPersistentSubscriptionResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn settle_persistent_subscription(
        &self,
        _request: Request<SettlePersistentSubscriptionRequest>,
    ) -> Result<Response<SettlePersistentSubscriptionResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn list_parked_persistent_subscription(
        &self,
        _request: Request<ListParkedPersistentSubscriptionRequest>,
    ) -> Result<Response<ListParkedPersistentSubscriptionResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn replay_parked_persistent_subscription(
        &self,
        _request: Request<ReplayParkedPersistentSubscriptionRequest>,
    ) -> Result<Response<ReplayParkedPersistentSubscriptionResponse>, Status> {
        Err(Status::unimplemented("stub"))
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
            .add_service(PersistentSubscriptionsServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    (address, state)
}

#[tokio::test]
async fn follows_leader_hint_to_uncached_node() {
    let (leader, leader_state) = start_stub().await;
    let (follower, follower_state) = start_stub().await;
    follower_state
        .lock()
        .expect("stub 锁")
        .get_queue
        .push_back(Err(Status::unavailable(format!(
            "not leader; leader_id=2 leader_addr={leader}"
        ))));

    let mut client = es_client::PersistentSubscriptionsClient::connect(vec![follower])
        .await
        .expect("连接 follower");
    let info = client.get("workers").await.expect("重定向到 leader");
    assert_eq!(info.name, "workers");
    assert_eq!(follower_state.lock().expect("stub 锁").get_calls, 1);
    assert_eq!(leader_state.lock().expect("stub 锁").get_calls, 1);
}

#[tokio::test]
async fn retries_same_node_after_election_hint_without_address() {
    let (address, state) = start_stub().await;
    state
        .lock()
        .expect("stub 锁")
        .get_queue
        .push_back(Err(Status::unavailable(
            "not leader; leader unknown, retry later",
        )));

    let mut client = es_client::PersistentSubscriptionsClient::connect(vec![address])
        .await
        .expect("连接 stub");
    assert_eq!(client.get("workers").await.unwrap().name, "workers");
    assert_eq!(state.lock().expect("stub 锁").get_calls, 2);
}

#[tokio::test]
async fn exhausted_election_budget_reports_not_leader_without_address() {
    let (address, state) = start_stub().await;
    for _ in 0..4 {
        state
            .lock()
            .expect("stub 锁")
            .get_queue
            .push_back(Err(Status::unavailable(
                "not leader; leader unknown, retry later",
            )));
    }

    let mut client = es_client::PersistentSubscriptionsClient::connect(vec![address])
        .await
        .expect("连接 stub");
    assert!(matches!(
        client.get("workers").await,
        Err(ClientError::NotLeader(None))
    ));
    assert_eq!(state.lock().expect("stub 锁").get_calls, 4);
}

#[tokio::test]
async fn permanent_rpc_error_is_returned_without_retry() {
    let (address, state) = start_stub().await;
    state
        .lock()
        .expect("stub 锁")
        .get_queue
        .push_back(Err(Status::invalid_argument("invalid group")));

    let mut client = es_client::PersistentSubscriptionsClient::connect(vec![address])
        .await
        .expect("连接 stub");
    assert!(matches!(
        client.get("workers").await,
        Err(ClientError::RpcFailed {
            code: tonic::Code::InvalidArgument,
            ref message,
        }) if message == "invalid group"
    ));
    assert_eq!(state.lock().expect("stub 锁").get_calls, 1);
}

#[tokio::test]
async fn unreachable_candidate_is_collected_in_all_nodes_failed() {
    let (address, state) = start_stub().await;
    for _ in 0..6 {
        state
            .lock()
            .expect("stub 锁")
            .get_queue
            .push_back(Err(Status::unavailable(
                "not leader; leader unknown, retry later",
            )));
    }

    let mut client = es_client::PersistentSubscriptionsClient::connect(vec![
        address,
        "http://127.0.0.1:9".into(),
    ])
    .await
    .expect("连接首个 stub");
    let error = client.get("workers").await.expect_err("全部节点应失败");
    assert!(matches!(
        error,
        ClientError::AllNodesFailed(ref details) if details.contains("127.0.0.1:9")
    ));
}
