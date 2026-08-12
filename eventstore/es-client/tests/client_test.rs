//! 客户端连接管理测试：空节点列表、非法地址、stub server 上的连接复用、
//! append leader 重定向、读方法节点轮换、read_all/subscribe/get_stream_meta。

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use es_proto::eventstore::event_store_server::EventStore;
use es_proto::eventstore::*;

/// 可编程 stub 状态：测试预设行为 + 调用计数（Arc 共享，多 stub 独立实例）。
#[derive(Default)]
struct StubState {
    append_calls: usize,
    /// append 预设响应队列（空 = 默认成功）
    append_queue: VecDeque<Result<Response<AppendResponse>, Status>>,
    read_stream_calls: usize,
    /// 建立 read_stream 流时返回的错误（None = 正常建立）
    read_stream_error: Option<Status>,
    /// read_stream 预设流内容（Err = 流中途错误；空 = 空流）
    read_stream_items: Vec<Result<ReadEventsResponse, Status>>,
    read_all_calls: usize,
    /// 建立 read_all 流时返回的错误（None = 正常建立）
    read_all_error: Option<Status>,
    /// read_all 预设流内容队列（空 = 空流）
    read_all_queue: VecDeque<Vec<ReadEventsResponse>>,
    /// read_all 收到的全部请求（断言翻页透传）
    read_all_requests: Vec<ReadAllRequest>,
    subscribe_calls: usize,
    /// subscribe 预设流内容（None = 空流）
    subscribe_stream: Option<Vec<Result<SubscribeResponse, Status>>>,
    get_stream_meta_calls: usize,
    /// get_stream_meta 预设响应（None = 默认不存在）
    get_stream_meta_response: Option<GetStreamMetaResponse>,
}

type SharedState = Arc<Mutex<StubState>>;

/// 可编程 stub：每个方法先记录调用，再按预设行为响应。
#[derive(Clone)]
struct StubServer {
    state: SharedState,
}

/// 构造默认 AppendResponse（与 es-server 一致的最小值）
fn ok_append() -> Result<Response<AppendResponse>, Status> {
    Ok(Response::new(AppendResponse {
        next_expected_version: 0,
        first_position: 0,
        last_position: 0,
        shard_id: 0,
    }))
}

/// 默认 GetStreamMetaResponse（流不存在）
fn default_meta() -> GetStreamMetaResponse {
    GetStreamMetaResponse {
        exists: false,
        current_version: 0,
        shard_id: 0,
    }
}

#[tonic::async_trait]
impl EventStore for StubServer {
    async fn append(
        &self,
        _request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.append_calls += 1;
        if let Some(resp) = state.append_queue.pop_front() {
            return resp;
        }
        ok_append()
    }

    type ReadStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReadEventsResponse, Status>> + Send>>;
    async fn read_stream(
        &self,
        _request: Request<ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.read_stream_calls += 1;
        if let Some(status) = &state.read_stream_error {
            return Err(status.clone());
        }
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        for item in &state.read_stream_items {
            let _ = tx.try_send(item.clone());
        }
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type ReadAllStream =
        Pin<Box<dyn Stream<Item = Result<ReadEventsResponse, Status>> + Send>>;
    async fn read_all(
        &self,
        request: Request<ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.read_all_calls += 1;
        if let Some(status) = &state.read_all_error {
            return Err(status.clone());
        }
        state.read_all_requests.push(request.into_inner());
        let pages = state.read_all_queue.pop_front().unwrap_or_default();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        for page in pages {
            let _ = tx.try_send(Ok(page));
        }
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeResponse, Status>> + Send>>;
    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.subscribe_calls += 1;
        let items = state.subscribe_stream.clone().unwrap_or_default();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        for item in items {
            let _ = tx.try_send(item);
        }
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_stream_meta(
        &self,
        _request: Request<GetStreamMetaRequest>,
    ) -> Result<Response<GetStreamMetaResponse>, Status> {
        let mut state = self.state.lock().expect("stub 锁");
        state.get_stream_meta_calls += 1;
        let resp = state
            .get_stream_meta_response
            .clone()
            .unwrap_or_else(default_meta);
        Ok(Response::new(resp))
    }

    async fn create_stream(
        &self,
        _request: Request<CreateStreamRequest>,
    ) -> Result<Response<CreateStreamResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

/// 起一个 stub gRPC 服务，返回 (地址, 共享状态)。
async fn start_stub_server() -> (String, SharedState) {
    let state = Arc::new(Mutex::new(StubState::default()));
    let addr = start_stub_server_with(state.clone()).await;
    (addr, state)
}

/// 用给定状态起 stub gRPC 服务，返回其地址。
async fn start_stub_server_with(state: SharedState) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = listener.local_addr().expect("取地址");
    let server = StubServer { state };
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(event_store_server::EventStoreServer::new(server))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    // 等服务器开始监听
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("http://{addr}")
}

/// 构造样例事件（stub 流内容用）
fn sample_event(version: u64) -> Event {
    Event {
        stream_id: "s1".to_string(),
        version,
        event_id: format!("id-{version}").into(),
        event_type: "T".to_string(),
        data: vec![],
        metadata: vec![],
        hlc: Some(Hlc { wall: 1, logical: 2 }),
        position: version,
        shard_id: 0,
    }
}

#[tokio::test]
async fn connect_empty_nodes_errors() {
    let result = es_client::EventStoreClient::connect(vec![]).await;
    assert!(
        matches!(result, Err(es_client::ClientError::InvalidConfig(_))),
        "空列表应报 InvalidConfig: {result:?}"
    );
}

#[tokio::test]
async fn connect_invalid_addr_errors() {
    // 缺 host 的 URI 无法被 tonic Endpoint 解析，应报 InvalidConfig
    let result = es_client::EventStoreClient::connect(vec!["http://".to_string()]).await;
    assert!(
        matches!(result, Err(es_client::ClientError::InvalidConfig(_))),
        "非法地址应报 InvalidConfig: {result:?}"
    );
}

#[tokio::test]
async fn connect_append_via_stub_reuses_conn() {
    let (addr, state) = start_stub_server().await;

    let mut client = es_client::EventStoreClient::connect(vec![addr.clone()])
        .await
        .expect("连接 stub");

    // 第一次 append：get_or_connect miss → connect_one 建连并缓存
    let resp = client
        .append(
            "s1".to_string(),
            es_client::ExpectedVersionBuilder::any(),
            vec![es_client::EventBuilder::new("T").build()],
        )
        .await
        .expect("append 成功");
    assert_eq!(resp.next_expected_version, 0);
    assert_eq!(state.lock().expect("stub 锁").append_calls, 1);

    // 第二次 append：get_or_connect 命中缓存分支
    let resp = client
        .append(
            "s1".to_string(),
            es_client::ExpectedVersionBuilder::any(),
            vec![],
        )
        .await
        .expect("append 复用连接成功");
    assert_eq!(resp.next_expected_version, 0);
    assert_eq!(state.lock().expect("stub 锁").append_calls, 2);
}

#[tokio::test]
async fn read_stream_via_stub_empty() {
    let (addr, _state) = start_stub_server().await;
    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let events = client
        .read_stream("s1".to_string(), 0, 10, es_client::Direction::Forward)
        .await
        .expect("read_stream 成功");
    assert!(events.is_empty());
}

#[tokio::test]
async fn append_redirects_to_leader_addr() {
    // 双 stub：节点 A 返回 Unavailable + leader_addr(节点 B)，节点 B 成功。
    // 重定向地址不在初始节点列表，验证完整重定向路径。
    let (addr_b, state_b) = start_stub_server().await;
    let (addr_a, state_a) = start_stub_server().await;
    state_a
        .lock()
        .expect("stub 锁")
        .append_queue
        .push_back(Err(Status::unavailable(format!(
            "not leader; leader_id=2 leader_addr={addr_b}"
        ))));

    let mut client = es_client::EventStoreClient::connect(vec![addr_a])
        .await
        .expect("连接 stub");
    let resp = client
        .append(
            "s1".to_string(),
            es_client::ExpectedVersionBuilder::any(),
            vec![es_client::EventBuilder::new("T").build()],
        )
        .await
        .expect("重定向后 append 成功");

    assert_eq!(resp.next_expected_version, 0);
    assert_eq!(
        state_a.lock().expect("stub 锁").append_calls,
        1,
        "节点 A 只被调用一次（重定向后不再回打）"
    );
    assert_eq!(
        state_b.lock().expect("stub 锁").append_calls,
        1,
        "重定向目标节点 B 被调用"
    );
}

#[tokio::test]
async fn append_retries_election_unknown() {
    // 选举中（无 leader 提示）：退避后重试同一节点，最终成功
    let (addr, state) = start_stub_server().await;
    state.lock().expect("stub 锁").append_queue.push_back(Err(
        Status::unavailable("not leader; leader unknown, retry later"),
    ));

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let resp = client
        .append(
            "s1".to_string(),
            es_client::ExpectedVersionBuilder::any(),
            vec![],
        )
        .await
        .expect("选举结束后 append 成功");

    assert_eq!(resp.next_expected_version, 0);
    assert_eq!(
        state.lock().expect("stub 锁").append_calls,
        2,
        "第一次退避重试后第二次成功"
    );
}

#[tokio::test]
async fn append_budget_exhausted_returns_not_leader() {
    // 单节点预算 = 1×2+2 = 4：一直处于选举中（无提示）→ 预算耗尽报 NotLeader
    let (addr, state) = start_stub_server().await;
    for _ in 0..4 {
        state.lock().expect("stub 锁").append_queue.push_back(Err(
            Status::unavailable("not leader; leader unknown, retry later"),
        ));
    }

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let err = client
        .append(
            "s1".to_string(),
            es_client::ExpectedVersionBuilder::any(),
            vec![],
        )
        .await
        .expect_err("预算耗尽应报错");

    assert!(
        matches!(err, es_client::ClientError::NotLeader(None)),
        "无重定向地址时报 NotLeader(None): {err:?}"
    );
    assert_eq!(state.lock().expect("stub 锁").append_calls, 4);
}

#[tokio::test]
async fn append_failed_precondition_raised() {
    // 乐观冲突等不可重试错误原样上抛，不重试
    let (addr, state) = start_stub_server().await;
    state
        .lock()
        .expect("stub 锁")
        .append_queue
        .push_back(Err(Status::failed_precondition("版本冲突")));

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let err = client
        .append(
            "s1".to_string(),
            es_client::ExpectedVersionBuilder::exact(5),
            vec![],
        )
        .await
        .expect_err("FailedPrecondition 应上抛");

    assert!(
        matches!(
            err,
            es_client::ClientError::RpcFailed {
                code: tonic::Code::FailedPrecondition,
                ref message,
            } if message.contains("版本冲突")
        ),
        "FailedPrecondition 上抛且保留码与消息: {err:?}"
    );
    assert_eq!(state.lock().expect("stub 锁").append_calls, 1);
}

#[tokio::test]
async fn read_stream_failover_to_healthy_node() {
    // 节点 A 建立流失败 → 轮换到节点 B 成功
    let (addr_b, state_b) = start_stub_server().await;
    let (addr_a, state_a) = start_stub_server().await;
    state_a
        .lock()
        .expect("stub 锁")
        .read_stream_error = Some(Status::internal("节点 A 存储故障"));

    let mut client = es_client::EventStoreClient::connect(vec![addr_a, addr_b])
        .await
        .expect("连接 stub");
    let events = client
        .read_stream("s1".to_string(), 0, 10, es_client::Direction::Forward)
        .await
        .expect("轮换后 read_stream 成功");
    assert!(events.is_empty());

    assert_eq!(state_a.lock().expect("stub 锁").read_stream_calls, 1);
    assert_eq!(
        state_b.lock().expect("stub 锁").read_stream_calls,
        1,
        "轮换到节点 B"
    );
}

#[tokio::test]
async fn read_all_merges_pages_and_exposes_next_positions() {
    // 单节点两页：事件合并、next_positions 取最后一页非空值
    let (addr, state) = start_stub_server().await;
    state.lock().expect("stub 锁").read_all_queue.push_back(vec![
        ReadEventsResponse {
            events: vec![sample_event(1)],
            next_positions: vec![ShardPosition {
                shard_id: 0,
                from_position: 5,
                ended: false,
            }],
        },
        ReadEventsResponse {
            events: vec![sample_event(2)],
            next_positions: vec![ShardPosition {
                shard_id: 0,
                from_position: 10,
                ended: false,
            }],
        },
    ]);

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let (events, next_positions) = client
        .read_all(vec![0], 0, 10, es_client::Direction::Forward, vec![])
        .await
        .expect("read_all 成功");

    assert_eq!(events.len(), 2, "两页事件合并");
    assert_eq!(
        next_positions,
        vec![ShardPosition {
            shard_id: 0,
            from_position: 10,
            ended: false,
        }],
        "next_positions 取最后一页"
    );
}

#[tokio::test]
async fn read_all_passes_through_from_positions() {
    // 翻页：调用方把上页 next_positions 原样透传，stub 收到同样的 from_positions
    let (addr, state) = start_stub_server().await;

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let page_cursor = vec![ShardPosition {
        shard_id: 1,
        from_position: 42,
        ended: false,
    }];
    let (_events, _next) = client
        .read_all(vec![], 0, 10, es_client::Direction::Forward, page_cursor.clone())
        .await
        .expect("read_all 成功");

    let state = state.lock().expect("stub 锁");
    assert_eq!(state.read_all_requests.len(), 1);
    assert_eq!(
        state.read_all_requests[0].from_positions, page_cursor,
        "from_positions 原样透传"
    );
    assert!(state.read_all_requests[0].shard_ids.is_empty());
}

#[tokio::test]
async fn read_all_all_nodes_failed() {
    // read_all 全部节点失败（建立流阶段）→ 汇总 AllNodesFailed
    let (addr_b, state_b) = start_stub_server().await;
    let (addr_a, state_a) = start_stub_server().await;
    state_a.lock().expect("stub 锁").read_all_error = Some(Status::internal("a 故障"));
    state_b.lock().expect("stub 锁").read_all_error = Some(Status::internal("b 故障"));

    let mut client = es_client::EventStoreClient::connect(vec![addr_a, addr_b])
        .await
        .expect("连接 stub");
    let err = client
        .read_all(vec![0], 0, 10, es_client::Direction::Forward, vec![])
        .await
        .expect_err("全部节点失败应报错");

    assert!(
        matches!(err, es_client::ClientError::AllNodesFailed(ref msg) if msg.contains("a 故障") && msg.contains("b 故障")),
        "错误汇总两个节点: {err:?}"
    );
    assert_eq!(state_a.lock().expect("stub 锁").read_all_calls, 1);
    assert_eq!(state_b.lock().expect("stub 锁").read_all_calls, 1);
}

#[tokio::test]
async fn read_all_permanent_error_not_rotated() {
    // 永久错误（InvalidArgument 等）不轮换节点：换节点结果相同，直接上抛
    let (addr_b, state_b) = start_stub_server().await;
    let (addr_a, state_a) = start_stub_server().await;
    state_a
        .lock()
        .expect("stub 锁")
        .read_all_error = Some(Status::invalid_argument("shard_ids 与 from_positions 不能同时为空"));

    let mut client = es_client::EventStoreClient::connect(vec![addr_a, addr_b])
        .await
        .expect("连接 stub");
    let err = client
        .read_all(vec![], 0, 10, es_client::Direction::Forward, vec![])
        .await
        .expect_err("InvalidArgument 应上抛");

    assert!(
        matches!(
            err,
            es_client::ClientError::RpcFailed {
                code: tonic::Code::InvalidArgument,
                ..
            }
        ),
        "永久错误应保留码并上抛，不包装成 AllNodesFailed: {err:?}"
    );
    assert_eq!(
        state_a.lock().expect("stub 锁").read_all_calls,
        1,
        "节点 A 只被调用一次（不轮换）"
    );
    assert_eq!(
        state_b.lock().expect("stub 锁").read_all_calls,
        0,
        "节点 B 不被尝试"
    );
}

#[tokio::test]
async fn read_stream_mid_stream_error_not_rerouted() {
    // 流建立成功后的中途错误：不轮换节点整页重读，直接上抛
    let (addr_b, state_b) = start_stub_server().await;
    let (addr_a, state_a) = start_stub_server().await;
    state_a.lock().expect("stub 锁").read_stream_items = vec![
        Ok(ReadEventsResponse {
            events: vec![sample_event(1)],
            next_positions: vec![],
        }),
        Err(Status::internal("流中途断开")),
    ];

    let mut client = es_client::EventStoreClient::connect(vec![addr_a, addr_b])
        .await
        .expect("连接 stub");
    let err = client
        .read_stream("s1".to_string(), 0, 10, es_client::Direction::Forward)
        .await
        .expect_err("中途错误应上抛");

    assert!(
        matches!(
            err,
            es_client::ClientError::RpcFailed {
                code: tonic::Code::Internal,
                ref message,
            } if message.contains("流中途断开")
        ),
        "中途错误保留码上抛: {err:?}"
    );
    assert_eq!(
        state_a.lock().expect("stub 锁").read_stream_calls,
        1,
        "节点 A 只调用一次（中途错误不轮换重读）"
    );
    assert_eq!(
        state_b.lock().expect("stub 锁").read_stream_calls,
        0,
        "节点 B 不被尝试"
    );
}

#[tokio::test]
async fn subscribe_delivers_events_and_caught_up() {
    // 订阅流：catch-up 事件后发 caught_up 分界信号
    let (addr, state) = start_stub_server().await;
    state.lock().expect("stub 锁").subscribe_stream = Some(vec![
        Ok(SubscribeResponse {
            payload: Some(subscribe_response::Payload::Event(sample_event(1))),
        }),
        Ok(SubscribeResponse {
            payload: Some(subscribe_response::Payload::CaughtUp(Empty {})),
        }),
    ]);

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let mut stream = client
        .subscribe(es_client::SubscribeTarget::Stream("s1".to_string()), 0, false)
        .await
        .expect("订阅成功");

    let first = stream.next().await.expect("第一个响应").expect("无错误");
    assert!(
        matches!(first.payload, Some(subscribe_response::Payload::Event(ref e)) if e.version == 1),
        "先收到 catch-up 事件: {first:?}"
    );
    let second = stream.next().await.expect("第二个响应").expect("无错误");
    assert!(
        matches!(second.payload, Some(subscribe_response::Payload::CaughtUp(_))),
        "再收到 caught_up 分界信号"
    );
    assert!(stream.next().await.is_none(), "流结束");
}

#[tokio::test]
async fn subscribe_stream_error_raised_without_resubscribe() {
    // 流内错误上抛，且订阅只发起一次（不自动重订阅）
    let (addr, state) = start_stub_server().await;
    state.lock().expect("stub 锁").subscribe_stream = Some(vec![Err(Status::internal(
        "订阅者落后，服务端关流",
    ))]);

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let mut stream = client
        .subscribe(es_client::SubscribeTarget::All { shard_id: 0 }, 0, false)
        .await
        .expect("订阅建立成功");

    let err = stream.next().await.expect("流内错误").expect_err("错误应上抛");
    assert!(
        matches!(
            err,
            es_client::ClientError::RpcFailed {
                code: tonic::Code::Internal,
                ref message,
            } if message.contains("订阅者落后")
        ),
        "流内错误映射 RpcFailed 且保留码: {err:?}"
    );
    assert_eq!(
        state.lock().expect("stub 锁").subscribe_calls,
        1,
        "不自动重订阅"
    );
}

#[tokio::test]
async fn append_cluster_unreachable_distinguished() {
    // 节点 A 一直「leader unknown」（可达但选举中），节点 B 不可达（懒连接失败）：
    // 预算耗尽时带建连失败详情而非 NotLeader(None)——区分「集群不可达」与「选举中」
    let (addr_a, state_a) = start_stub_server().await;
    for _ in 0..3 {
        state_a.lock().expect("stub 锁").append_queue.push_back(Err(
            Status::unavailable("not leader; leader unknown, retry later"),
        ));
    }

    let mut client = es_client::EventStoreClient::connect(vec![
        addr_a,
        "http://127.0.0.1:1".to_string(), // 无服务监听，懒连接必拒
    ])
    .await
    .expect("连接（预热 A 成功，B 懒连接）");
    let err = client
        .append(
            "s1".to_string(),
            es_client::ExpectedVersionBuilder::any(),
            vec![],
        )
        .await
        .expect_err("集群不可达应报错");

    assert!(
        matches!(
            err,
            es_client::ClientError::RpcFailed { ref message, .. } if message.contains("连接失败")
        ),
        "建连失败应带错误详情而非 NotLeader: {err:?}"
    );
}

#[tokio::test]
async fn get_stream_meta_via_stub() {
    let (addr, state) = start_stub_server().await;
    state.lock().expect("stub 锁").get_stream_meta_response = Some(GetStreamMetaResponse {
        exists: true,
        current_version: 7,
        shard_id: 1,
    });

    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let meta = client
        .get_stream_meta("s1".to_string())
        .await
        .expect("get_stream_meta 成功");

    assert!(meta.exists);
    assert_eq!(meta.current_version, 7);
    assert_eq!(meta.shard_id, 1);
    assert_eq!(state.lock().expect("stub 锁").get_stream_meta_calls, 1);
}
