//! esctl 连接层故障转移与重试集成测试：用 mock tonic 服务 + esctl 子进程，
//! 覆盖代码审查发现的 3/5/12（建连失败不再上抛、leader-unknown 退避重试
//! 不再被 tried 集合挡死、with_admin_leader 重试覆盖 leader 探测失败）与
//! 发现 8（watch --once 未追平必须非零退出）。

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use es_proto::eventstore::event_store_server::{EventStore, EventStoreServer};
use es_proto::eventstore::raft_admin_server::{RaftAdmin, RaftAdminServer};
use es_proto::eventstore::*;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

/// 启动 mock EventStore 服务器，返回监听地址
async fn serve_event_store(svc: MockEventStore) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().expect("取地址"));
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

/// 启动 mock RaftAdmin 服务器，返回监听地址
async fn serve_admin(svc: MockAdmin) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().expect("取地址"));
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(RaftAdminServer::new(svc))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

/// 未监听端口：绑定后立即释放（端口几乎不可能被复用，作为"宕机端点"）
async fn dead_addr() -> String {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定");
    format!("http://{}", l.local_addr().expect("取地址"))
}

/// 以 esctl 子进程运行，返回 (退出码, stdout, stderr)
fn esctl(endpoints: &str, args: &[&str]) -> (Option<i32>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(["--endpoints", endpoints]);
    cmd.args(args);
    let out = cmd.output().expect("运行 esctl");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// mock 数据面：append 前 N 次返回「leader unknown」，之后成功；订阅返回立即关闭的空流
#[derive(Clone, Default)]
struct MockEventStore {
    /// append 返回 leader-unknown 的剩余次数（fetch_sub 语义：旧值 > 0 即 unknown）
    unknown_appends: Arc<AtomicU32>,
}

#[tonic::async_trait]
impl EventStore for MockEventStore {
    type ReadStreamStream = ReceiverStream<Result<ReadEventsResponse, Status>>;
    type ReadAllStream = ReceiverStream<Result<ReadEventsResponse, Status>>;
    type SubscribeStream = ReceiverStream<Result<SubscribeResponse, Status>>;

    async fn append(
        &self,
        _r: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        // 剩余 unknown 次数 > 0 时消耗一次并返回 unknown；耗尽后（计数保持 0）成功
        let was_unknown = self
            .unknown_appends
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if was_unknown {
            // 服务端选举中的真实提示（es-server service.rs client_write_to_status）
            return Err(Status::unavailable("not leader; leader unknown, retry later"));
        }
        Ok(Response::new(AppendResponse {
            next_expected_version: 1,
            first_position: 1,
            last_position: 1,
            shard_id: 0,
        }))
    }

    async fn read_stream(
        &self,
        _r: Request<ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ReadEventsResponse {
                    events: vec![],
                    next_positions: vec![],
                }))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn read_all(
        &self,
        _r: Request<ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ReadEventsResponse {
                    events: vec![],
                    next_positions: vec![],
                }))
                .await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn subscribe(
        &self,
        _r: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // 立即关闭的空流：模拟服务端在 catch-up 阶段关闭订阅
        // （真实场景：订阅者落后被 Lagged 踢出，es-server service.rs:553）
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        drop(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_stream_meta(
        &self,
        _r: Request<GetStreamMetaRequest>,
    ) -> Result<Response<GetStreamMetaResponse>, Status> {
        Err(Status::unimplemented("mock 未实现"))
    }
}

/// mock 管理面：get_raft_state 前 N 次返回无 leader（选举中），之后是 leader；
/// 仅分片 0 存在（其余 NotFound，供分片探测停止）
#[derive(Clone)]
struct MockAdmin {
    no_leader_rounds: Arc<AtomicU32>,
}

#[tonic::async_trait]
impl RaftAdmin for MockAdmin {
    async fn get_raft_state(
        &self,
        r: Request<GetRaftStateRequest>,
    ) -> Result<Response<GetRaftStateResponse>, Status> {
        if r.into_inner().shard_id != 0 {
            return Err(Status::not_found("分片不存在"));
        }
        let leader = self.no_leader_rounds.fetch_sub(1, Ordering::SeqCst) == 0;
        Ok(Response::new(GetRaftStateResponse {
            node_id: 1,
            server_state: if leader { "Leader".into() } else { "Follower".into() },
            is_leader: leader,
            has_leader: leader,
            current_leader: if leader { 1 } else { 0 },
            current_term: 1,
            has_last_log_index: true,
            last_log_index: 1,
            has_last_applied: true,
            last_applied: 0,
            voter_ids: vec![1],
        }))
    }

    async fn initialize(
        &self,
        _r: Request<InitializeRequest>,
    ) -> Result<Response<InitializeResponse>, Status> {
        Err(Status::unimplemented("mock 未实现"))
    }

    async fn add_learner(
        &self,
        _r: Request<AddLearnerRequest>,
    ) -> Result<Response<AddLearnerResponse>, Status> {
        Ok(Response::new(AddLearnerResponse {}))
    }

    async fn change_membership(
        &self,
        _r: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        Err(Status::unimplemented("mock 未实现"))
    }
}

/// 发现 3：多端点列表里首个端点建连失败，必须故障转移到下一个端点
/// （修复前 `event_client(&ep).await?` 在首个端点直接上抛，健康端点从未被尝试）
#[tokio::test(flavor = "multi_thread")]
async fn with_any_endpoint_first_down_failover_ok() {
    let dead = dead_addr().await;
    // status 走 get_raft_state（管理面），需同时注册 EventStore + RaftAdmin
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let live = format!("http://{}", listener.local_addr().expect("取地址"));
    let es = EventStoreServer::new(MockEventStore::default());
    let admin = RaftAdminServer::new(MockAdmin {
        no_leader_rounds: Arc::new(AtomicU32::new(0)),
    });
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(es)
            .add_service(admin)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    let endpoints = format!("{dead},{live}");
    let (code, out, err) = esctl(&endpoints, &["status"]);
    assert_eq!(code, Some(0), "故障转移应成功：{err} {out}");
}

/// 发现 5：leader unknown（选举中）退避重试不再被 tried 集合挡死
/// （修复前 push_back 后再次 pop 被 `tried.insert` 挡下，重试永不发生）
#[tokio::test(flavor = "multi_thread")]
async fn with_leader_unknown_backoff_retry_ok() {
    let mock = MockEventStore {
        unknown_appends: Arc::new(AtomicU32::new(2)),
    };
    let live = serve_event_store(mock.clone()).await;

    // append 走 with_leader；--shards 1 跳过分片探测（mock 无管理面）
    let (code, out, err) = esctl(&live, &["--shards", "1", "append", "s/x", "--event-type", "T", "--data", "d"]);
    assert_eq!(code, Some(0), "leader-unknown 重试后应成功：{err} {out}");
    assert_eq!(
        mock.unknown_appends.load(Ordering::SeqCst),
        0,
        "append 应被重试到计数耗尽（修复前只尝试 1 次）"
    );
}

/// 发现 12：with_admin_leader 的 3 轮重试覆盖 leader 探测失败（选举中）
#[tokio::test(flavor = "multi_thread")]
async fn with_admin_leader_probe_fail_retry_ok() {
    let mock = MockAdmin {
        no_leader_rounds: Arc::new(AtomicU32::new(2)),
    };
    let live = serve_admin(mock).await;

    // member add --learner-only 只走一次 with_admin_leader（add_learner）
    let (code, out, err) = esctl(
        &live,
        &[
            "member",
            "add",
            "--shard",
            "0",
            "--learner-only",
            "--no-blocking",
            "--member",
            "2@127.0.0.1:59999",
        ],
    );
    assert_eq!(code, Some(0), "leader 探测重试后应成功：{err} {out}");
}

/// 发现 8：watch --once 在收到 caught_up 前流关闭必须非零退出
/// （修复前返回 Ok(0)，脚本会把"未追平"误判为"已追平"）
#[tokio::test(flavor = "multi_thread")]
async fn watch_once_not_caught_up_exit_code_1() {
    let live = serve_event_store(MockEventStore::default()).await;

    let (code, _out, err) = esctl(&live, &["watch", "s", "--once", "--from-start"]);
    assert_eq!(code, Some(1), "未追平必须退出码 1：{err}");
    assert!(err.contains("caught_up"), "错误应说明未收到追平信号：{err}");
}
