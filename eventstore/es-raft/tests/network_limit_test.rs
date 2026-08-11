//! es-raft 网络层消息上限测试。
//!
//! 直测 network.rs 的三条路径(partition_test 的网络层不走 network.rs,
//! 是这些改动的盲区):
//! - 发送前拦截:超限批量不发 RPC,直接返回 PayloadTooLarge / Unreachable
//! - 接收侧兜底:对端返回 ResourceExhausted 时映射为 PayloadTooLarge
//! - 正常回程:小请求走通

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::error::RPCError;
use openraft::network::{RPCOption, RaftNetwork};
use openraft::raft::{AppendEntriesRequest, AppendEntriesResponse, VoteRequest};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Vote};
use tokio::net::TcpListener;

use es_core::{ExpectedVersion, Hlc, NewEvent};
use es_proto::eventstore::raft_rpc_server::{RaftRpc, RaftRpcServer};
use es_proto::eventstore::{
    RaftAppendEntriesRequest, RaftAppendEntriesResponse, RaftInstallSnapshotRequest,
    RaftInstallSnapshotResponse, RaftVoteRequest, RaftVoteResponse,
};
use es_raft::GrpcConnection;
use es_storage::{EsRequest, TypeConfig};

/// stub 服务模式
enum StubMode {
    /// append_entries 恒返回超限拒绝(模拟对端 gRPC 8MB 解码上限)
    Reject,
    /// 正常解码并返回 Success,记录调用次数
    Echo(Arc<AtomicU64>),
}

/// 进程内 tonic stub 服务端:只实现 append_entries,其余方法不可达
struct StubService {
    mode: StubMode,
}

#[tonic::async_trait]
impl RaftRpc for StubService {
    async fn vote(
        &self,
        _request: tonic::Request<RaftVoteRequest>,
    ) -> Result<tonic::Response<RaftVoteResponse>, tonic::Status> {
        match &self.mode {
            // Reject 模式同时拒 vote：验证 network.rs 对 vote 不做
            // ResourceExhausted → PayloadTooLarge 映射（openraft 对
            // Vote 动作的 PayloadTooLarge 是 unreachable! panic）
            StubMode::Reject => Err(tonic::Status::resource_exhausted(
                "Error, decoded message length too large: 8388608 > 8388608",
            )),
            StubMode::Echo(_) => Err(tonic::Status::unimplemented("vote")),
        }
    }

    async fn append_entries(
        &self,
        request: tonic::Request<RaftAppendEntriesRequest>,
    ) -> Result<tonic::Response<RaftAppendEntriesResponse>, tonic::Status> {
        match &self.mode {
            StubMode::Reject => Err(tonic::Status::resource_exhausted(
                // 与 tonic 0.14 解码超限的返回一致
                "Error, decoded message length too large: 8388608 > 8388608",
            )),
            StubMode::Echo(count) => {
                count.fetch_add(1, Ordering::SeqCst);
                let req: AppendEntriesRequest<TypeConfig> =
                    bincode::serde::decode_from_slice(
                        &request.get_ref().payload,
                        bincode::config::standard(),
                    )
                    .expect("解码请求")
                    .0;
                // 响应必须基于请求的 term/vote,否则上层逻辑异常
                let resp = AppendEntriesResponse::<u64>::Success;
                let payload = bincode::serde::encode_to_vec(&resp, bincode::config::standard())
                    .expect("编码响应");
                let _ = req; // 仅验证可解码
                Ok(tonic::Response::new(RaftAppendEntriesResponse { payload }))
            }
        }
    }

    async fn install_snapshot(
        &self,
        _request: tonic::Request<RaftInstallSnapshotRequest>,
    ) -> Result<tonic::Response<RaftInstallSnapshotResponse>, tonic::Status> {
        Err(tonic::Status::unimplemented("install_snapshot"))
    }
}

/// 起一个进程内 stub 服务,返回监听地址
async fn start_stub(mode: StubMode) -> String {
    let service = StubService { mode };
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("绑定端口");
    let addr = listener.local_addr().expect("本地地址");
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RaftRpcServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("stub 服务退出异常");
    });
    addr.to_string()
}

/// 手工构造 AppendEntriesRequest,data 字节数控制负载大小
fn append_req(entries: usize, data_len: usize) -> AppendEntriesRequest<TypeConfig> {
    AppendEntriesRequest {
        vote: Vote::new_committed(1, 1),
        prev_log_id: None,
        entries: (0..entries as u64)
            .map(|i| Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), i + 1),
                payload: EntryPayload::Normal(EsRequest::Append {
                    stream_id: "s".into(),
                    expected_version: ExpectedVersion::Any,
                    events: vec![NewEvent {
                        event_id: uuid::Uuid::new_v4(),
                        event_type: "t".into(),
                        data: vec![0u8; data_len],
                        metadata: vec![],
                    }],
                    hlc: Hlc::now(),
                }),
            })
            .collect(),
        leader_commit: None,
    }
}

fn rpc_option() -> RPCOption {
    RPCOption::new(std::time::Duration::from_secs(10))
}

/// 对端 ResourceExhausted → PayloadTooLarge(兜底映射,hint = 条数/2)
#[tokio::test]
async fn resource_exhausted_maps_to_payload_too_large_with_hint() {
    let addr = start_stub(StubMode::Reject).await;
    let mut conn = GrpcConnection::new(1, 2, addr, None);

    // 4 条中事件(远小于预算):发送前不拦,由对端拒绝后兜底映射
    let req = append_req(4, 1024);
    let err = conn
        .append_entries(req, rpc_option())
        .await
        .expect_err("应被拒");
    match err {
        RPCError::PayloadTooLarge(p) => {
            assert_eq!(p.entries_hint(), 2, "4 条被拒 → hint = 4/2 = 2");
        }
        other => panic!("应映射为 PayloadTooLarge,实际 {other:?}"),
    }
}

/// 发送前拦截:超限批量不发 RPC(对端调用计数为 0),直接返回 PayloadTooLarge
#[tokio::test]
async fn oversized_payload_rejected_before_rpc() {
    let count = Arc::new(AtomicU64::new(0));
    let addr = start_stub(StubMode::Echo(count.clone())).await;
    let mut conn = GrpcConnection::new(1, 2, addr, None);

    // 3 × 3MiB ≈ 9MiB > 8MB 预算:发送前必须拦截
    let req = append_req(3, 3 * 1024 * 1024);
    let err = conn
        .append_entries(req, rpc_option())
        .await
        .expect_err("超限应被拦截");
    assert!(
        matches!(err, RPCError::PayloadTooLarge(_)),
        "应返回 PayloadTooLarge,实际 {err:?}"
    );
    assert_eq!(
        count.load(Ordering::SeqCst),
        0,
        "超限请求不应发出 RPC"
    );
}

/// 单条超限 → Unreachable(不构造 hint=1,避免 openraft 死循环)
#[tokio::test]
async fn single_oversized_entry_maps_to_unreachable() {
    let count = Arc::new(AtomicU64::new(0));
    let addr = start_stub(StubMode::Echo(count.clone())).await;
    let mut conn = GrpcConnection::new(1, 2, addr, None);

    // 1 条 × 8MiB ≈ 8MiB+ > 预算:单条超限,拆无可拆
    let req = append_req(1, 8 * 1024 * 1024);
    let err = conn
        .append_entries(req, rpc_option())
        .await
        .expect_err("单条超限应报错");
    assert!(
        matches!(err, RPCError::Unreachable(_)),
        "单条超限应映射为 Unreachable,实际 {err:?}"
    );
    assert_eq!(count.load(Ordering::SeqCst), 0, "不应发出 RPC");
}

/// 正常回程:小请求走通,响应正确解码
#[tokio::test]
async fn small_request_roundtrips() {
    let count = Arc::new(AtomicU64::new(0));
    let addr = start_stub(StubMode::Echo(count.clone())).await;
    let mut conn = GrpcConnection::new(1, 2, addr, None);

    let req = append_req(2, 1024);
    let resp = conn
        .append_entries(req, rpc_option())
        .await
        .expect("小请求应成功");
    assert!(
        matches!(resp, AppendEntriesResponse::Success),
        "应得到 Success,实际 {resp:?}"
    );
    assert_eq!(count.load(Ordering::SeqCst), 1, "应恰好调用一次");
}

/// vote 的 ResourceExhausted 不映射为 PayloadTooLarge。
///
/// openraft 0.9.25 对 Vote 动作的 PayloadTooLarge 在 update_hint 里是
/// `unreachable!` panic(replication/mod.rs),映射会直接打崩 leader;
/// network.rs 对 vote 保持 net_err(Network 错误)原样包装。
#[tokio::test]
async fn vote_resource_exhausted_stays_network_error() {
    let addr = start_stub(StubMode::Reject).await;
    let mut conn = GrpcConnection::new(1, 2, addr, None);

    let req = openraft::raft::VoteRequest {
        vote: Vote::new_committed(1, 1),
        last_log_id: None,
    };
    let err = conn.vote(req, rpc_option()).await.expect_err("应被拒");
    assert!(
        matches!(err, RPCError::Network(_)),
        "vote 超限应保持 Network 错误(不映射 PayloadTooLarge),实际 {err:?}"
    );
}
