//! 客户端连接管理测试：空节点列表、非法地址、stub server 上的连接复用。

use std::pin::Pin;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use es_proto::eventstore::event_store_server::EventStore;
use es_proto::eventstore::*;

/// 最小 stub：append 返回空响应，流式方法返回空流。
struct StubServer;

#[tonic::async_trait]
impl EventStore for StubServer {
    async fn append(
        &self,
        _request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        Ok(Response::new(AppendResponse {
            next_expected_version: 0,
            first_position: 0,
            last_position: 0,
            shard_id: 0,
        }))
    }

    type ReadStreamStream =
        Pin<Box<dyn Stream<Item = Result<ReadEventsResponse, Status>> + Send>>;
    async fn read_stream(
        &self,
        _request: Request<ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type ReadAllStream =
        Pin<Box<dyn Stream<Item = Result<ReadEventsResponse, Status>> + Send>>;
    async fn read_all(
        &self,
        _request: Request<ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<SubscribeResponse, Status>> + Send>>;
    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_stream_meta(
        &self,
        _request: Request<GetStreamMetaRequest>,
    ) -> Result<Response<GetStreamMetaResponse>, Status> {
        Ok(Response::new(GetStreamMetaResponse {
            exists: false,
            current_version: 0,
            shard_id: 0,
        }))
    }
}

/// 起一个 stub gRPC 服务，返回其地址。
async fn start_stub_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = listener.local_addr().expect("取地址");
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(event_store_server::EventStoreServer::new(StubServer))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    // 等服务器开始监听
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    format!("http://{addr}")
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
    let addr = start_stub_server().await;

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
}

#[tokio::test]
async fn read_stream_via_stub_empty() {
    let addr = start_stub_server().await;
    let mut client = es_client::EventStoreClient::connect(vec![addr])
        .await
        .expect("连接 stub");
    let events = client
        .read_stream(
            "s1".to_string(),
            0,
            10,
            es_client::Direction::Forward,
        )
        .await
        .expect("read_stream 成功");
    assert!(events.is_empty());
}
