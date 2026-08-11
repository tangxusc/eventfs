//! 端到端集成测试：进程内单节点服务器上的 SDK read_all / subscribe / get_stream_meta。

use std::collections::HashSet;
use std::time::Duration;

use tokio_stream::StreamExt;

use es_client::{EventStoreClient, SubscribeStream, SubscribeTarget, subscribe_response};
use es_server::config::{Config, NodeConfig, ShardConfig, StorageConfig};
use es_server::Server;

/// 启动进程内测试服务器（单节点，2 分片，立即成为 leader）。
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
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
        },
        shards: ShardConfig { num_shards: 2 },
        tls: None,
    };

    let server = Server::new(config).expect("创建服务器");
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

    let service = es_server::service::EsService::new(server.shard_manager().clone());
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(
                es_proto::eventstore::event_store_server::EventStoreServer::new(service),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    // 轮询等待 gRPC 服务器就绪（替代固定 sleep，抗 CI 高负载）
    wait_server_ready(&addr, Duration::from_secs(10)).await;

    (addr, handle, server, dir)
}

/// 轮询建连直到 gRPC 服务器就绪或超时。
async fn wait_server_ready(addr: &str, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
            .expect("测试地址合法");
        match endpoint.connect().await {
            Ok(_channel) => return,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("等待 gRPC 服务器就绪超时: {e}"),
        }
    }
}

/// 追加 1 条事件（期望版本 any）
async fn append_one(client: &mut EventStoreClient, stream_id: &str, data: u8) {
    client
        .append(
            stream_id.to_string(),
            es_client::ExpectedVersionBuilder::any(),
            vec![es_client::EventBuilder::new("T").data(vec![data]).build()],
        )
        .await
        .expect("append 成功");
}

/// 收订阅流直到 caught_up 分界信号，返回期间的事件。
async fn drain_until_caught_up(stream: &mut SubscribeStream) -> Vec<es_proto::eventstore::Event> {
    let mut events = Vec::new();
    while let Some(resp) = stream.next().await {
        let resp = resp.expect("订阅流无错误");
        match resp.payload {
            Some(subscribe_response::Payload::Event(e)) => events.push(e),
            Some(subscribe_response::Payload::CaughtUp(_)) => break,
            None => {}
        }
    }
    events
}

#[tokio::test]
async fn read_all_pages_roundtrip() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 10 个流各 1 条，分布在 2 个分片
    for i in 0..10 {
        append_one(&mut client, &format!("s{i}"), i).await;
    }

    // 每分片限额 max_count=4，归并后首 max_count 条；翻页直到空页
    //（服务端对读尽分片仍返回非空游标，空页才是终止条件）
    let mut all = Vec::new();
    let (page, mut cursor) = client
        .read_all(vec![0, 1], 0, 4, es_client::Direction::Forward, vec![])
        .await
        .expect("首页");
    all.extend(page);
    let mut pages = 1;
    loop {
        assert!(pages < 10, "翻页应有限，疑似死循环: {pages}");
        let (page, next) = client
            .read_all(vec![], 0, 4, es_client::Direction::Forward, cursor)
            .await
            .expect("翻页");
        pages += 1;
        let page_len = page.len();
        all.extend(page);
        if page_len == 0 {
            break;
        }
        cursor = next;
    }

    assert!(pages >= 2, "max_count=4 应产生多页: {pages}");
    assert_eq!(all.len(), 10, "翻页后应集齐全部事件");
    // 不重不漏（每个流 1 条，data[0] 唯一）
    let versions: HashSet<u8> = all.iter().map(|e| e.data[0]).collect();
    assert_eq!(versions.len(), 10, "事件应全部不同且无重复");
}

#[tokio::test]
async fn subscribe_catch_up_then_live() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 3 条历史
    for i in 0..3 {
        append_one(&mut client, "sub", i).await;
    }

    let mut stream = client
        .subscribe(
            SubscribeTarget::Stream("sub".to_string()),
            0,
            true, // 从头开始
        )
        .await
        .expect("订阅成功");

    // catch-up 阶段补齐历史
    let history = drain_until_caught_up(&mut stream).await;
    assert_eq!(history.len(), 3, "catch-up 应补齐 3 条历史");
    assert_eq!(history[0].data, vec![0]);
    assert_eq!(history[1].data, vec![1]);
    assert_eq!(history[2].data, vec![2]);

    // live 阶段：新写入应被推送（连发多条，验证恰好一次、无重复无丢失）
    for i in 0..5 {
        append_one(&mut client, "sub", 100 + i).await;
    }
    for i in 0..5 {
        let live = stream.next().await.expect("收到推送").expect("无错误");
        match live.payload {
            Some(subscribe_response::Payload::Event(e)) => {
                assert_eq!(e.data, vec![100 + i], "第 {i} 条 live 事件")
            }
            other => panic!("应收到 live 事件: {other:?}"),
        }
    }
}

#[tokio::test]
async fn subscribe_all_target_receives_shard_events() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 写 1 条，经 get_stream_meta 确认所在分片，再订阅该分片
    append_one(&mut client, "s0", 1).await;
    let meta = client
        .get_stream_meta("s0".to_string())
        .await
        .expect("get_stream_meta");
    assert!(meta.exists);

    let mut stream = client
        .subscribe(SubscribeTarget::All { shard_id: meta.shard_id }, 0, true)
        .await
        .expect("订阅全部");

    // catch-up 收到该分片已有事件
    let history = drain_until_caught_up(&mut stream).await;
    assert!(
        history.iter().any(|e| e.stream_id == "s0"),
        "catch-up 应含 s0: {:?}",
        history.iter().map(|e| &e.stream_id).collect::<Vec<_>>()
    );

    // live 阶段：写同分片新流，应被推送
    append_one(&mut client, "s0-live", 2).await;
    let live = stream.next().await.expect("收到推送").expect("无错误");
    match live.payload {
        Some(subscribe_response::Payload::Event(e)) => assert_eq!(e.stream_id, "s0-live"),
        other => panic!("应收到 live 事件: {other:?}"),
    }
}

#[tokio::test]
async fn get_stream_meta_reports_version_and_shard() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 不存在的流
    let missing = client
        .get_stream_meta("nope".to_string())
        .await
        .expect("get_stream_meta");
    assert!(!missing.exists);

    // 3 条事件
    for i in 0..3 {
        append_one(&mut client, "meta", i).await;
    }
    let meta = client
        .get_stream_meta("meta".to_string())
        .await
        .expect("get_stream_meta");
    assert!(meta.exists);
    assert_eq!(meta.current_version, 2, "3 条事件 version 0..2");
    assert_eq!(meta.shard_id, es_core::route("meta", 2), "与分片路由一致");
}
