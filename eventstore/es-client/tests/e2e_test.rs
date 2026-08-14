//! 端到端集成测试：进程内单节点服务器上的 SDK read_all / subscribe / get_stream_meta。

use std::collections::HashSet;
use std::time::Duration;

use tokio_stream::StreamExt;

use es_client::{
    ClientError, Direction, EventStoreClient, SubscribeStream, SubscribeTarget, subscribe_response,
};
use es_client::{EventBuilder, ExpectedVersionBuilder};
use es_server::Server;
use es_server::config::{Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig};
use tonic::Code;

/// 启动进程内测试服务器（单节点，2 分片，立即成为 leader，默认大小限制）。
///
/// 返回 (地址, 服务器任务句柄, Server, TempDir)。
/// TempDir 必须由调用方持有到测试结束，drop 即删数据目录。
async fn start_test_server() -> (
    String,
    tokio::task::JoinHandle<()>,
    Server,
    tempfile::TempDir,
) {
    start_test_server_with_limits(Default::default()).await
}

/// 启动进程内测试服务器（可自定义 [LimitsSection] 大小限制）。
async fn start_test_server_with_limits(
    limits: es_server::config::LimitsSection,
) -> (
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
        limits: limits.clone(),
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

    let service = es_server::service::EsService::with_limits(
        server.shard_manager().clone(),
        limits,
        server.route_table().clone(),
        &config,
    )
    .expect("创建服务");
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(es_proto::eventstore::event_store_server::EventStoreServer::new(service))
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
        let endpoint =
            tonic::transport::Endpoint::from_shared(addr.to_string()).expect("测试地址合法");
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
async fn drain_until_caught_up(
    stream: &mut SubscribeStream,
) -> Vec<es_proto::eventstore::SubscriptionEvent> {
    let mut events = Vec::new();
    while let Some(resp) = stream.next().await {
        let resp = resp.expect("订阅流无错误");
        match resp.payload {
            Some(subscribe_response::Payload::Event(e)) => events.push(e),
            Some(subscribe_response::Payload::CaughtUp(_)) => break,
            Some(subscribe_response::Payload::Degraded(_)) => panic!("健康订阅不应降级"),
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

/// 反向翻页必须干净终止：消费到 position 0 的分片游标带 ended 标记，
/// 服务端返回空页 —— 空页是正反两向统一的终止条件。
/// 修复前（position 0 游标被丢弃 → 客户端保留旧游标）该测试死循环在
/// pages 守卫上，或首页即读尽时第二页触发 invalid_argument。
#[tokio::test]
async fn read_all_backward_pages_roundtrip() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 10 个流各 1 条，分布在 2 个分片
    for i in 0..10 {
        append_one(&mut client, &format!("b{i}"), i).await;
    }

    // 首页从 u64::MAX 哨兵反向读；翻页把 next_positions 原样透传，直到空页
    let mut all = Vec::new();
    let (page, mut cursor) = client
        .read_all(
            vec![0, 1],
            u64::MAX,
            4,
            es_client::Direction::Backward,
            vec![],
        )
        .await
        .expect("首页");
    all.extend(page);
    let mut pages = 1;
    loop {
        assert!(pages < 10, "反向翻页应有限，疑似死循环: {pages}");
        let (page, next) = client
            .read_all(vec![], 0, 4, es_client::Direction::Backward, cursor)
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
    assert_eq!(all.len(), 10, "反向翻页后应集齐全部事件");
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
        .subscribe(SubscribeTarget::Streams(vec!["sub".to_string()]))
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
        .subscribe(SubscribeTarget::All)
        .await
        .expect("订阅全部");

    // catch-up 收到该分片已有事件
    let history = drain_until_caught_up(&mut stream).await;
    assert!(
        history.iter().any(|e| e.stream_id == "s0"),
        "catch-up 应含 s0: {:?}",
        history.iter().map(|e| &e.stream_id).collect::<Vec<_>>()
    );

    // live 阶段：写已有流 s0（显式分配下流的归属固定，事件必进本分片
    // broadcast；不能写新流——最少流分配可能把它放到别的分片，订阅收不到）
    append_one(&mut client, "s0", 2).await;
    let live = stream.next().await.expect("收到推送").expect("无错误");
    match live.payload {
        Some(subscribe_response::Payload::Event(e)) => assert_eq!(e.stream_id, "s0"),
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

/// 带超时的下一条订阅响应（live 阶段防挂死）。
///
/// 订阅流在 live 阶段会一直挂着等新事件，若服务端丢事件或测试断言写错，
/// `next()` 会永久阻塞，表现为整个测试卡死而非失败——必须带超时。
async fn next_sub_timeout(stream: &mut SubscribeStream) -> Option<subscribe_response::Payload> {
    let fut = stream.next();
    match tokio::time::timeout(Duration::from_secs(5), fut).await {
        Ok(Some(Ok(resp))) => resp.payload,
        Ok(Some(Err(e))) => panic!("订阅流出错: {e}"),
        Ok(None) => None,
        Err(_) => panic!("等订阅响应超过 5 秒，事件可能丢失"),
    }
}

/// catch-up 窗口丢事件回归测试：订阅进行中（历史扫描窗口内）提交的事件
/// 必须恰好投递一次。
///
/// 服务端实现（service.rs）：先注册广播接收器 → 再扫描历史 → 用历史尾水位
/// 跳过「快照与广播各一份」的重复窗口。本测试用大历史拉开扫描窗口，订阅
/// 返回后不先 drain 立即追加，让提交落在窗口内/后；断言 2000 历史 + 5 窗口
/// 写入全部收到且无重复。修复前（先扫描历史再注册广播）窗口期事件既不在
/// 历史也不在广播，live 阶段等待 5 秒超时失败。
#[tokio::test]
async fn subscribe_writes_during_catchup_exactly_once() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 大历史：拉开 catch-up 扫描窗口
    for i in 0..2000 {
        append_one(&mut client, "sub", (i % 251) as u8).await;
    }

    let mut stream = client
        .subscribe(SubscribeTarget::Streams(vec!["sub".to_string()]))
        .await
        .expect("订阅成功");

    // 关键：不先 drain，立刻追加，让提交落在 catch-up 窗口内/后
    for i in 0..5 {
        append_one(&mut client, "sub", (200 + i) as u8).await;
    }

    // 以 version 为事件身份（单流内严格递增），断言并集恰好一次：
    // 窗口内事件可能在历史快照（catch-up 阶段）或广播（live 阶段）
    let mut versions: HashSet<u64> = HashSet::new();
    for ev in drain_until_caught_up(&mut stream).await {
        assert!(versions.insert(ev.version), "版本 {} 重复投递", ev.version);
    }
    while versions.len() < 2005 {
        let payload = next_sub_timeout(&mut stream)
            .await
            .expect("订阅流不应提前结束");
        match payload {
            subscribe_response::Payload::Event(e) => {
                assert!(versions.insert(e.version), "版本 {} 重复投递", e.version);
            }
            subscribe_response::Payload::CaughtUp(_) => continue, // 防重入
            subscribe_response::Payload::Degraded(_) => panic!("健康订阅不应降级"),
        }
    }

    assert_eq!(versions.len(), 2005, "应恰收到 2005 条");
    for v in 0..2005 {
        assert!(versions.contains(&v), "版本 {v} 缺失");
    }
}

/// `$all` 变体以公开业务身份 `(stream_id, version)` 去重。
#[tokio::test]
async fn subscribe_all_writes_during_catchup_exactly_once() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    append_one(&mut client, "all-sub", 1).await;

    // 大历史：拉开 catch-up 扫描窗口
    for i in 1..2000 {
        append_one(&mut client, "all-sub", (i % 251) as u8).await;
    }

    let mut stream = client
        .subscribe(SubscribeTarget::All)
        .await
        .expect("订阅全部");

    // 订阅返回后立刻追加，落在窗口内/后
    for i in 0..5 {
        append_one(&mut client, "all-sub", (200 + i) as u8).await;
    }

    let mut versions: HashSet<u64> = HashSet::new();
    for ev in drain_until_caught_up(&mut stream).await {
        if ev.stream_id == "all-sub" {
            assert!(versions.insert(ev.version), "版本 {} 重复投递", ev.version);
        }
    }
    while versions.len() < 2005 {
        let payload = next_sub_timeout(&mut stream)
            .await
            .expect("订阅流不应提前结束");
        match payload {
            subscribe_response::Payload::Event(e) => {
                if e.stream_id == "all-sub" {
                    assert!(versions.insert(e.version), "版本 {} 重复投递", e.version);
                }
            }
            subscribe_response::Payload::CaughtUp(_) => continue,
            subscribe_response::Payload::Degraded(_) => panic!("健康订阅不应降级"),
        }
    }

    assert_eq!(versions.len(), 2005, "应恰收到 2005 条");
    for version in 0..2005 {
        assert!(versions.contains(&version), "版本 {version} 缺失");
    }
}

/// 服务端小上限时超限 append 被权威拒绝（FailedPrecondition 原样上抛）。
///
/// 客户端默认常量（1MiB）大于服务端配置（1KB），验证「终以服务端为准」。
#[tokio::test]
async fn append_oversized_event_rejected_by_server_limits() {
    let limits = es_server::config::LimitsSection {
        max_event_bytes: 1024,
        max_append_batch_bytes: 4096,
    };
    let (addr, _handle, _server, _dir) = start_test_server_with_limits(limits).await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 2KB 事件:客户端前置校验（1MiB）放行,服务端（1KB）拒绝
    let err = client
        .append(
            "s".into(),
            ExpectedVersionBuilder::any(),
            vec![EventBuilder::new("T").data(vec![0u8; 2048]).build()],
        )
        .await
        .expect_err("超限应被服务端拒绝");
    match err {
        ClientError::RpcFailed { code, message } => {
            assert_eq!(code, tonic::Code::FailedPrecondition);
            assert!(
                message.starts_with("append payload too large"),
                "消息应说明超限: {message}"
            );
        }
        other => panic!("应原样上抛 RpcFailed(FailedPrecondition),实际 {other:?}"),
    }

    // 流里没有任何事件被写入（服务端校验先于分配，流未创建 → 读 NotFound）
    let err = client
        .read_stream("s".into(), 0, 10, Direction::Forward)
        .await
        .expect_err("未创建流应读不到");
    assert!(
        matches!(
            err,
            ClientError::RpcFailed {
                code: Code::NotFound,
                ..
            }
        ),
        "应为 NotFound,实际 {err:?}"
    );
}

/// 服务端 batch 级校验:单事件各不过限、但 proto 编码后总字节超限被拒。
///
/// 覆盖 service.rs 的 encoded_len 权威校验分支——「海量小事件 × 逐事件头」
/// 的线缆膨胀正是该分支存在的意义(客户端 data+metadata 总和近似拦不住)。
#[tokio::test]
async fn append_batch_encoded_len_over_limit_rejected() {
    let limits = es_server::config::LimitsSection {
        max_event_bytes: 1024,
        max_append_batch_bytes: 4096,
    };
    let (addr, _handle, _server, _dir) = start_test_server_with_limits(limits).await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 5 × 1000B:单事件(1000 ≤ 1024)过逐事件检查;总和 5000B,
    // proto 编码后(每事件 ~21B 头)≈ 5100 > 4096,触发 batch 级拒绝
    let events = (0..5)
        .map(|i| {
            EventBuilder::new(&format!("T{i}"))
                .data(vec![0u8; 1000])
                .build()
        })
        .collect::<Vec<_>>();
    let err = client
        .append("s".into(), ExpectedVersionBuilder::any(), events)
        .await
        .expect_err("batch 编码超限应被拒");
    match err {
        ClientError::RpcFailed { code, message } => {
            assert_eq!(code, tonic::Code::FailedPrecondition);
            assert!(
                message.starts_with("append payload too large: request"),
                "应说明 request 超限: {message}"
            );
        }
        other => panic!("应原样上抛 RpcFailed(FailedPrecondition),实际 {other:?}"),
    }
}

/// 客户端前置校验:单事件超过默认上限（1MiB）本地拒绝,不发 RPC。
#[tokio::test]
async fn append_client_side_limit_rejects_without_rpc() {
    let (addr, _handle, _server, _dir) = start_test_server().await;
    let mut client = EventStoreClient::connect(vec![addr]).await.expect("连接");

    // 1MiB + 1 字节:超过客户端默认单事件上限（es_core::limits::MAX_EVENT_PAYLOAD_BYTES）
    let big = vec![0u8; 1024 * 1024 + 1];
    let err = client
        .append(
            "s".into(),
            ExpectedVersionBuilder::any(),
            vec![EventBuilder::new("T").data(big).build()],
        )
        .await
        .expect_err("超限应在本地拒绝");
    assert!(
        matches!(err, ClientError::PayloadTooLarge(_)),
        "应为 PayloadTooLarge,实际 {err:?}"
    );

    // 未发出 RPC:流未创建（路由表无记录）→ 读返回 NotFound（显式分配语义：
    // 未创建过的流不存在于路由表，读不到）
    let err = client
        .read_stream("s".into(), 0, 10, Direction::Forward)
        .await
        .expect_err("未创建流应读不到");
    assert!(
        matches!(
            err,
            ClientError::RpcFailed {
                code: Code::NotFound,
                ..
            }
        ),
        "应为 NotFound,实际 {err:?}"
    );
}
