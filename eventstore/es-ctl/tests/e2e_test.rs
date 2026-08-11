//! esctl 端到端测试：进程内起服务（EventStore + RaftAdmin 双服务），
//! 用 esctl 真实二进制子进程跑全链路命令。

use std::process::{Command, Output};
use std::time::Duration;

use es_proto::eventstore::event_store_server::EventStoreServer;
use es_proto::eventstore::raft_admin_server::RaftAdminServer;
use es_proto::eventstore::raft_rpc_server::RaftRpcServer;
use es_server::Server;
use es_server::config::{Config, NodeConfig, ShardConfig, StorageConfig};

/// 启动测试服务器（单节点、num_shards=2、每分片单成员自举）。
///
/// 与 es-server 的 e2e 基建差异：补注册 RaftAdminServer，esctl 管理面命令可用。
/// 返回 (地址, 服务句柄, Server, TempDir)；TempDir 由调用方持有至测试结束。
async fn start_server() -> (
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

    // 单节点集群：每个分片把自己设为唯一成员，立即成为 leader
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

    let sm = server.shard_manager().clone();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(es_server::service::EsService::new(
                sm.clone(),
            )))
            .add_service(RaftAdminServer::new(es_raft::RaftAdminService::new(sm)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    // 等 gRPC 服务器真正开始监听
    tokio::time::sleep(Duration::from_millis(100)).await;

    (addr, handle, server, dir)
}

/// 启动测试服务器但不对分片自举（esctl init 用例）。
async fn start_server_uninitialized(num_shards: u64) -> (
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
        shards: ShardConfig { num_shards },
        tls: None,
    };

    let server = Server::new(config).expect("创建服务器");
    server.init().await.expect("初始化");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().expect("取本地地址"));

    let sm = server.shard_manager().clone();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(es_server::service::EsService::new(
                sm.clone(),
            )))
            .add_service(RaftAdminServer::new(es_raft::RaftAdminService::new(sm)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, handle, server, dir)
}

/// 以子进程方式运行 esctl，返回完整输出。
fn esctl(endpoints: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(["--endpoints", endpoints]);
    cmd.args(args);
    cmd.output().expect("运行 esctl")
}

/// 标准输出转 UTF-8（失败则空串）
fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).to_string()
}

/// 标准错误转 UTF-8
fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn append_read_meta_readall_data_plane_roundtrip() {
    let (addr, handle, _server, _dir) = start_server().await;

    // append 两条事件
    let out = esctl(
        &addr,
        &[
            "append",
            "orders/1",
            "--event-type",
            "OrderPlaced",
            "--data",
            r#"{"qty":1}"#,
            "--metadata",
            "k=v",
        ],
    );
    assert!(out.status.success(), "append 失败: {}", stderr(&out));
    assert!(stdout(&out).contains("OK 写入成功"), "{}", stdout(&out));
    // 版本从 0 起：写入 1 条后当前版本 0
    assert!(
        stdout(&out).contains("next_expected_version: 0"),
        "{}",
        stdout(&out)
    );

    let out = esctl(
        &addr,
        &[
            "append",
            "orders/1",
            "--event-type",
            "OrderShipped",
            "--data",
            "shipped",
        ],
    );
    assert!(out.status.success(), "第二条 append 失败: {}", stderr(&out));

    // meta：两条事件后当前版本 1（版本从 0 起）
    let out = esctl(&addr, &["meta", "orders/1"]);
    assert!(out.status.success(), "meta 失败: {}", stderr(&out));
    assert!(stdout(&out).contains("exists: true"), "{}", stdout(&out));
    assert!(
        stdout(&out).contains("current_version: 1"),
        "{}",
        stdout(&out)
    );

    // read：两条事件按版本序输出
    let out = esctl(&addr, &["read", "orders/1"]);
    assert!(out.status.success(), "read 失败: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("[OrderPlaced]"), "{text}");
    assert!(text.contains("[OrderShipped]"), "{text}");
    assert!(text.contains(r#"{"qty":1}"#), "{text}");

    // readall：跨分片聚合，至少包含已写事件
    let out = esctl(&addr, &["readall"]);
    assert!(out.status.success(), "readall 失败: {}", stderr(&out));
    assert!(stdout(&out).contains("OrderPlaced"), "{}", stdout(&out));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn optimistic_conflict_exit_1_chinese_hint() {
    let (addr, handle, _server, _dir) = start_server().await;

    let out = esctl(
        &addr,
        &["append", "s/conflict", "--event-type", "T", "--data", "1"],
    );
    assert!(out.status.success());

    // 流已存在，期望 nostream 必然冲突
    let out = esctl(
        &addr,
        &[
            "append",
            "s/conflict",
            "--event-type",
            "T",
            "--data",
            "2",
            "--expected-version",
            "nostream",
        ],
    );
    assert_eq!(out.status.code(), Some(1), "乐观冲突应退出码 1");
    assert!(stderr(&out).contains("乐观并发冲突"), "{}", stderr(&out));

    // exact 版本对不上同样冲突
    let out = esctl(
        &addr,
        &[
            "append",
            "s/conflict",
            "--event-type",
            "T",
            "--data",
            "3",
            "--expected-version",
            "99",
        ],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("乐观并发冲突"), "{}", stderr(&out));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn json_output_parsable() {
    let (addr, handle, _server, _dir) = start_server().await;

    esctl(
        &addr,
        &["append", "s/json", "--event-type", "T", "--data", "x"],
    );

    let out = esctl(&addr, &["-w", "json", "read", "s/json"]);
    assert!(out.status.success());
    let json: serde_json::Value =
        serde_json::from_str(&stdout(&out)).expect("read -w json 必须是合法 JSON");
    let events = json["events"].as_array().expect("events 数组");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["stream_id"], "s/json");
    assert_eq!(events[0]["data"], "x");
    assert!(
        events[0]["event_id"].as_str().is_some(),
        "event_id 应为字符串"
    );

    // meta 的 json 结构（1 条事件后当前版本 0）
    let out = esctl(&addr, &["-w", "json", "meta", "s/json"]);
    let json: serde_json::Value = serde_json::from_str(&stdout(&out)).expect("meta -w json");
    assert_eq!(json["exists"], true);
    assert_eq!(json["current_version"], 0);

    // table 格式有表头
    let out = esctl(&addr, &["-w", "table", "read", "s/json"]);
    assert!(stdout(&out).contains("STREAM"), "{}", stdout(&out));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_exits_after_catchup() {
    let (addr, handle, _server, _dir) = start_server().await;

    esctl(
        &addr,
        &["append", "s/watch", "--event-type", "W1", "--data", "a"],
    );
    esctl(
        &addr,
        &["append", "s/watch", "--event-type", "W2", "--data", "b"],
    );

    let out = esctl(&addr, &["watch", "s/watch", "--once", "--from-start"]);
    assert!(out.status.success(), "watch 失败: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("[W1]"), "{text}");
    assert!(text.contains("[W2]"), "{text}");
    assert!(text.contains("已追平"), "{text}");

    // 增量订阅：from-exclusive=0（不含）→ 只给版本 1（第二条）
    let out = esctl(
        &addr,
        &["watch", "s/watch", "--once", "--from-exclusive", "0"],
    );
    let text = stdout(&out);
    assert!(text.contains("[W2]"), "{text}");
    assert!(!text.contains("[W1]"), "增量订阅不应重复历史: {text}");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn status_and_member_list_single_node() {
    let (addr, handle, _server, _dir) = start_server().await;

    let out = esctl(&addr, &["status"]);
    assert!(out.status.success(), "status 失败: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("reachable=true"), "{text}");
    assert!(text.contains("leader_of=[0,1]"), "{text}");

    let out = esctl(&addr, &["member", "list"]);
    assert!(out.status.success(), "member list 失败: {}", stderr(&out));
    let text = stdout(&out);
    assert!(text.contains("Leader"), "{text}");
    assert!(text.contains("voters=[1]"), "{text}");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn init_initializes_non_self_bootstrapped() {
    let (addr, handle, _server, _dir) = start_server_uninitialized(1).await;

    // 未自举时 status 显示可达但没有 leader
    let out = esctl(&addr, &["status"]);
    assert!(out.status.success(), "status 失败: {}", stderr(&out));
    assert!(stdout(&out).contains("reachable=true"), "{}", stdout(&out));

    // init 自举分片 0
    let out = esctl(
        &addr,
        &["init", "--shard", "0", "--member", "1@127.0.0.1:50051"],
    );
    assert!(out.status.success(), "init 失败: {}", stderr(&out));
    assert!(stdout(&out).contains("已初始化"), "{}", stdout(&out));

    // 等 raft 选举出 leader
    tokio::time::sleep(Duration::from_millis(800)).await;
    let out = esctl(&addr, &["status"]);
    assert!(stdout(&out).contains("leader_of=[0]"), "{}", stdout(&out));

    // 重复 init：已初始化，退出码 1 且告警
    let out = esctl(
        &addr,
        &["init", "--shard", "0", "--member", "1@127.0.0.1:50051"],
    );
    assert_eq!(out.status.code(), Some(1), "重复 init 应失败");
    assert!(stderr(&out).contains("已初始化"), "{}", stderr(&out));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn unreachable_endpoint_exit_1() {
    // 无服务在监听
    let out = esctl("http://127.0.0.1:59999", &["status"]);
    assert_eq!(out.status.code(), Some(1), "不可达应退出码 1");
    assert!(
        stderr(&out).contains("不可达") || !stderr(&out).is_empty(),
        "{}",
        stderr(&out)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bad_args_exit_2() {
    let out = esctl("http://127.0.0.1:59999", &["append", "s", "--data", "x"]);
    // 缺 --event-type：clap 报错退出码 2
    assert_eq!(out.status.code(), Some(2));
    assert!(stderr(&out).contains("event-type"), "{}", stderr(&out));
}

#[tokio::test(flavor = "multi_thread")]
async fn https_self_signed_cert() {
    use tonic::transport::ServerTlsConfig;

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
        shards: ShardConfig { num_shards: 1 },
        tls: None,
    };
    let server = Server::new(config).expect("创建服务器");
    server.init().await.expect("初始化");
    let members = std::collections::BTreeSet::from([1u64]);
    for shard_id in 0..1 {
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

    // 自签证书（与 es-proto tls 测试同款做法）
    let certified =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("生成自签证书");
    let cert_pem = certified.cert.pem();
    let key_pem = certified.key_pair.serialize_pem();
    let identity = tonic::transport::Identity::from_pem(cert_pem.as_bytes(), key_pem.as_bytes());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("https://{}", listener.local_addr().expect("取地址"));
    let sm = server.shard_manager().clone();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))
            .expect("TLS 配置")
            .add_service(EventStoreServer::new(es_server::service::EsService::new(
                sm.clone(),
            )))
            .add_service(RaftAdminServer::new(es_raft::RaftAdminService::new(sm)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 默认行为（跳过校验）可连
    let out = esctl(&addr, &["status"]);
    assert!(
        out.status.success(),
        "https 默认跳过校验应成功: {}",
        stderr(&out)
    );
    assert!(stdout(&out).contains("reachable=true"), "{}", stdout(&out));

    // 显式 --insecure-skip-tls-verify
    let out = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args(["--endpoints", &addr, "--insecure-skip-tls-verify", "status"])
        .output()
        .expect("运行 esctl");
    assert!(out.status.success(), "{}", stderr(&out));

    // --cacert 传 CA（自签证书自身即 CA）严格校验
    let ca_path = dir.path().join("ca.pem");
    std::fs::write(&ca_path, cert_pem.as_bytes()).expect("写 CA 文件");
    let out = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args([
            "--endpoints",
            &addr,
            "--cacert",
            ca_path.to_str().unwrap(),
            "status",
        ])
        .output()
        .expect("运行 esctl");
    assert!(out.status.success(), "--cacert 应成功: {}", stderr(&out));

    // --cacert 与 --insecure-skip-tls-verify 互斥：参数错误退出码 2
    let out = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args([
            "--endpoints",
            &addr,
            "--cacert",
            ca_path.to_str().unwrap(),
            "--insecure-skip-tls-verify",
            "status",
        ])
        .output()
        .expect("运行 esctl");
    assert_eq!(out.status.code(), Some(2), "互斥参数应退出码 2");

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn readall_paging_cursor_hint() {
    let (addr, handle, _server, _dir) = start_server().await;

    for i in 0..3 {
        let out = esctl(
            &addr,
            &[
                "append",
                &format!("s/page/{i}"),
                "--event-type",
                "P",
                "--data",
                "x",
            ],
        );
        assert!(out.status.success(), "{}", stderr(&out));
    }

    // max-count=2 取满时输出续读提示
    let out = esctl(&addr, &["readall", "--max-count", "2"]);
    assert!(out.status.success());
    assert!(stderr(&out).contains("下一页"), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("--from-positions"),
        "{}",
        stderr(&out)
    );

    handle.abort();
}

/// 启动进程内双节点测试服务器（单分片）。
///
/// node1 自举为单成员 leader；node2 不初始化（由 member add 加入）。
/// 两个节点都注册 EventStore + RaftRpc + RaftAdmin 三服务——
/// add_learner(blocking) 时 leader 需经 RaftRpc 给 node2 复制日志。
/// 返回 (node1 地址, node2 地址, 服务句柄, 服务器, 临时目录)。
async fn start_two_nodes() -> (
    String,
    String,
    Vec<tokio::task::JoinHandle<()>>,
    Vec<Server>,
    Vec<tempfile::TempDir>,
) {
    let mut handles = Vec::new();
    let mut servers = Vec::new();
    let mut dirs = Vec::new();
    let mut addrs = Vec::new();

    for id in 1..=2u64 {
        let dir = tempfile::tempdir().expect("临时目录");
        let config = Config {
            node: NodeConfig {
                id,
                listen_addr: "127.0.0.1:0".to_string(),
                peers: vec![],
            },
            storage: StorageConfig {
                data_dir: dir.path().to_path_buf(),
            },
            shards: ShardConfig { num_shards: 1 },
            tls: None,
        };
        let server = Server::new(config).expect("创建服务器");
        server.init().await.expect("初始化");

        // 只有 node1 自举（单成员）
        if id == 1 {
            let shard = server.shard_manager().get_shard(0).await.expect("取分片");
            shard
                .raft
                .initialize(std::collections::BTreeSet::from([1u64]))
                .await
                .expect("初始化 raft");
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定端口");
        let addr = format!("http://{}", listener.local_addr().expect("取地址"));
        let sm = server.shard_manager().clone();
        let handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(EventStoreServer::new(es_server::service::EsService::new(
                    sm.clone(),
                )))
                .add_service(RaftRpcServer::new(es_raft::RaftRpcService::new(sm.clone())))
                .add_service(RaftAdminServer::new(es_raft::RaftAdminService::new(sm)))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await;
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        handles.push(handle);
        servers.push(server);
        dirs.push(dir);
        addrs.push(addr);
    }
    (addrs[0].clone(), addrs[1].clone(), handles, servers, dirs)
}

#[tokio::test(flavor = "multi_thread")]
async fn member_add_remove_two_node_inprocess() {
    let (addr1, addr2, handles, _servers, _dirs) = start_two_nodes().await;
    let member2 = format!("2@{addr2}");

    // add node2：find_leader → add_learner(blocking) → change_membership 完整路径
    let out = esctl(
        &addr1,
        &["member", "add", "--shard", "0", "--member", &member2],
    );
    assert!(out.status.success(), "member add 失败: {}", stderr(&out));
    assert!(stdout(&out).contains("已提升"), "{}", stdout(&out));

    // member list：两节点、一个 Leader（list 只聚合 --endpoints 指定的端点）
    let both = format!("{addr1},{addr2}");
    let out = esctl(&both, &["member", "list"]);
    let text = stdout(&out);
    assert!(text.contains("voters=[1,2]"), "{text}");
    assert!(text.contains("(Leader)"), "{text}");
    assert!(text.contains("(Follower)"), "{text}");

    // 写入走 node2 端点：若 node2 非 leader，经 Unavailable 提示重定向到 node1。
    // node2 的 leader 信息来自心跳/日志复制，轮询等待就绪（最多 5s）。
    let mut ready = false;
    for _ in 0..50 {
        let out = esctl(&addr2, &["-w", "json", "member", "list"]);
        if out.status.success()
            && stdout(&out).contains("\"current_leader\":")
            && !stdout(&out).contains("\"has_leader\":false")
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "node2 未获得 leader 信息");

    // 写入带两个端点：node2 非 leader 时返回 Unavailable（其 leader_addr 提示可能
    // 为空——openraft 不总填充 leader_node），esctl 应轮换到 node1 成功。
    let out = esctl(
        &both,
        &["append", "s/two", "--event-type", "T", "--data", "x"],
    );
    assert!(out.status.success(), "经双端点写入失败: {}", stderr(&out));

    // remove node2：完整移除路径（change_membership）
    let out = esctl(
        &addr1,
        &["member", "remove", "--shard", "0", "--node-id", "2"],
    );
    assert!(out.status.success(), "member remove 失败: {}", stderr(&out));
    assert!(
        stdout(&out).contains("已从投票成员中移除"),
        "{}",
        stdout(&out)
    );

    let out = esctl(&addr1, &["member", "list"]);
    assert!(stdout(&out).contains("voters=[1]"), "{}", stdout(&out));

    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn member_learner_and_validation() {
    let (addr, handle, _server, _dir) = start_server().await;

    // learner-only 添加不存在的节点 2（--no-blocking 避免追平等待挂起）
    let out = esctl(
        &addr,
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
    assert!(
        out.status.success(),
        "learner-only add 失败: {}",
        stderr(&out)
    );
    assert!(stdout(&out).contains("learner"), "{}", stdout(&out));

    // remove 不在 voters 的节点 → 校验失败，退出码 1
    let out = esctl(
        &addr,
        &["member", "remove", "--shard", "0", "--node-id", "2"],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("不在其中"), "{}", stderr(&out));

    // init --all-shards：已初始化的分片逐个告警，退出码 1
    let out = esctl(
        &addr,
        &["init", "--all-shards", "--member", "1@127.0.0.1:50051"],
    );
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("已初始化"), "{}", stderr(&out));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn meta_missing_stream_and_status_formats() {
    let (addr, handle, _server, _dir) = start_server().await;

    // meta 不存在的流：exists: false
    let out = esctl(&addr, &["meta", "no-such-stream"]);
    assert!(out.status.success());
    assert!(stdout(&out).contains("exists: false"), "{}", stdout(&out));

    // status 的 table / json 格式
    let out = esctl(&addr, &["-w", "table", "status"]);
    let text = stdout(&out);
    assert!(text.contains("ENDPOINT"), "{text}");
    assert!(text.contains("REACHABLE"), "{text}");

    let out = esctl(&addr, &["-w", "json", "status"]);
    let json: serde_json::Value = serde_json::from_str(&stdout(&out)).expect("status -w json");
    assert_eq!(json["endpoints"][0]["reachable"], true);

    // member list 的 table 格式
    let out = esctl(&addr, &["-w", "table", "member", "list"]);
    assert!(stdout(&out).contains("SHARD"), "{}", stdout(&out));

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn read_backward_and_readall_cursor() {
    let (addr, handle, _server, _dir) = start_server().await;

    esctl(
        &addr,
        &["append", "s/rev", "--event-type", "A", "--data", "1"],
    );
    esctl(
        &addr,
        &["append", "s/rev", "--event-type", "B", "--data", "2"],
    );

    // 反向读：新事件在前
    let out = esctl(&addr, &["read", "s/rev", "--backward"]);
    let text = stdout(&out);
    let pos_b = text.find("[B]").expect("应有 B");
    let pos_a = text.find("[A]").expect("应有 A");
    assert!(pos_b < pos_a, "反向读 B 应在 A 前: {text}");

    // readall 显式游标 from-positions
    let out = esctl(&addr, &["readall", "--from-positions", "0:0,1:0"]);
    assert!(
        out.status.success(),
        "from-positions 失败: {}",
        stderr(&out)
    );

    // readall json 翻页：max-count 取满输出 next_from_positions
    let out = esctl(&addr, &["-w", "json", "readall", "--max-count", "1"]);
    let json: serde_json::Value = serde_json::from_str(&stdout(&out)).expect("json");
    assert!(json["next_from_positions"].as_array().is_some(), "{}", json);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn watch_all_subscribe() {
    let (addr, handle, _server, _dir) = start_server().await;

    esctl(
        &addr,
        &["append", "s/all", "--event-type", "T", "--data", "x"],
    );

    // $all 订阅（默认分片 0）：追平后退出
    let out = esctl(&addr, &["watch", "--all", "--once", "--from-start"]);
    assert!(out.status.success(), "watch --all 失败: {}", stderr(&out));
    assert!(stdout(&out).contains("已追平"), "{}", stdout(&out));

    handle.abort();
}

/// 发现 1：readall 翻页续读游标必须覆盖全部分片（偏斜数据场景），
/// 多页续读全量到达且 (shard, position) 无重复
#[tokio::test(flavor = "multi_thread")]
async fn readall_skewed_paging_no_duplicates() {
    let (addr, handle, _server, _dir) = start_server().await;

    // 找两个路由到不同分片的流名（2 分片集群）
    let s0 = (0..100u64)
        .map(|i| format!("bulk/0/{i}"))
        .find(|n| es_core::route(n, 2) == 0)
        .expect("应有路由到分片 0 的流名");
    let s1 = (0..100u64)
        .map(|i| format!("bulk/1/{i}"))
        .find(|n| es_core::route(n, 2) == 1)
        .expect("应有路由到分片 1 的流名");
    for _ in 0..6 {
        let out = esctl(&addr, &["append", &s0, "--event-type", "T", "--data", "x"]);
        assert!(out.status.success(), "{}", stderr(&out));
    }
    for _ in 0..2 {
        let out = esctl(&addr, &["append", &s1, "--event-type", "T", "--data", "x"]);
        assert!(out.status.success(), "{}", stderr(&out));
    }

    // 自动翻页到读完：每页用上一页返回的 next_from_positions 续读。
    // 结束条件 = 页不满（各分片数据已尽）或续读游标为空（倒序到边界）。
    // 注意：服务端对空分片返回「游标不动」（next=from），游标列表可能始终非空，
    // 必须靠页不满终止，否则空分片会让翻页永不停止。
    let mut all: Vec<serde_json::Value> = Vec::new();
    let mut first_next: Option<Vec<(u64, u64)>> = None;
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    for _ in 0..10 {
        let out = match &cursor {
            None => esctl(&addr, &["-w", "json", "readall", "--max-count", "3"]),
            Some(c) => esctl(
                &addr,
                &["-w", "json", "readall", "--max-count", "3", "--from-positions", c],
            ),
        };
        assert!(out.status.success(), "{}", stderr(&out));
        let page: serde_json::Value = serde_json::from_str(&stdout(&out)).expect("json");
        let count = page["events"].as_array().expect("events").len();
        let next = page["next_from_positions"].as_array().map(|arr| {
            arr.iter()
                .map(|v| {
                    let s = v.as_str().expect("游标文本");
                    let (a, b) = s.split_once(':').expect("shard:pos");
                    (a.parse().expect("shard"), b.parse().expect("pos"))
                })
                .collect::<Vec<_>>()
        });
        if first_next.is_none() {
            first_next = next.clone();
        }
        all.extend(page["events"].as_array().expect("events").clone());
        pages += 1;
        // 页不满 = 没有更多数据；页满但游标空 = 倒序到边界
        if count < 3 || next.is_none() {
            break;
        }
        cursor = next.map(|v| {
            v.iter()
                .map(|(s, p)| format!("{s}:{p}"))
                .collect::<Vec<_>>()
                .join(",")
        });
    }
    assert!(pages >= 3, "偏斜数据应翻页至少 3 次，实际 {pages} 次");

    // 核心修复点：第一页的续读游标必须覆盖两个分片（旧实现只含本页有事件的分片，
    // 未产出事件的分片在续读中永久消失）
    let first = first_next.expect("第一页应有续读游标");
    assert!(
        first.iter().any(|(s, _)| *s == 0) && first.iter().any(|(s, _)| *s == 1),
        "续读游标必须覆盖两个分片: {first:?}"
    );

    // 全量到达且无重复
    let mut seen: std::collections::HashSet<(u64, u64)> = std::collections::HashSet::new();
    for ev in &all {
        let key = (
            ev["shard_id"].as_u64().expect("shard_id"),
            ev["position"].as_u64().expect("position"),
        );
        assert!(seen.insert(key), "翻页出现重复事件: {key:?}");
    }
    assert_eq!(seen.len(), 8, "应读到全部 8 条事件，实际 {} 条", seen.len());
    handle.abort();
}

/// 发现 6：readall --backward 默认 from-position=0 时必须以 u64::MAX 起读
/// （旧缺陷：默认反向读只返回每分片 position=0 的最旧事件/空）
#[tokio::test(flavor = "multi_thread")]
async fn readall_backward_defaults_to_latest() {
    let (addr, handle, _server, _dir) = start_server().await;

    for i in 0..3 {
        let out = esctl(
            &addr,
            &[
                "append",
                "s/revall",
                "--event-type",
                "T",
                "--data",
                &i.to_string(),
            ],
        );
        assert!(out.status.success(), "{}", stderr(&out));
    }

    let out = esctl(&addr, &["readall", "--backward"]);
    assert!(out.status.success(), "{}", stderr(&out));
    let text = stdout(&out);
    assert_eq!(text.matches("[T]").count(), 3, "应读到全部 3 条: {text}");
    handle.abort();
}

/// 发现 14：watch --all --shard N 订阅指定分片的 $all（旧缺陷：参数被静默忽略，
/// 服务端硬编码分片 0）
#[tokio::test(flavor = "multi_thread")]
async fn watch_all_per_shard() {
    let (addr, handle, _server, _dir) = start_server().await;

    let s1 = (0..100u64)
        .map(|i| format!("watch/shard1/{i}"))
        .find(|n| es_core::route(n, 2) == 1)
        .expect("应有路由到分片 1 的流名");
    let out = esctl(&addr, &["append", &s1, "--event-type", "T", "--data", "x"]);
    assert!(out.status.success(), "{}", stderr(&out));

    // 指定 --shard 1：收到分片 1 的事件（json 模式，simple 行不含流名）
    let out = esctl(
        &addr,
        &["-w", "json", "watch", "--all", "--shard", "1", "--once", "--from-start"],
    );
    assert!(
        out.status.success(),
        "watch --shard 1 失败: {}",
        stderr(&out)
    );
    assert!(
        stdout(&out).contains(&s1),
        "应收到分片 1 的事件: {}",
        stdout(&out)
    );

    // 默认 shard 0：收不到分片 1 的事件
    let out = esctl(
        &addr,
        &["-w", "json", "watch", "--all", "--once", "--from-start"],
    );
    assert!(out.status.success(), "watch 默认失败: {}", stderr(&out));
    assert!(
        !stdout(&out).contains(&s1),
        "默认订阅 shard 0 不应收到分片 1 事件"
    );
    handle.abort();
}

/// 发现 9：member list 全部端点不可达必须报错（旧缺陷：输出"未初始化"退出码 0，
/// 网络故障伪装成"需要 init"）
#[tokio::test(flavor = "multi_thread")]
async fn member_list_all_down_exit_1() {
    let out = esctl("http://127.0.0.1:59999", &["member", "list"]);
    assert_eq!(out.status.code(), Some(1), "应退出码 1: {}", stderr(&out));
    assert!(stderr(&out).contains("不可达"), "{}", stderr(&out));
}

/// 发现 11：init --all-shards 部分初始化集群上必须补完其余分片
/// （旧缺陷：在第一个已初始化分片处中止，其余分片永远无 leader）
#[tokio::test(flavor = "multi_thread")]
async fn init_all_shards_partial_completes_rest() {
    let (addr, handle, _server, _dir) = start_server_uninitialized(2).await;

    // 先只初始化分片 0
    let out = esctl(
        &addr,
        &["init", "--shard", "0", "--member", "1@127.0.0.1:50051"],
    );
    assert!(out.status.success(), "init 分片 0 失败: {}", stderr(&out));

    // --all-shards：分片 0 已初始化告警，但分片 1 必须被补完
    let out = esctl(
        &addr,
        &["init", "--all-shards", "--member", "1@127.0.0.1:50051"],
    );
    assert_eq!(out.status.code(), Some(1), "有分片失败应退出码 1");
    assert!(stderr(&out).contains("已初始化"), "{}", stderr(&out));

    // 分片 1 应已被初始化并选出 leader
    tokio::time::sleep(Duration::from_millis(800)).await;
    let out = esctl(&addr, &["-w", "json", "status"]);
    let json: serde_json::Value = serde_json::from_str(&stdout(&out)).expect("status json");
    let leader_of = json["endpoints"][0]["leader_of"]
        .as_array()
        .expect("leader_of");
    assert!(
        leader_of.iter().any(|v| v.as_u64() == Some(1)),
        "分片 1 应已被补完并成为 leader: {json}"
    );
    handle.abort();
}

/// 发现 7/10：--timeout 0 = 不设超时；--shards 0 是参数错误（退出码 2，
/// 旧缺陷：--shards 0 直接除零 panic 退出 101）
#[tokio::test(flavor = "multi_thread")]
async fn timeout_zero_shards_zero_semantics() {
    let (addr, handle, _server, _dir) = start_server().await;

    // --timeout 0 = 不设超时，RPC 应正常成功（旧缺陷：Duration::ZERO 使所有 RPC 立即失败）
    let out = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args(["--endpoints", &addr, "--timeout", "0", "status"])
        .output()
        .expect("运行 esctl");
    assert!(
        out.status.success(),
        "--timeout 0 应成功: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // --shards 0：clap 参数错误退出码 2
    let out = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args(["--endpoints", &addr, "--shards", "0", "status"])
        .output()
        .expect("运行 esctl");
    assert_eq!(out.status.code(), Some(2), "应退出码 2");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("≥ 1"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    handle.abort();
}

/// 发现 15：change_membership CAS——陈旧 voters 快照必须拒绝（FailedPrecondition），
/// 正确快照成功（旧缺陷：无校验，并发变更被后到者静默覆盖）
#[tokio::test(flavor = "multi_thread")]
async fn member_cas_stale_snapshot_rejected() {
    use es_proto::eventstore::raft_admin_client::RaftAdminClient;
    use es_proto::eventstore::*;
    let (addr, handle, _server, _dir) = start_server().await;

    let mut client = RaftAdminClient::connect(addr.clone()).await.expect("连接");

    // 正确快照（当前 voters=[1]）：成功（幂等提交，不改变状态）
    let ok = client
        .change_membership(ChangeMembershipRequest {
            shard_id: 0,
            voter_ids: vec![1],
            expected_voters: vec![1],
            retain: false,
        })
        .await;
    assert!(ok.is_ok(), "正确快照应成功: {ok:?}");

    // 陈旧快照：FailedPrecondition
    let err = client
        .change_membership(ChangeMembershipRequest {
            shard_id: 0,
            voter_ids: vec![1],
            expected_voters: vec![2],
            retain: false,
        })
        .await
        .expect_err("陈旧快照应被拒绝");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("成员集合已变更"), "{}", err.message());

    handle.abort();
}
