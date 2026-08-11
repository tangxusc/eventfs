//! 进程内自动组建集群测试：bootstrap 走真实 gRPC 路径（探测/选举/复制与
//! 多进程测试一致），但全部在测试进程内运行——默认套件即可执行，且可被
//! 覆盖率统计（多进程测试的节点进程被 SIGKILL 强杀，LLVM profile 无法落盘）。
//!
//! 多进程版自动组建测试见 multi_node_test.rs（`三节点配置peers自动组建并复制数据` 等）。

use std::sync::Arc;
use std::time::Duration;

use es_proto::eventstore::raft_admin_client::RaftAdminClient;
use es_proto::eventstore::GetRaftStateRequest;
use es_proto::tls::{apply_endpoint_tls, TlsClientConfig};
use es_server::config::{Config, NodeConfig, PeerConfig, ShardConfig, StorageConfig, TlsConfig};
use es_server::Server;

/// 测试固定用分片 0
const SHARD: u64 = 0;

/// TLS 测试材料：本节点证书/私钥 + 可选的全节点证书拼接（严格校验用 ca）
#[derive(Clone)]
struct TestTls {
    cert: String,
    key: String,
    /// Some(全部节点证书拼接)：配置 ca_file 严格校验；None：跳过校验
    ca: Option<String>,
}

/// 生成 n 张 127.0.0.1 的自签证书（IP SAN），每张独立密钥对
fn gen_tls(n: usize) -> Vec<TestTls> {
    let mut all = String::new();
    let mut per_node = Vec::new();
    for _ in 0..n {
        let certified =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()])
                .expect("生成自签证书");
        let cert = certified.cert.pem();
        let key = certified.key_pair.serialize_pem();
        all.push_str(&cert);
        per_node.push((cert, key));
    }
    per_node
        .into_iter()
        .map(|(cert, key)| TestTls {
            cert,
            key,
            ca: Some(all.clone()),
        })
        .collect()
}

/// 进程内节点：完整 Server（三个 gRPC 服务共端口）+ 配置 peers（自动组建）
struct TestNode {
    server: Arc<Server>,
    addr: String,
    handle: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

impl Drop for TestNode {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// 启动一个进程内节点。
///
/// `Server::init` 会 spawn 自动组建任务；gRPC 服务随后在独立任务中监听。
/// bootstrap 任务的第一步是 TCP 轮询等所有 peers（含自己）端口就绪，
/// 因此先 init 后起 serve 的时序自洽（与真实进程一致）。
///
/// `tls` 提供时：证书落盘到数据目录、config 配置 `[tls]`、以 https 监听。
async fn start_node(
    id: u64,
    port: u16,
    peers: &[(u64, String)],
    num_shards: u64,
    tls: Option<&TestTls>,
) -> TestNode {
    let dir = tempfile::tempdir().expect("临时目录");
    let tls_config = match tls {
        Some(t) => {
            let cert_path = dir.path().join("server.crt");
            let key_path = dir.path().join("server.key");
            std::fs::write(&cert_path, &t.cert).expect("写证书");
            std::fs::write(&key_path, &t.key).expect("写私钥");
            // ca 可选：None 时跳过校验（节点间仍以 https 监听，只是不校验对端）
            let ca_file = match &t.ca {
                Some(ca) => {
                    let ca_path = dir.path().join("ca.crt");
                    std::fs::write(&ca_path, ca).expect("写 CA");
                    Some(ca_path)
                }
                None => None,
            };
            Some(TlsConfig {
                cert_file: Some(cert_path),
                key_file: Some(key_path),
                ca_file,
            })
        }
        None => None,
    };
    let config = Config {
        node: NodeConfig {
            id,
            listen_addr: format!("127.0.0.1:{}", port),
            peers: peers
                .iter()
                .map(|(pid, addr)| PeerConfig {
                    id: *pid,
                    addr: addr.clone(),
                })
                .collect(),
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
        },
        shards: ShardConfig { num_shards },
        tls: tls_config,
    };

    let server = Arc::new(Server::new(config).expect("创建服务器"));
    server.init().await.expect("初始化");

    let s = server.clone();
    let handle = tokio::spawn(async move {
        let _ = s.serve().await;
    });

    TestNode {
        server,
        // tonic 客户端需要合法 URI（带 scheme）
        addr: format!(
            "{}://127.0.0.1:{}",
            if tls.is_some() { "https" } else { "http" },
            port
        ),
        handle,
        _dir: dir,
    }
}

/// 构建 TLS 感知的管理客户端（https 目标按信任策略装配）
async fn admin_client(
    addr: &str,
    tls: Option<&TlsClientConfig>,
) -> Result<RaftAdminClient<tonic::transport::Channel>, tonic::transport::Error> {
    let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())?;
    let endpoint = apply_endpoint_tls(endpoint, tls)?;
    Ok(RaftAdminClient::new(endpoint.connect_lazy()))
}

/// 轮询直到集群自动组建完成：全部节点 voter_ids == want_voters 且存在 leader。
/// 返回 leader 的 node_id。
async fn wait_cluster_formed(
    addrs: &[String],
    want_voters: usize,
    timeout: Duration,
    tls: Option<&TlsClientConfig>,
) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let mut formed = true;
        let mut leader = None;
        for a in addrs {
            // 节点可能仍在启动（gRPC 未就绪），失败即视为未形成，下轮再查
            let mut admin = match admin_client(a, tls).await {
                Ok(c) => c,
                Err(_) => {
                    formed = false;
                    break;
                }
            };
            let s = match admin
                .get_raft_state(GetRaftStateRequest { shard_id: SHARD })
                .await
            {
                Ok(resp) => resp.into_inner(),
                Err(_) => {
                    formed = false;
                    break;
                }
            };
            if s.voter_ids.len() != want_voters {
                formed = false;
                break;
            }
            if s.is_leader {
                leader = Some(s.node_id);
            }
        }
        if formed {
            if let Some(l) = leader {
                return l;
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("等待集群自动组建超时（{timeout:?}），want_voters={want_voters}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 分配一个临时端口（drop 后 50ms 复用窗口，与 multi_node_test 同模式）
fn alloc_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("绑定临时端口");
    let port = listener.local_addr().expect("取地址").port();
    drop(listener);
    std::thread::sleep(std::time::Duration::from_millis(50));
    port
}

/// 初始化 tracing（try_init 容忍并行测试重复调用；RUST_LOG 可调级别）
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "es_server=info,es_raft=info".into()),
        )
        .with_test_writer()
        .try_init();
}

#[tokio::test]
async fn 进程内三节点自动组建() {
    init_tracing();
    eprintln!("\n=== 进程内 3 节点（配置完整 peers，自动组建）===");
    let ports: Vec<u16> = (0..3).map(|_| alloc_port()).collect();
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{}", p)).collect();
    let peers: Vec<(u64, String)> = vec![
        (1, addrs[0].clone()),
        (2, addrs[1].clone()),
        (3, addrs[2].clone()),
    ];

    // 依次启动（模拟同时上线：bootstrap 任务会等全部端口就绪再探测）
    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        nodes.push(start_node(id, ports[(id - 1) as usize], &peers, 1, None).await);
    }

    eprintln!("\n=== 等待自动组建完成 ===");
    let addrs: Vec<String> = nodes.iter().map(|n| n.addr.clone()).collect();
    let _ = wait_cluster_formed(&addrs, 3, Duration::from_secs(60), None).await;

    // 校验：3 个投票成员、leader 唯一且一致
    let mut leaders = Vec::new();
    for node in &nodes {
        let shard = node
            .server
            .shard_manager()
            .get_shard(SHARD)
            .await
            .expect("取分片");
        let m = shard.raft.metrics().borrow().clone();
        assert!(
            shard.raft.is_initialized().await.expect("查初始化"),
            "节点 {} 应已初始化",
            m.id
        );
        assert_eq!(
            m.membership_config.membership().voter_ids().count(),
            3,
            "节点 {} 投票成员应为 3",
            m.id
        );
        if m.state.is_leader() {
            leaders.push(m.id);
        }
    }
    assert_eq!(leaders.len(), 1, "只能有一个 leader: {leaders:?}");
    eprintln!("✓ 进程内自动组建完成，leader 唯一，3 个投票成员");
}

#[tokio::test]
async fn 进程内乱序启动自动组建() {
    eprintln!("\n=== 进程内乱序启动：先起 node2，再起 node1、node3 ===");
    let ports: Vec<u16> = (0..3).map(|_| alloc_port()).collect();
    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{}", p)).collect();
    let peers: Vec<(u64, String)> = vec![
        (1, addrs[0].clone()),
        (2, addrs[1].clone()),
        (3, addrs[2].clone()),
    ];

    // node2 先起：探测不到其它节点，自行用完整成员 initialize（无 quorum 条目未提交）；
    // node1/node3 后起探测到 node2 已初始化 → 跳过自举 → 投票使其提交
    let mut nodes = Vec::new();
    for id in [2u64, 1, 3] {
        nodes.push(start_node(id, ports[(id - 1) as usize], &peers, 1, None).await);
    }

    eprintln!("\n=== 等待自动组建完成 ===");
    let addrs: Vec<String> = nodes.iter().map(|n| n.addr.clone()).collect();
    let _ = wait_cluster_formed(&addrs, 3, Duration::from_secs(90), None).await;

    // 全部节点成为 3 投票成员
    for node in &nodes {
        let shard = node
            .server
            .shard_manager()
            .get_shard(SHARD)
            .await
            .expect("取分片");
        let m = shard.raft.metrics().borrow().clone();
        assert_eq!(
            m.membership_config.membership().voter_ids().count(),
            3,
            "节点 {} 投票成员应为 3",
            m.id
        );
    }
    eprintln!("✓ 进程内乱序启动收敛，全部节点 3 个投票成员");
}

// ============ https（自签证书）============

/// 启动三节点全 https 集群并等待自动组建。
/// `strict`：true 每节点配置 ca_file（全部证书拼接）严格校验；false 不配 ca_file（跳过校验）。
/// 返回 (nodes, leader_id, ca_all)——ca_all 供严格模式下的 SDK 客户端复用同一批证书。
async fn start_https_cluster(strict: bool) -> (Vec<TestNode>, u64, String) {
    let certs = gen_tls(3);
    let ports: Vec<u16> = (0..3).map(|_| alloc_port()).collect();
    let addrs: Vec<String> = ports
        .iter()
        .map(|p| format!("https://127.0.0.1:{}", p))
        .collect();
    let peers: Vec<(u64, String)> = vec![
        (1, addrs[0].clone()),
        (2, addrs[1].clone()),
        (3, addrs[2].clone()),
    ];

    let mut nodes = Vec::new();
    for id in 1..=3u64 {
        // 每节点都有自己的 cert/key（https 监听）；strict 决定是否配 ca_file
        let mut t = certs[(id - 1) as usize].clone();
        if !strict {
            t.ca = None;
        }
        nodes.push(start_node(id, ports[(id - 1) as usize], &peers, 1, Some(&t)).await);
    }

    // 轮询信任策略与集群一致：严格模式用 CA，否则默认跳过校验
    let trust = if strict {
        Some(TlsClientConfig::Ca(certs[0].ca.clone().unwrap().into_bytes()))
    } else {
        None
    };
    let addrs: Vec<String> = nodes.iter().map(|n| n.addr.clone()).collect();
    let leader = wait_cluster_formed(&addrs, 3, Duration::from_secs(60), trust.as_ref()).await;
    (nodes, leader, certs[0].ca.clone().unwrap())
}

#[tokio::test]
async fn 进程内三节点全https严格校验自动组建() {
    init_tracing();
    eprintln!("\n=== 进程内 3 节点全 https（ca_file 严格校验）自动组建 ===");
    let (nodes, leader, ca_all) = start_https_cluster(true).await;
    eprintln!("✓ 全 https 严格校验自动组建完成，leader = node{leader}");

    // 校验 3 投票成员 + leader 唯一
    let mut leaders = Vec::new();
    for node in &nodes {
        let shard = node
            .server
            .shard_manager()
            .get_shard(SHARD)
            .await
            .expect("取分片");
        let m = shard.raft.metrics().borrow().clone();
        assert_eq!(
            m.membership_config.membership().voter_ids().count(),
            3,
            "节点 {} 投票成员应为 3",
            m.id
        );
        if m.state.is_leader() {
            leaders.push(m.id);
        }
    }
    assert_eq!(leaders.len(), 1, "只能有一个 leader: {leaders:?}");

    // 用 es-client SDK 经 https 写入并读回（覆盖客户端 API 的 TLS 链路）
    eprintln!("\n=== es-client 经 https 写读 ===");
    let leader_addr = nodes[(leader - 1) as usize].addr.clone();
    let mut client = es_client::EventStoreClient::connect_with_tls(
        vec![leader_addr],
        Some(es_client::TlsClientConfig::Ca(ca_all.into_bytes())),
    )
    .await
    .expect("SDK 连接 https 节点");

    use es_proto::eventstore::*;
    client
        .append(
            "tls-strict".to_string(),
            ExpectedVersion {
                kind: Some(expected_version::Kind::NoStream(Empty {})),
            },
            vec![NewEvent {
                event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                event_type: "E".to_string(),
                data: b"via-tls".to_vec(),
                metadata: vec![],
            }],
        )
        .await
        .expect("https 写入");
    let events = client
        .read_stream("tls-strict".to_string(), 0, 0, Direction::Forward)
        .await
        .expect("https 读取");
    assert_eq!(events[0].data, b"via-tls");
    eprintln!("✓ es-client 经 https 写读成功");
}

#[tokio::test]
async fn 进程内三节点全https默认跳过校验自动组建() {
    init_tracing();
    eprintln!("\n=== 进程内 3 节点全 https（无 ca_file，默认跳过校验）自动组建 ===");
    let (nodes, leader, _) = start_https_cluster(false).await;
    eprintln!("✓ 全 https 跳过校验自动组建完成，leader = node{leader}");

    // 校验 3 投票成员
    for node in &nodes {
        let shard = node
            .server
            .shard_manager()
            .get_shard(SHARD)
            .await
            .expect("取分片");
        let m = shard.raft.metrics().borrow().clone();
        assert_eq!(
            m.membership_config.membership().voter_ids().count(),
            3,
            "节点 {} 投票成员应为 3",
            m.id
        );
    }
}
