//! 多节点集成测试：验证 Raft 共识与网络层

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::time::Duration;
#[cfg(unix)]
use std::time::Instant;

use es_proto::eventstore::event_store_client::EventStoreClient;
use es_proto::eventstore::migration_client::MigrationClient;
use es_proto::eventstore::raft_admin_client::RaftAdminClient;
use es_proto::eventstore::*;

/// 测试固定用分片 0（集群按 num_shards=1 启动）
const SHARD: u64 = 0;

/// 测试集群：3 个节点
struct TestCluster {
    /// 节点进程
    nodes: HashMap<u64, NodeHandle>,
    /// 分片总数
    num_shards: u64,
    /// 临时目录（drop 时自动删除）。重启节点要复用同一目录才能验证数据恢复，
    /// 因此必须持有到测试结束。
    _dirs: Vec<tempfile::TempDir>,
}

struct NodeHandle {
    /// 对外地址，形如 http://127.0.0.1:50051
    addr: String,
    /// 监听端口，重启后仍用同一个（membership 里记的是这个地址）
    port: u16,
    /// 配置文件路径，重启时复用
    config_path: std::path::PathBuf,
    process: Child,
}

/// 启动一个 eventstored 子进程
///
/// 直接运行已编译的二进制（cargo 在测试编译时注入路径），而不是 `cargo run`：
/// 后者是 cargo → 二进制的两层进程，测试 kill 掉 cargo 会留下孤儿二进制进程。
/// 覆盖率运行可经 EVENTSTORED_BIN 注入带 instrumentation 的二进制；默认仍使用
/// cargo 注入的路径，确保普通测试不依赖额外环境变量。
/// 日志级别默认 warn，可用 RUST_LOG 环境变量覆盖（自动组建等测试排障用）。
fn spawn_node(config_path: &std::path::Path) -> Child {
    let binary = std::env::var("EVENTSTORED_BIN")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_eventstored").to_string());
    Command::new(binary)
        .args(["--config", config_path.to_str().expect("配置路径非 UTF-8")])
        .env(
            "RUST_LOG",
            std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".to_string()),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("启动节点进程")
}

/// 优雅回收节点进程，保证覆盖率运行时子进程能写出 profile。
fn terminate_node_process(process: &mut Child) {
    #[cfg(unix)]
    {
        // SIGTERM 会触发服务端 flush WAL 与 LLVM profile；超时后才强制回收。
        let _ = unsafe { libc::kill(process.id() as libc::pid_t, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match process.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                Err(_) => break,
            }
        }
    }

    let _ = process.kill();
    let _ = process.wait();
}

/// 检测端口是否可连接（进程是否真正启动）
async fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port)).await {
            Ok(_) => return true,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

impl TestCluster {
    /// 启动 3 节点单分片集群，配置不含 peers（手动组建路径）
    async fn start() -> Self {
        Self::start_with_shards(1, false).await
    }

    /// 启动 3 节点集群，指定分片数。
    ///
    /// `write_peers=true` 时每个节点写入**完整 peers（含自己）**——与
    /// config.example.toml 的 etcd 语义一致，节点启动后自动组建集群；
    /// `false` 时不写 peers 字段，保持手动组建路径。
    async fn start_with_shards(num_shards: u64, write_peers: bool) -> Self {
        Self::start_n(3, num_shards, &[1, 2, 3], write_peers).await
    }

    /// 启动 3 节点单分片集群并自动组建（完整 peers，含自己）。
    ///
    /// `order` 指定节点启动顺序（如 `&[2, 1, 3]` 验证乱序启动）。
    async fn start_auto(order: &[u64]) -> Self {
        Self::start_n(order.len() as u64, 1, order, true).await
    }

    /// 启动单节点集群：peers 只含自己，启动后自动单成员自举
    async fn start_single() -> Self {
        Self::start_n(1, 1, &[1], true).await
    }

    /// 启动 n 个节点（id = 1..=n），按 `order` 指定的顺序启动。
    async fn start_n(n: u64, num_shards: u64, order: &[u64], write_peers: bool) -> Self {
        let mut nodes = HashMap::new();
        let mut dirs = Vec::new();

        // 为节点分配端口（节点 id 与端口一一对应，id i 用 ports[i-1]）
        let ports: Vec<u16> = (0..n)
            .map(|_| {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("绑定临时端口");
                let port = listener.local_addr().expect("取地址").port();
                drop(listener);
                // 等待端口真正释放
                std::thread::sleep(std::time::Duration::from_millis(50));
                port
            })
            .collect();
        let internal_ports: Vec<u16> = (0..n)
            .map(|_| {
                let listener =
                    std::net::TcpListener::bind("127.0.0.1:0").expect("绑定内部临时端口");
                let port = listener.local_addr().expect("取内部地址").port();
                drop(listener);
                port
            })
            .collect();

        // 完整成员列表（含自己）——启动时自动组建的配置依据
        let all_peers: Vec<serde_json::Value> = ports
            .iter()
            .enumerate()
            .map(|(i, p)| {
                serde_json::json!({
                    "id": (i + 1) as u64,
                    "addr": format!("127.0.0.1:{}", p),
                    "internal_addr": format!("127.0.0.1:{}", internal_ports[i]),
                })
            })
            .collect();

        for &node_id in order {
            let port = ports[(node_id - 1) as usize];
            let dir = tempfile::tempdir().expect("创建临时目录");

            // 创建配置文件
            let mut node_json = serde_json::json!({
                "id": node_id,
                "listen_addr": format!("127.0.0.1:{}", port),
                "internal_listen_addr": format!(
                    "127.0.0.1:{}",
                    internal_ports[(node_id - 1) as usize]
                ),
            });
            if write_peers {
                node_json["peers"] = serde_json::Value::Array(all_peers.clone());
            } else {
                node_json["peers"] = serde_json::Value::Array(
                    all_peers
                        .iter()
                        .filter(|peer| peer["id"].as_u64() != Some(node_id))
                        .cloned()
                        .collect(),
                );
            }
            // 放置表：
            // - write_peers=true（自动组建）：全复制语义（原「每节点全量 N 分片、
            //   全部节点都是成员」）——rf=节点数，node1 主承载全部分片，其余节点
            //   副本承载全部分片（primary 分区互斥，每 shard 承载数=节点数=rf）；
            // - write_peers=false（手动组建）：无 peers，validate 要求放置表节点
            //   ∈ peers∪self，只能引用本节点——rf=1、本节点主承载全部分片，
            //   成员关系由 RaftAdmin 手动组建，承载语义与原全量一致。
            let placement_nodes: Vec<serde_json::Value> = if write_peers {
                ports
                    .iter()
                    .enumerate()
                    .map(|(i, _)| {
                        let pid = (i + 1) as u64;
                        serde_json::json!({
                            "id": pid,
                            "primary": if pid == 1 {
                                (0..num_shards).collect::<Vec<u64>>()
                            } else {
                                vec![]
                            },
                            "replica": if pid == 1 {
                                vec![]
                            } else {
                                (0..num_shards).collect::<Vec<u64>>()
                            },
                        })
                    })
                    .collect()
            } else {
                let nodes: Vec<serde_json::Value> = vec![serde_json::json!({
                    "id": node_id,
                    "primary": (0..num_shards).collect::<Vec<u64>>(),
                    "replica": Vec::<u64>::new(),
                })];
                nodes
            };
            let config = serde_json::json!({
                "node": node_json,
                "storage": {
                    "data_dir": dir.path().to_str().unwrap(),
                    "memtable_arena_bytes": 4 * 1024 * 1024,
                },
                "placement": {
                    "replication_factor": if write_peers { n } else { 1 },
                    "nodes": placement_nodes,
                },
            });

            let config_path = dir.path().join("config.json");
            std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap())
                .expect("写配置文件");

            let child = spawn_node(&config_path);

            // 等待该节点端口可连接（最多 10 秒）
            if !wait_for_port(port, Duration::from_secs(10)).await {
                panic!("节点 {} 启动超时（端口 {} 不可达）", node_id, port);
            }

            eprintln!("✓ 节点 {} 已启动（端口 {}）", node_id, port);

            nodes.insert(
                node_id,
                NodeHandle {
                    addr: format!("http://127.0.0.1:{}", port),
                    port,
                    config_path,
                    process: child,
                },
            );
            dirs.push(dir);
        }

        eprintln!("✓ 全部 {} 个节点已启动（{num_shards} 个分片）", order.len());

        Self {
            nodes,
            num_shards,
            _dirs: dirs,
        }
    }

    /// 重启节点：复用原配置与数据目录，验证能否从本地日志恢复
    async fn restart_node(&mut self, node_id: u64) {
        let (config_path, port) = {
            let n = self.nodes.get_mut(&node_id).expect("节点不存在");
            let _ = n.process.kill();
            let _ = n.process.wait();
            (n.config_path.clone(), n.port)
        };
        eprintln!("↻ 已停止 node{node_id}，准备重启");

        // 等端口释放，否则新进程 bind 会失败
        tokio::time::sleep(Duration::from_millis(300)).await;

        let child = spawn_node(&config_path);
        self.nodes.get_mut(&node_id).expect("节点不存在").process = child;

        if !wait_for_port(port, Duration::from_secs(15)).await {
            panic!("node{node_id} 重启后端口 {port} 不可达");
        }
        eprintln!("✓ node{node_id} 已重启");
    }

    /// 获取某节点的客户端
    async fn client(&self, node_id: u64) -> EventStoreClient<tonic::transport::Channel> {
        let handle = self.nodes.get(&node_id).expect("节点不存在");
        EventStoreClient::connect(handle.addr.clone())
            .await
            .expect("连接节点")
    }

    /// 获取某节点的管理客户端
    async fn admin(&self, node_id: u64) -> RaftAdminClient<tonic::transport::Channel> {
        let handle = self.nodes.get(&node_id).expect("节点不存在");
        RaftAdminClient::connect(handle.addr.clone())
            .await
            .expect("连接管理接口")
    }

    /// 节点的对外地址（供 membership 记录，其它节点据此回连）
    fn addr_of(&self, node_id: u64) -> String {
        self.nodes.get(&node_id).expect("节点不存在").addr.clone()
    }

    /// 手动组建夹具未配置 peers，需要显式同步服务端路由表。
    ///
    /// 真实集群由 RouteTableManager 根据 peers 自动广播；此处通过同一 Migration
    /// RPC 保持测试环境的路由元数据与已经复制的 Raft 数据一致。
    async fn sync_route_table_from(&self, source: u64) {
        let mut source_client = MigrationClient::connect(self.addr_of(source))
            .await
            .expect("连接路由表来源节点");
        let table = source_client
            .get_route_table(GetRouteTableRequest {})
            .await
            .expect("读取来源路由表")
            .into_inner()
            .table;
        for node_id in self.nodes.keys().copied() {
            if node_id == source {
                continue;
            }
            let mut target = MigrationClient::connect(self.addr_of(node_id))
                .await
                .expect("连接路由表目标节点");
            target
                .push_route_table(PushRouteTableRequest {
                    table: table.clone(),
                })
                .await
                .expect("同步路由表");
        }
    }

    /// 查询某节点在分片 0 上的 Raft 状态
    async fn raft_state(&self, node_id: u64) -> GetRaftStateResponse {
        self.raft_state_of(node_id, SHARD).await
    }

    /// 查询某节点在指定分片上的 Raft 状态
    async fn raft_state_of(&self, node_id: u64, shard_id: u64) -> GetRaftStateResponse {
        self.admin(node_id)
            .await
            .get_raft_state(GetRaftStateRequest { shard_id })
            .await
            .expect("查 Raft 状态")
            .into_inner()
    }

    /// 轮询直到指定分片出现 leader，返回其 node_id
    async fn wait_leader_of_shard(&self, shard_id: u64, timeout: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for id in 1..=3u64 {
                if let Ok(mut a) = RaftAdminClient::connect(self.addr_of(id)).await {
                    if let Ok(resp) = a.get_raft_state(GetRaftStateRequest { shard_id }).await {
                        let s = resp.into_inner();
                        if s.is_leader {
                            return s.node_id;
                        }
                    }
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("分片 {shard_id} 等待选主超时（{timeout:?}）");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// 组建 3 节点集群。
    ///
    /// 采用 openraft 推荐的自举流程：先在 node1 上以单成员初始化，
    /// 它立刻成为 leader；再把 2、3 加为学习者追平日志；
    /// 最后一次性提升为投票成员。这样全程都有 leader，
    /// 避免三个空节点同时竞选导致的选举活锁。
    async fn form_cluster(&self) {
        // 每个分片是一个独立的 Raft group，必须各自初始化并各自选主。
        // 分片之间不共享 membership，也不共享 leader。
        for shard_id in 0..self.num_shards {
            self.form_shard(shard_id).await;
        }
        eprintln!("✓ 集群组建完成（{} 个分片）", self.num_shards);
    }

    /// 组建单个分片的 Raft group
    async fn form_shard(&self, shard_id: u64) {
        let mut admin1 = self.admin(1).await;

        admin1
            .initialize(InitializeRequest {
                shard_id,
                members: vec![RaftMember {
                    node_id: 1,
                    addr: self.addr_of(1),
                }],
            })
            .await
            .unwrap_or_else(|e| panic!("分片 {shard_id} initialize 失败: {e}"));

        // 等 node1 真正当上 leader 再加成员，否则 add_learner 会被拒
        self.wait_leader_of_shard(shard_id, Duration::from_secs(10))
            .await;

        for id in [2u64, 3] {
            admin1
                .add_learner(AddLearnerRequest {
                    shard_id,
                    member: Some(RaftMember {
                        node_id: id,
                        addr: self.addr_of(id),
                    }),
                    blocking: true,
                })
                .await
                .unwrap_or_else(|e| panic!("分片 {shard_id} add_learner node{id} 失败: {e}"));
        }

        admin1
            .change_membership(ChangeMembershipRequest {
                shard_id,
                voter_ids: vec![1, 2, 3],
                retain: false,
                expected_voters: Vec::new(), // 测试直连：不做 CAS 校验
            })
            .await
            .unwrap_or_else(|e| panic!("分片 {shard_id} change_membership 失败: {e}"));

        eprintln!("  ✓ 分片 {shard_id} 就绪");
    }

    /// 轮询直到出现 leader，返回其 node_id
    async fn wait_for_leader(&self, timeout: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for id in 1..=3u64 {
                // 节点可能还在启动，查询失败就跳过重试
                if let Ok(mut a) = RaftAdminClient::connect(self.addr_of(id)).await {
                    if let Ok(resp) = a
                        .get_raft_state(GetRaftStateRequest { shard_id: SHARD })
                        .await
                    {
                        let s = resp.into_inner();
                        if s.is_leader {
                            return s.node_id;
                        }
                    }
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("等待选主超时（{timeout:?}）");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// 轮询直到某节点的 last_applied 追上目标值
    async fn wait_applied(&self, node_id: u64, want: u64, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let s = self.raft_state(node_id).await;
            if s.has_last_applied && s.last_applied >= want {
                return;
            }
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "节点 {node_id} 等待 last_applied>={want} 超时，当前 {}",
                    s.last_applied
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// 重启全部节点（复用各自配置与数据目录）
    async fn restart_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.restart_node(id).await;
        }
    }

    /// 轮询直到集群自动组建完成：全部节点 voter_ids 数 == want_voters 且存在 leader。
    /// 返回 leader 的 node_id。
    async fn wait_cluster_formed(&self, want_voters: usize, timeout: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let mut formed = true;
            let mut leader = None;
            for id in self.nodes.keys() {
                // 节点可能仍在启动，失败即视为未形成，下轮再查
                let mut a = match RaftAdminClient::connect(self.addr_of(*id)).await {
                    Ok(a) => a,
                    Err(_) => {
                        formed = false;
                        break;
                    }
                };
                let s = match a
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

    /// 杀掉指定节点，模拟进程崩溃
    fn kill_node(&mut self, node_id: u64) {
        let node = self.nodes.get_mut(&node_id).expect("节点不存在");
        let _ = node.process.kill();
        let _ = node.process.wait();
        eprintln!("✗ 已杀掉 node{node_id}");
    }

    /// 轮询直到在指定候选节点中出现 leader
    async fn wait_leader_among(&self, candidates: &[u64], timeout: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for &id in candidates {
                if let Ok(mut a) = RaftAdminClient::connect(self.addr_of(id)).await {
                    if let Ok(resp) = a
                        .get_raft_state(GetRaftStateRequest { shard_id: SHARD })
                        .await
                    {
                        let s = resp.into_inner();
                        if s.is_leader {
                            return s.node_id;
                        }
                    }
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("在 {candidates:?} 中等待新 leader 超时（{timeout:?}）");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// 关闭集群
    fn shutdown(mut self) {
        for (node_id, node) in self.nodes.iter_mut() {
            terminate_node_process(&mut node.process);
            eprintln!("✓ 节点 {} 已关闭", node_id);
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for (_, node) in self.nodes.iter_mut() {
            terminate_node_process(&mut node.process);
        }
    }
}

#[tokio::test]
#[ignore = "需要较长时间编译与启动进程"]
async fn three_node_start_and_accept() {
    eprintln!("\n=== 启动 3 节点集群 ===");
    let cluster = TestCluster::start().await;

    eprintln!("\n=== 测试各节点连通性 ===");
    for node_id in 1..=3 {
        let mut client = cluster.client(node_id).await;

        // 尝试调用 GetStreamMeta（不需要 Raft 初始化就能响应）
        let result = client
            .get_stream_meta(GetStreamMetaRequest {
                stream_id: "test".to_string(),
            })
            .await;

        match result {
            Ok(resp) => {
                eprintln!(
                    "✓ 节点 {} 响应正常: exists={}",
                    node_id,
                    resp.into_inner().exists
                );
            }
            Err(e) => {
                eprintln!("✗ 节点 {} 响应失败: {}", node_id, e);
                panic!("节点 {} 不可达", node_id);
            }
        }
    }

    eprintln!("\n=== 测试通过：3 个节点均可连接 ===");
    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn three_node_elect_and_replicate() {
    eprintln!("\n=== 启动 3 节点集群 ===");
    let cluster = TestCluster::start().await;

    eprintln!("\n=== 组建集群 ===");
    cluster.form_cluster().await;

    eprintln!("\n=== 校验选主结果 ===");
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
    eprintln!("✓ leader 是 node{leader}");

    // 恰好一个 leader，且三个节点都认同它
    let mut leaders = Vec::new();
    for id in 1..=3u64 {
        let s = cluster.raft_state(id).await;
        eprintln!(
            "  node{}: state={} term={} leader={:?} voters={:?}",
            s.node_id,
            s.server_state,
            s.current_term,
            if s.has_leader {
                Some(s.current_leader)
            } else {
                None
            },
            s.voter_ids
        );
        if s.is_leader {
            leaders.push(s.node_id);
        }
        assert!(s.has_leader, "node{id} 应已知 leader");
        assert_eq!(s.current_leader, leader, "node{id} 认同的 leader 应一致");
        assert_eq!(
            s.voter_ids.len(),
            3,
            "node{id} 的投票成员应为 3 个，实际 {:?}",
            s.voter_ids
        );
    }
    assert_eq!(leaders.len(), 1, "同一 term 只能有一个 leader: {leaders:?}");

    eprintln!("\n=== 向 leader 写入 ===");
    let mut client = cluster.client(leader).await;
    let resp = client
        .append(AppendRequest {
            stream_id: "replicated".to_string(),
            expected_version: Some(ExpectedVersion {
                kind: Some(expected_version::Kind::NoStream(Empty {})),
            }),
            events: vec![
                NewEvent {
                    event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                    event_type: "E".to_string(),
                    data: b"one".to_vec(),
                    metadata: vec![],
                },
                NewEvent {
                    event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                    event_type: "E".to_string(),
                    data: b"two".to_vec(),
                    metadata: vec![],
                },
            ],
        })
        .await
        .expect("append 到 leader")
        .into_inner();
    assert_eq!(resp.next_expected_version, 1);
    eprintln!("✓ 写入成功，version=0..=1");
    cluster.sync_route_table_from(leader).await;

    eprintln!("\n=== 校验日志已复制到 follower ===");
    let applied = cluster.raft_state(leader).await.last_applied;
    for id in 1..=3u64 {
        if id == leader {
            continue;
        }
        // 复制是异步的，轮询等 follower 追平
        cluster
            .wait_applied(id, applied, Duration::from_secs(10))
            .await;

        // 直接读 follower 的本地状态机，确认数据真的落到了对端
        let mut c = cluster.client(id).await;
        let mut s = c
            .read_stream(ReadStreamRequest {
                stream_id: "replicated".to_string(),
                from_version: 0,
                max_count: 0,
                direction: Direction::Forward as i32,
            })
            .await
            .expect("从 follower 读流")
            .into_inner();

        let mut events = Vec::new();
        while let Some(r) = s.message().await.expect("读流式响应") {
            events.extend(r.events);
        }

        assert_eq!(events.len(), 2, "follower node{id} 应有 2 条事件");
        assert_eq!(events[0].data, b"one");
        assert_eq!(events[1].data, b"two");
        eprintln!("✓ node{id} 已复制 2 条事件");
    }

    eprintln!("\n=== 测试通过 ===");
    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn non_leader_write_rejected_read_ok() {
    let cluster = TestCluster::start().await;
    cluster.form_cluster().await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;

    // 先经 leader 写入
    let mut lc = cluster.client(leader).await;
    lc.append(AppendRequest {
        stream_id: "s".to_string(),
        expected_version: Some(ExpectedVersion {
            kind: Some(expected_version::Kind::NoStream(Empty {})),
        }),
        events: vec![NewEvent {
            event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            event_type: "E".to_string(),
            data: b"x".to_vec(),
            metadata: vec![],
        }],
    })
    .await
    .expect("leader 写入");
    cluster.sync_route_table_from(leader).await;

    let follower = (1..=3u64).find(|id| *id != leader).expect("应有 follower");
    let applied = cluster.raft_state(leader).await.last_applied;
    cluster
        .wait_applied(follower, applied, Duration::from_secs(10))
        .await;

    // 写请求打到 follower 必须被拒，且要告诉客户端 leader 在哪，
    // 否则客户端只能盲目轮询其它节点
    let mut fc = cluster.client(follower).await;
    let err = fc
        .append(AppendRequest {
            stream_id: "s2".to_string(),
            expected_version: Some(ExpectedVersion {
                kind: Some(expected_version::Kind::Any(Empty {})),
            }),
            events: vec![NewEvent {
                event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                event_type: "E".to_string(),
                data: b"y".to_vec(),
                metadata: vec![],
            }],
        })
        .await
        .expect_err("写 follower 应失败");

    assert_eq!(
        err.code(),
        tonic::Code::Unavailable,
        "非 leader 应返回可重试的 Unavailable，实际: {:?}",
        err.code()
    );
    assert!(
        err.message().contains("not leader"),
        "错误信息应说明不是 leader: {}",
        err.message()
    );
    // 必须带上 leader 地址，客户端据此重定向
    let leader_addr = cluster.addr_of(leader);
    assert!(
        err.message().contains(&leader_addr),
        "错误信息应含 leader 地址 {leader_addr}，实际: {}",
        err.message()
    );
    eprintln!("✓ follower 拒绝写入并给出 leader 地址: {}", err.message());

    // 客户端按提示重定向到 leader，应当成功
    let mut redirected = cluster.client(leader).await;
    redirected
        .append(AppendRequest {
            stream_id: "s2".to_string(),
            expected_version: Some(ExpectedVersion {
                kind: Some(expected_version::Kind::Any(Empty {})),
            }),
            events: vec![NewEvent {
                event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                event_type: "E".to_string(),
                data: b"y".to_vec(),
                metadata: vec![],
            }],
        })
        .await
        .expect("重定向到 leader 后应写入成功");
    eprintln!("✓ 重定向到 leader 后写入成功");

    // 但读取可以走 follower（读本地已复制的状态机）
    let mut s = fc
        .read_stream(ReadStreamRequest {
            stream_id: "s".to_string(),
            from_version: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
        })
        .await
        .expect("从 follower 读")
        .into_inner();
    let mut events = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        events.extend(r.events);
    }
    assert_eq!(events.len(), 1, "follower 应能读到已复制的数据");
    assert_eq!(events[0].data, b"x");
    eprintln!("✓ follower 可读已复制数据");

    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn sdk_append_redirects_to_leader() {
    // SDK 只给 follower 地址：leader_addr 不在初始节点列表，
    // 必须走完整重定向路径（解析 Unavailable → 连接新地址）才能写入。
    let cluster = TestCluster::start().await;
    cluster.form_cluster().await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
    let follower = (1..=3u64).find(|id| *id != leader).expect("应有 follower");

    let mut sdk = es_client::EventStoreClient::connect(vec![cluster.addr_of(follower)])
        .await
        .expect("连接 SDK");
    sdk.append(
        "sdk-redirect".to_string(),
        es_client::ExpectedVersionBuilder::any(),
        vec![
            es_client::EventBuilder::new("E")
                .data(b"x".to_vec())
                .build(),
        ],
    )
    .await
    .expect("经 leader_addr 重定向后 append 成功");
    eprintln!("✓ SDK 经 leader_addr 重定向写入成功");

    // 等复制后经 SDK 从 follower 读回（读走本地存储，任一节点可达）
    let applied = cluster.raft_state(leader).await.last_applied;
    cluster
        .wait_applied(follower, applied, Duration::from_secs(10))
        .await;
    let events = sdk
        .read_stream(
            "sdk-redirect".to_string(),
            0,
            10,
            es_client::Direction::Forward,
        )
        .await
        .expect("SDK 读回");
    assert_eq!(events.len(), 1, "follower 应能读到经重定向写入的数据");
    assert_eq!(events[0].data, b"x");
    eprintln!("✓ SDK 从 follower 读回重定向写入的数据");

    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn leader_killed_re_elect_data_intact() {
    let mut cluster = TestCluster::start().await;
    cluster.form_cluster().await;

    let old_leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
    eprintln!("✓ 初始 leader 是 node{old_leader}");

    // 写入两条，确认已复制到全部 follower 再杀 leader，
    // 这样才能断言「数据不丢」而非「数据还没来得及复制」
    let mut lc = cluster.client(old_leader).await;
    lc.append(AppendRequest {
        stream_id: "failover".to_string(),
        expected_version: Some(ExpectedVersion {
            kind: Some(expected_version::Kind::NoStream(Empty {})),
        }),
        events: vec![
            NewEvent {
                event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                event_type: "E".to_string(),
                data: b"before".to_vec(),
                metadata: vec![],
            },
            NewEvent {
                event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                event_type: "E".to_string(),
                data: b"crash".to_vec(),
                metadata: vec![],
            },
        ],
    })
    .await
    .expect("写入旧 leader");
    cluster.sync_route_table_from(old_leader).await;

    let applied = cluster.raft_state(old_leader).await.last_applied;
    let survivors: Vec<u64> = (1..=3u64).filter(|id| *id != old_leader).collect();
    for &id in &survivors {
        cluster
            .wait_applied(id, applied, Duration::from_secs(10))
            .await;
    }
    eprintln!("✓ 数据已复制到 {survivors:?}");

    // 杀掉 leader。剩下 2 个节点在 3 成员集群中仍构成多数派，应能选出新 leader
    cluster.kill_node(old_leader);

    let new_leader = cluster
        .wait_leader_among(&survivors, Duration::from_secs(20))
        .await;
    assert_ne!(new_leader, old_leader, "新 leader 不该是被杀的节点");
    eprintln!("✓ 新 leader 是 node{new_leader}");

    // 崩溃前已提交的数据必须仍在
    let mut nc = cluster.client(new_leader).await;
    let mut s = nc
        .read_stream(ReadStreamRequest {
            stream_id: "failover".to_string(),
            from_version: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
        })
        .await
        .expect("从新 leader 读")
        .into_inner();
    let mut events = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        events.extend(r.events);
    }
    assert_eq!(events.len(), 2, "崩溃前已提交的 2 条数据不能丢");
    assert_eq!(events[0].data, b"before");
    assert_eq!(events[1].data, b"crash");
    eprintln!("✓ 崩溃前数据完好");

    // 新 leader 应能继续接受写入，版本号从旧数据之后接续
    let resp = nc
        .append(AppendRequest {
            stream_id: "failover".to_string(),
            expected_version: Some(ExpectedVersion {
                kind: Some(expected_version::Kind::Exact(1)),
            }),
            events: vec![NewEvent {
                event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                event_type: "E".to_string(),
                data: b"after".to_vec(),
                metadata: vec![],
            }],
        })
        .await
        .expect("新 leader 应能写入")
        .into_inner();
    assert_eq!(resp.next_expected_version, 2, "版本号应接续到 2");
    eprintln!("✓ 新 leader 可继续写入，version=2");

    cluster.shutdown();
}

/// 从指定节点读取某个流的全部事件
async fn read_stream_from(cluster: &TestCluster, node_id: u64, stream_id: &str) -> Vec<Event> {
    let mut c = cluster.client(node_id).await;
    let mut s = c
        .read_stream(ReadStreamRequest {
            stream_id: stream_id.to_string(),
            from_version: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
        })
        .await
        .expect("read_stream")
        .into_inner();
    let mut out = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        out.extend(r.events);
    }
    out
}

/// 往指定节点写一条事件
async fn append_to(
    cluster: &TestCluster,
    node_id: u64,
    stream_id: &str,
    data: &[u8],
) -> AppendResponse {
    let mut c = cluster.client(node_id).await;
    c.append(AppendRequest {
        stream_id: stream_id.to_string(),
        expected_version: Some(ExpectedVersion {
            kind: Some(expected_version::Kind::Any(Empty {})),
        }),
        events: vec![NewEvent {
            event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            event_type: "E".to_string(),
            data: data.to_vec(),
            metadata: vec![],
        }],
    })
    .await
    .unwrap_or_else(|e| panic!("向 node{node_id} 写 {stream_id} 失败: {e}"))
    .into_inner()
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn per_shard_election_independent() {
    const SHARDS: u64 = 3;
    eprintln!("\n=== 启动 3 节点 × {SHARDS} 分片 ===");
    let cluster = TestCluster::start_with_shards(SHARDS, false).await;
    cluster.form_cluster().await;

    eprintln!("\n=== 校验每个分片各自选出 leader ===");
    let mut leaders = Vec::new();
    for shard_id in 0..SHARDS {
        let leader = cluster
            .wait_leader_of_shard(shard_id, Duration::from_secs(15))
            .await;
        leaders.push(leader);

        // 每个分片内部：唯一 leader、三节点认同一致、voters 为 3
        let mut count = 0;
        for id in 1..=3u64 {
            let s = cluster.raft_state_of(id, shard_id).await;
            if s.is_leader {
                count += 1;
            }
            assert!(s.has_leader, "分片 {shard_id} node{id} 应已知 leader");
            assert_eq!(
                s.current_leader, leader,
                "分片 {shard_id} 各节点认同的 leader 应一致"
            );
            assert_eq!(s.voter_ids.len(), 3, "分片 {shard_id} 应有 3 个投票成员");
        }
        assert_eq!(count, 1, "分片 {shard_id} 只能有一个 leader");
        eprintln!("  ✓ 分片 {shard_id} leader = node{leader}");
    }

    eprintln!("\n=== 校验分片间数据隔离 ===");
    // 写足够多的流，确保覆盖到多个分片
    let mut by_shard: std::collections::HashMap<u64, Vec<String>> =
        std::collections::HashMap::new();
    for i in 0..30 {
        let name = format!("ms-{i}");
        // 写请求可能打到非 leader，逐节点重试直到成功
        let mut done = None;
        for node in 1..=3u64 {
            let mut c = cluster.client(node).await;
            if let Ok(r) = c
                .append(AppendRequest {
                    stream_id: name.clone(),
                    expected_version: Some(ExpectedVersion {
                        kind: Some(expected_version::Kind::NoStream(Empty {})),
                    }),
                    events: vec![NewEvent {
                        event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
                        event_type: "E".to_string(),
                        data: name.as_bytes().to_vec(),
                        metadata: vec![],
                    }],
                })
                .await
            {
                done = Some(r.into_inner());
                break;
            }
        }
        let r = done.unwrap_or_else(|| panic!("{name} 在所有节点上都写入失败"));
        by_shard.entry(r.shard_id).or_default().push(name);
    }

    assert!(
        by_shard.len() >= 2,
        "30 个流应至少落在 2 个分片上，实际 {:?}",
        by_shard.keys().collect::<Vec<_>>()
    );
    eprintln!(
        "  ✓ 流分布：{:?}",
        by_shard
            .iter()
            .map(|(k, v)| (k, v.len()))
            .collect::<Vec<_>>()
    );

    // 每个分片的 ReadAll 只应看到属于自己的流
    for (shard_id, names) in &by_shard {
        let mut c = cluster.client(1).await;
        let mut s = c
            .read_all(ReadAllRequest {
                shard_ids: vec![*shard_id],
                from_position: 0,
                max_count: 0,
                direction: Direction::Forward as i32,
                from_positions: vec![],
            })
            .await
            .expect("read_all")
            .into_inner();
        let mut got = Vec::new();
        while let Some(r) = s.message().await.expect("读响应") {
            got.extend(r.events);
        }

        let got_names: std::collections::HashSet<&str> =
            got.iter().map(|e| e.stream_id.as_str()).collect();
        let want_names: std::collections::HashSet<&str> =
            names.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            got_names, want_names,
            "分片 {shard_id} 的 ReadAll 应恰好含本分片的流"
        );
        // 分片内 position 严格递增
        for w in got.windows(2) {
            assert!(
                w[1].position > w[0].position,
                "分片 {shard_id} position 应严格递增"
            );
        }
    }
    eprintln!("  ✓ 各分片 ReadAll 数据互不串");

    eprintln!("\n=== 校验跨分片 ReadAll 汇总全部数据 ===");
    let all_shards: Vec<u64> = (0..SHARDS).collect();
    let mut c = cluster.client(1).await;
    let mut s = c
        .read_all(ReadAllRequest {
            shard_ids: all_shards,
            from_position: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
            from_positions: vec![],
        })
        .await
        .expect("跨分片 read_all")
        .into_inner();
    let mut merged = Vec::new();
    while let Some(r) = s.message().await.expect("读响应") {
        merged.extend(r.events);
    }
    assert_eq!(merged.len(), 30, "跨分片应汇总全部 30 条");

    // 归并后各分片子序列仍保持 position 序
    for shard_id in 0..SHARDS {
        let seq: Vec<u64> = merged
            .iter()
            .filter(|e| e.shard_id == shard_id)
            .map(|e| e.position)
            .collect();
        let mut sorted = seq.clone();
        sorted.sort_unstable();
        assert_eq!(
            seq, sorted,
            "分片 {shard_id} 在归并结果中应保持 position 序"
        );
    }
    eprintln!("  ✓ 跨分片归并 30 条，各分片内序不乱");

    eprintln!("\n=== 测试通过 ===");
    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn restart_rejoin_catchup() {
    let mut cluster = TestCluster::start().await;
    cluster.form_cluster().await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
    eprintln!("✓ leader = node{leader}");

    // 选一个 follower 作为待重启节点
    let victim = (1..=3u64).find(|id| *id != leader).expect("应有 follower");

    // 第一批：重启前写入，重启后必须仍在（验证本地日志恢复）
    append_to(&cluster, leader, "restart", b"before-1").await;
    append_to(&cluster, leader, "restart", b"before-2").await;
    cluster.sync_route_table_from(leader).await;
    let applied_before = cluster.raft_state(leader).await.last_applied;
    cluster
        .wait_applied(victim, applied_before, Duration::from_secs(10))
        .await;
    eprintln!("✓ 重启前 2 条已复制到 node{victim}");

    // 停掉 victim
    cluster.restart_node(victim).await;

    // 重启后节点需要重新连上 leader 并追平。
    // 期间 leader 仍在，剩余 2 节点构成多数派，写入不应受影响。
    let applied_mid = cluster.raft_state(leader).await.last_applied;
    assert_eq!(
        applied_mid, applied_before,
        "重启一个 follower 不应影响 leader 的已应用位置"
    );

    // 第二批：在 victim 停机期间写入，重启后应通过日志复制追上
    append_to(&cluster, leader, "restart", b"during-1").await;
    append_to(&cluster, leader, "restart", b"during-2").await;
    append_to(&cluster, leader, "restart", b"during-3").await;
    let applied_after = cluster.raft_state(leader).await.last_applied;
    eprintln!("✓ 停机期间又写入 3 条，leader applied={applied_after}");

    // 重启后的节点应追平到最新
    cluster
        .wait_applied(victim, applied_after, Duration::from_secs(20))
        .await;
    eprintln!("✓ node{victim} 已追平到 applied={applied_after}");

    // 从重启后的节点读，5 条数据顺序完整
    let events = read_stream_from(&cluster, victim, "restart").await;
    let datas: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
    assert_eq!(
        datas,
        vec![
            b"before-1".as_ref(),
            b"before-2".as_ref(),
            b"during-1".as_ref(),
            b"during-2".as_ref(),
            b"during-3".as_ref()
        ],
        "重启节点应恢复重启前数据并追平停机期间的数据"
    );

    // 版本号连续无空洞
    let versions: Vec<u64> = events.iter().map(|e| e.version).collect();
    assert_eq!(versions, vec![0, 1, 2, 3, 4], "版本应连续");
    eprintln!("✓ 重启节点数据完整且版本连续");

    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn concurrent_stream_creation_is_linearized_across_nodes() {
    let cluster = TestCluster::start_with_shards(2, false).await;
    cluster.form_cluster().await;

    // 两个入口同时提交同一未知 Stream，必须由控制 Shard 串行化为唯一归属。
    let mut first = cluster.client(1).await;
    let mut second = cluster.client(2).await;
    let stream_id = "concurrent-owner".to_string();
    let first_create = first.create_stream(CreateStreamRequest {
        stream_id: stream_id.clone(),
    });
    let second_create = second.create_stream(CreateStreamRequest { stream_id });
    let (first_result, second_result) = tokio::join!(first_create, second_create);
    let first_shard = first_result
        .expect("节点一创建 Stream")
        .into_inner()
        .shard_id;
    let second_shard = second_result
        .expect("节点二创建 Stream")
        .into_inner()
        .shard_id;

    assert_eq!(
        first_shard, second_shard,
        "并发首次归属必须由同一个线性化裁决点返回相同 Shard"
    );

    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn unknown_stream_is_rejected_without_control_shard_quorum_and_recovers() {
    let mut cluster = TestCluster::start().await;
    cluster.form_cluster().await;
    let leader = cluster.wait_for_leader(Duration::from_secs(10)).await;
    let followers: Vec<u64> = (1..=3).filter(|node_id| *node_id != leader).collect();

    cluster.kill_node(followers[0]);
    cluster.kill_node(followers[1]);
    let mut isolated_leader = cluster.client(leader).await;
    let rejected = tokio::time::timeout(
        Duration::from_secs(10),
        isolated_leader.create_stream(CreateStreamRequest {
            stream_id: "requires-quorum".into(),
        }),
    )
    .await;
    match rejected {
        Ok(Err(status)) => assert_eq!(status.code(), tonic::Code::Unavailable),
        Err(_) => {} // 客户端超时同样没有确认成功，允许安全重试。
        Ok(Ok(response)) => panic!("无 quorum 时不得确认首次归属: {:?}", response.into_inner()),
    }

    cluster.restart_node(followers[0]).await;
    let recovered_leader = cluster
        .wait_leader_among(&[leader, followers[0]], Duration::from_secs(20))
        .await;
    let mut recovered = cluster.client(recovered_leader).await;
    let created = tokio::time::timeout(
        Duration::from_secs(10),
        recovered.create_stream(CreateStreamRequest {
            stream_id: "requires-quorum".into(),
        }),
    )
    .await
    .expect("恢复 quorum 后请求不应超时")
    .expect("恢复 quorum 后应可确认归属")
    .into_inner();
    assert!(created.shard_id < cluster.num_shards);

    cluster.shutdown();
}

// ============ 自动组建集群（etcd 静态引导语义）============

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn three_node_peers_bootstrap_replicate() {
    eprintln!("\n=== 启动 3 节点集群（配置完整 peers，自动组建）===");
    let cluster = TestCluster::start_auto(&[1, 2, 3]).await;

    eprintln!("\n=== 等待自动组建完成（不调用任何组建 API）===");
    // 不调 form_cluster：三个节点同时竞选，验证随机化选举超时收敛
    let leader = cluster
        .wait_cluster_formed(3, Duration::from_secs(60))
        .await;
    eprintln!("✓ 自动组建完成，leader = node{leader}");

    // 恰好一个 leader，三个节点都认同
    let mut leaders = Vec::new();
    for id in 1..=3u64 {
        let s = cluster.raft_state(id).await;
        assert!(s.has_leader, "node{id} 应已知 leader");
        assert_eq!(s.current_leader, leader, "node{id} 认同的 leader 应一致");
        assert_eq!(s.voter_ids.len(), 3, "node{id} 投票成员应为 3");
        if s.is_leader {
            leaders.push(s.node_id);
        }
    }
    assert_eq!(leaders.len(), 1, "只能有一个 leader: {leaders:?}");
    eprintln!("✓ 三节点 voter_ids=3 且 leader 唯一（多节点同时竞选已收敛）");

    eprintln!("\n=== 写入并验证复制 ===");
    append_to(&cluster, leader, "auto", b"one").await;
    append_to(&cluster, leader, "auto", b"two").await;
    let applied = cluster.raft_state(leader).await.last_applied;
    for id in 1..=3u64 {
        if id == leader {
            continue;
        }
        cluster
            .wait_applied(id, applied, Duration::from_secs(10))
            .await;
    }
    for id in 1..=3u64 {
        let events = read_stream_from(&cluster, id, "auto").await;
        let datas: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
        assert_eq!(
            datas,
            vec![b"one".as_ref(), b"two".as_ref()],
            "node{id} 应有 2 条事件"
        );
    }
    eprintln!("✓ 3 个节点均可读到 2 条事件");

    eprintln!("\n=== 测试通过 ===");
    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn bootstrap_restart_no_reinit() {
    eprintln!("\n=== 启动 3 节点集群（自动组建）===");
    let mut cluster = TestCluster::start_auto(&[1, 2, 3]).await;
    let leader = cluster
        .wait_cluster_formed(3, Duration::from_secs(60))
        .await;
    eprintln!("✓ 自动组建完成，leader = node{leader}");

    // 选一个 follower 重启
    let victim = (1..=3u64).find(|id| *id != leader).expect("应有 follower");

    // 重启前写入
    append_to(&cluster, leader, "auto-restart", b"before").await;
    let applied_before = cluster.raft_state(leader).await.last_applied;
    cluster
        .wait_applied(victim, applied_before, Duration::from_secs(10))
        .await;

    // 重启 victim：日志持久化 → 重启后 is_initialized 跳过自动组建，
    // 不会重复 initialize（无 NotAllowed 错误，集群健康即隐式证明）
    cluster.restart_node(victim).await;

    // 停机期间 leader 继续写入
    append_to(&cluster, leader, "auto-restart", b"during").await;
    let applied_after = cluster.raft_state(leader).await.last_applied;

    // 重启的节点追平，且集群仍是 3 投票成员
    cluster
        .wait_applied(victim, applied_after, Duration::from_secs(20))
        .await;
    let s = cluster.raft_state(victim).await;
    assert_eq!(s.voter_ids.len(), 3, "重启节点应保留投票成员身份");
    eprintln!("✓ node{victim} 重启后追平且 membership 保留（未重复初始化）");

    let events = read_stream_from(&cluster, victim, "auto-restart").await;
    let datas: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
    assert_eq!(datas, vec![b"before".as_ref(), b"during".as_ref()]);

    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn full_cluster_restart_recovers() {
    eprintln!("\n=== 启动 3 节点集群（自动组建）===");
    let mut cluster = TestCluster::start_auto(&[1, 2, 3]).await;
    let leader = cluster
        .wait_cluster_formed(3, Duration::from_secs(60))
        .await;
    eprintln!("✓ 自动组建完成，leader = node{leader}");

    append_to(&cluster, leader, "auto-all-restart", b"persist").await;

    eprintln!("\n=== 重启全部节点 ===");
    cluster.restart_all().await;

    eprintln!("\n=== 等待集群从本地日志自动恢复（不调用任何组建 API）===");
    let leader = cluster
        .wait_cluster_formed(3, Duration::from_secs(60))
        .await;
    eprintln!("✓ full_cluster_restart_recovers，leader = node{leader}");

    // 重启前写入的数据完好
    let events = read_stream_from(&cluster, leader, "auto-all-restart").await;
    let datas: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
    assert_eq!(datas, vec![b"persist".as_ref()], "重启前数据应保留");

    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn out_of_order_start_bootstrap() {
    eprintln!("\n=== 乱序启动：先起 node2，再起 node1、node3 ===");
    // node2 先起：探测不到其它节点，自行用完整成员 initialize（无 quorum 条目未提交）；
    // node1/node3 起来后探测到 node2 已初始化 → 跳过自举 → 投票使其提交
    let cluster = TestCluster::start_auto(&[2, 1, 3]).await;

    eprintln!("\n=== 等待自动组建完成 ===");
    let leader = cluster
        .wait_cluster_formed(3, Duration::from_secs(90))
        .await;
    eprintln!("✓ 乱序启动下集群收敛，leader = node{leader}");

    // 写入验证（写 leader，任一节点读）
    append_to(&cluster, leader, "auto-ordered", b"ok").await;
    let applied = cluster.raft_state(leader).await.last_applied;
    for id in 1..=3u64 {
        cluster
            .wait_applied(id, applied, Duration::from_secs(10))
            .await;
    }
    for id in 1..=3u64 {
        let events = read_stream_from(&cluster, id, "auto-ordered").await;
        assert_eq!(events.len(), 1, "node{id} 应有 1 条事件");
    }
    eprintln!("✓ 乱序启动下数据复制正常");

    cluster.shutdown();
}

#[tokio::test]
#[ignore = "需启动多个进程，耗时较长"]
async fn single_node_self_peer_self_bootstrap() {
    eprintln!("\n=== 启动单节点集群（peers 只含自己）===");
    let cluster = TestCluster::start_single().await;

    let leader = cluster
        .wait_cluster_formed(1, Duration::from_secs(30))
        .await;
    assert_eq!(leader, 1, "单节点集群 leader 应是自己");
    eprintln!("✓ 单节点自动自举，leader = node{leader}");

    // 单成员 quorum，写读闭环
    append_to(&cluster, 1, "auto-single", b"solo").await;
    let events = read_stream_from(&cluster, 1, "auto-single").await;
    let datas: Vec<&[u8]> = events.iter().map(|e| e.data.as_slice()).collect();
    assert_eq!(datas, vec![b"solo".as_ref()]);

    cluster.shutdown();
}
