//! esctl 多节点真实进程测试：用 esctl 命令组建三节点集群，
//! 验证成员管理、数据复制与非 leader 端点写入重定向。
//!
//! 需要启动多个真实进程，默认忽略；运行：
//! `cargo test -p es-ctl --test multi_node_test -- --ignored --test-threads=1`

use std::collections::HashMap;
use std::process::{Child, Command, Output, Stdio};
use std::time::Duration;

/// 测试集群：3 个节点、1 个分片（手动组建路径）
struct TestCluster {
    nodes: HashMap<u64, NodeHandle>,
    /// 持有临时目录到测试结束（重启节点时复用同一数据目录）
    _dirs: Vec<tempfile::TempDir>,
}

struct NodeHandle {
    /// 对外地址，形如 http://127.0.0.1:port
    addr: String,
    port: u16,
    _config_path: std::path::PathBuf,
    process: Child,
}

/// 定位 eventstored 二进制。
///
/// cargo 只为本 crate 的 bin 注入 `CARGO_BIN_EXE_*`，跨 crate 拿不到，
/// 故按「环境变量 → 与测试二进制同 profile 目录（debug/release）→ 兜底路径」
/// 的顺序查找。测试二进制位于 `target/<profile>/deps/`，eventstored 在
/// `target/<profile>/` 下。
fn eventstored_bin() -> std::path::PathBuf {
    if let Ok(p) = std::env::var("EVENTSTORED_BIN") {
        return std::path::PathBuf::from(p);
    }
    if let Some(exe) = std::env::current_exe().ok() {
        if let Some(dir) = exe.parent() {
            // deps/ 下的测试二进制：上一级即 target/<profile>/
            if dir.file_name().map(|n| n == "deps").unwrap_or(false) {
                if let Some(profile) = dir.parent() {
                    let p = profile.join("eventstored");
                    if p.exists() {
                        return p;
                    }
                }
            }
            // 与 esctl 同目录（cargo build 后并存）
            let p = dir.join("eventstored");
            if p.exists() {
                return p;
            }
        }
    }
    std::path::PathBuf::from("target/debug/eventstored")
}

/// 启动 eventstored 子进程（直接跑编译好的二进制，避免 cargo 二层进程的孤儿问题）
fn spawn_node(config_path: &std::path::Path) -> Child {
    Command::new(eventstored_bin())
        .args(["--config", config_path.to_str().expect("配置路径非 UTF-8")])
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("启动节点进程（先 cargo build --bin eventstored）")
}

/// 轮询端口可连（进程真正就绪）
async fn wait_for_port(port: u16, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        match tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
            Ok(_) => return true,
            Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        for node in self.nodes.values_mut() {
            let _ = node.process.kill();
            let _ = node.process.wait();
        }
    }
}

impl TestCluster {
    /// 启动 3 节点单分片集群（配置不含 peers，手动组建路径）
    async fn start() -> Self {
        // 分配端口（与既有 multi_node_test 相同做法：先绑定 0 端口取号再释放）
        let mut ports = Vec::new();
        for _ in 0..3 {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("绑定临时端口");
            let port = listener.local_addr().expect("取地址").port();
            drop(listener);
            std::thread::sleep(Duration::from_millis(50));
            ports.push(port);
        }

        let mut nodes = HashMap::new();
        let mut dirs = Vec::new();
        for (i, port) in ports.iter().enumerate() {
            let id = (i + 1) as u64;
            let dir = tempfile::tempdir().expect("临时目录");
            let config_path = dir.path().join(format!("node{id}.toml"));
            let data_dir = dir.path().join("data");
            std::fs::write(
                &config_path,
                // 手动组建路径（无 peers）：放置表节点须 ∈ peers∪self，只能引用
                // 本节点——rf=1、本节点主承载分片 0，成员关系由 esctl member add 组建
                format!(
                    "[node]\nid = {id}\nlisten_addr = \"127.0.0.1:{port}\"\n\n[storage]\ndata_dir = \"{}\"\n\n[placement]\nreplication_factor = 1\n\n[[placement.nodes]]\nid = {id}\nprimary = [0]\n",
                    data_dir.display()
                ),
            )
            .expect("写配置文件");
            let process = spawn_node(&config_path);
            nodes.insert(
                id,
                NodeHandle {
                    addr: format!("http://127.0.0.1:{port}"),
                    port: *port,
                    _config_path: config_path,
                    process,
                },
            );
            dirs.push(dir);
        }

        // 等全部端口就绪
        for handle in nodes.values() {
            assert!(
                wait_for_port(handle.port, Duration::from_secs(10)).await,
                "节点端口未就绪"
            );
        }

        Self { nodes, _dirs: dirs }
    }

    fn addr_of(&self, id: u64) -> String {
        self.nodes[&id].addr.clone()
    }

    /// 三个端点逗号分隔（esctl --endpoints 格式）
    fn endpoints(&self) -> String {
        (1..=3)
            .map(|id| self.addr_of(id))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// 以子进程方式运行 esctl，返回完整输出
fn esctl(endpoints: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(["--endpoints", endpoints]);
    cmd.args(args);
    cmd.output().expect("运行 esctl")
}

fn err(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "需启动多个真实进程，耗时较长"]
async fn three_node_bootstrap_replicate_membership() {
    let cluster = TestCluster::start().await;
    let endpoints = cluster.endpoints();

    // 1. init：分片 0 以 node1 单成员自举（对应 openraft 推荐流程的第一步）
    let out = esctl(
        &endpoints,
        &[
            "init",
            "--shard",
            "0",
            "--member",
            &format!("1@{}", cluster.addr_of(1)),
        ],
    );
    assert!(out.status.success(), "init 失败: {}", err(&out));
    assert!(stdout(&out).contains("已初始化"), "{}", stdout(&out));

    // 等 node1 选主
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 2. 依次加 node2、node3 为投票成员（add_learner blocking + change_membership）
    for id in [2u64, 3] {
        let member = format!("{id}@{}", cluster.addr_of(id));
        let out = esctl(
            &endpoints,
            &["member", "add", "--shard", "0", "--member", &member],
        );
        assert!(
            out.status.success(),
            "member add node{id} 失败: {}",
            err(&out)
        );
        assert!(stdout(&out).contains("已提升"), "{}", stdout(&out));
    }

    // 3. member list：3 个投票成员、恰好一个 Leader
    let out = esctl(&endpoints, &["member", "list"]);
    assert!(out.status.success(), "member list 失败: {}", err(&out));
    let text = stdout(&out);
    assert!(text.contains("voters=[1,2,3]"), "{text}");
    let leaders = text.matches("(Leader)").count();
    assert_eq!(leaders, 1, "应恰好一个 Leader: {text}");

    // 4. 复制验证：append 到集群（可能经重定向打 leader），从 node3 端点读回
    let out = esctl(
        &endpoints,
        &[
            "append",
            "orders/1",
            "--event-type",
            "OrderPlaced",
            "--data",
            "hello",
        ],
    );
    assert!(out.status.success(), "append 失败: {}", err(&out));

    let out = esctl(&cluster.addr_of(3), &["read", "orders/1"]);
    assert!(out.status.success(), "从 node3 读失败: {}", err(&out));
    assert!(stdout(&out).contains("[OrderPlaced]"), "{}", stdout(&out));

    // 5. member remove node3：voters 变 [1,2]
    let out = esctl(
        &endpoints,
        &["member", "remove", "--shard", "0", "--node-id", "3"],
    );
    assert!(out.status.success(), "member remove 失败: {}", err(&out));
    let out = esctl(&endpoints, &["member", "list"]);
    assert!(stdout(&out).contains("voters=[1,2]"), "{}", stdout(&out));
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "需启动多个真实进程，耗时较长"]
async fn non_leader_write_redirected() {
    let cluster = TestCluster::start().await;
    let endpoints = cluster.endpoints();

    let out = esctl(
        &endpoints,
        &[
            "init",
            "--shard",
            "0",
            "--member",
            &format!("1@{}", cluster.addr_of(1)),
        ],
    );
    assert!(out.status.success(), "init 失败: {}", err(&out));
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // 只加 node2（node3 保持关闭的 learner 无关——这里只验证重定向路径）
    let out = esctl(
        &endpoints,
        &[
            "member",
            "add",
            "--shard",
            "0",
            "--member",
            &format!("2@{}", cluster.addr_of(2)),
        ],
    );
    assert!(out.status.success(), "member add 失败: {}", err(&out));

    // 关键：--endpoints 只给 node2。若 node2 非 leader，append 必须
    // 经 Unavailable 提示重定向到 leader 成功，而不是失败。
    let out = esctl(
        &cluster.addr_of(2),
        &["append", "s/redirect", "--event-type", "T", "--data", "x"],
    );
    assert!(
        out.status.success(),
        "经 node2 写入失败（重定向未生效）: {}",
        err(&out)
    );

    // 数据确实落盘：从 node1 读回
    let out = esctl(&cluster.addr_of(1), &["read", "s/redirect"]);
    assert!(stdout(&out).contains("[T]"), "{}", stdout(&out));
}
