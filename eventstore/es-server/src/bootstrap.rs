//! 启动后按配置自动组建 Raft 集群（etcd 静态引导语义）。
//!
//! `node.peers` 非空即触发：日志为空的节点探测 peer 上是否已有集群，
//! 无则用完整 peers 调用 `raft.initialize()` 一步到位（与 etcd
//! `--initial-cluster` 语义一致）；已有日志的节点（重启）直接跳过。
//! 组建不成功不阻塞 serve，节点以 Learner 运行并告警，运维可经 RaftAdmin 手动接管。
//!
//! 双集群防护：所有节点配置相同 ⟹ 并发 initialize 的 membership 日志内容一致，
//! openraft 官方明确"同配置并发 initialize 安全"（日志仲裁收敛，同 etcd 机制）；
//! 不同配置的 split brain 运行时不可解，由探测时 voter_ids 对比告警 + 文档约束兜底。

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InitializeError, RaftError};
use openraft::BasicNode;
use tokio::time::sleep;
use tonic::transport::Channel;
use uuid::Uuid;

use es_proto::eventstore::raft_admin_client::RaftAdminClient;
use es_proto::eventstore::GetRaftStateRequest;
use es_proto::tls::{apply_endpoint_tls, TlsClientConfig};
use es_raft::{normalize_endpoint, Shard, ShardManager};

use crate::config::Config;

/// 初始集群形成延迟上限（毫秒）：错开各节点同一分片的 initialize。
///
/// openraft 官方建议配置 initial cluster formation delay，给初始成员被发现的时间；
/// 各节点随机取 0..=上限，选举收敛的微观保证仍是 600-900ms 随机化选举超时。
const FORMATION_DELAY_MS: u64 = 2_000;

/// 等待所有 peers 端口就绪的超时。超时仅告警，继续尝试（复制/选举自动重试可自愈）。
const PEER_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// 探测重试次数与退避：peer 尚未启动、分片未注册（NotFound）都要重试，
/// gRPC 错误 ≠"无集群"。
const PROBE_ATTEMPTS: usize = 5;
const PROBE_BACKOFF: Duration = Duration::from_millis(300);

/// initialize 重试次数与退避（应对网络/存储抖动）。
const INIT_ATTEMPTS: usize = 3;
const INIT_BACKOFF: Duration = Duration::from_secs(1);

/// 自举模式：由本节点配置决定
#[derive(Debug)]
enum BootstrapMode {
    /// peers 为空：不触发（保持现状，手动组建）
    Disabled,
    /// peers 不含自己：只等复制加入，不探测不初始化
    WaitOnly,
    /// 参与探测，可能发起完整成员 initialize
    Bootstrap(BTreeMap<u64, BasicNode>),
}

/// 探测结果
enum ProbeOutcome {
    /// 发现某 peer 已初始化，携带其 voter_ids
    ClusterExists { at: u64, voters: Vec<u64> },
    /// 所有 peer 可达且均未初始化（确定性结论）
    NoCluster,
    /// 部分 peer 重试后仍不可达（无法判定；同配置下仍可安全 initialize）
    Unreachable(Vec<u64>),
}

/// 顶层流程：校验配置 → 全局等端口 → 每分片一个后台任务。
pub async fn run(config: &Config, sm: Arc<ShardManager>) {
    let mode = match decide_mode(config) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!("自动组建配置错误，跳过：{e}");
            return;
        }
    };
    let members = match mode {
        BootstrapMode::Disabled => {
            tracing::info!("node.peers 为空，不自动组建（可经 RaftAdmin 手动组建）");
            return;
        }
        BootstrapMode::WaitOnly => {
            tracing::info!(
                "node.peers 不含本节点（id={}），等待集群组建后以复制方式加入",
                config.node.id
            );
            return;
        }
        BootstrapMode::Bootstrap(m) => m,
    };

    // 端口就绪是全局条件，只做一次（含自己——本节点 RaftRpc 必须在监听，
    // 完整成员 initialize 无 blocking，leader 向其他 voter 复制时它们必须可达）
    let addrs: Vec<String> = members.values().map(|n| n.addr.clone()).collect();
    if !wait_peers_ready(&addrs, PEER_READY_TIMEOUT).await {
        tracing::warn!("部分 peer 端口超时未就绪，继续尝试（openraft 复制/选举自动重试，可自愈）");
    }

    // 客户端信任策略：ca_file 严格校验，缺省跳过校验（自签友好）。
    // ca_file 读取失败绝不静默降级，直接跳过自动组建（可手动接管）。
    let tls = match config.tls.as_ref().map(|t| t.client_trust()) {
        Some(Ok(t)) => Some(t),
        Some(Err(e)) => {
            tracing::error!("TLS 客户端配置无效，跳过自动组建：{e}");
            return;
        }
        None => None,
    };
    if tls.is_none() && members.values().any(|n| n.addr.starts_with("https://")) {
        tracing::warn!(
            "node.peers 含 https:// 地址但未配置 [tls]：将以默认跳过校验方式连接；若为明文部署请检查地址 scheme"
        );
    }

    // 探测客户端：connect_lazy 无 I/O，各分片共享
    let clients = match build_clients(&members, tls.as_ref()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!("构建探测客户端失败，跳过自动组建：{e}");
            return;
        }
    };
    let self_id = config.node.id;

    // 每分片独立探测/自举，分片间并行（分片是独立 Raft group）
    let mut tasks = Vec::new();
    for shard_id in 0..sm.num_shards() {
        let sm = sm.clone();
        let members = members.clone();
        let clients = clients.clone();
        tasks.push(tokio::spawn(async move {
            bootstrap_shard(shard_id, members, clients, self_id, sm).await;
        }));
    }
    for t in tasks {
        let _ = t.await;
    }
}

/// 单个分片的探测与自举。
async fn bootstrap_shard(
    shard_id: u64,
    members: BTreeMap<u64, BasicNode>,
    clients: Arc<BTreeMap<u64, RaftAdminClient<Channel>>>,
    self_id: u64,
    sm: Arc<ShardManager>,
) {
    let shard = match sm.get_shard(shard_id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(shard_id, "取分片失败：{e}");
            return;
        }
    };

    // 幂等：已有日志（重启恢复 / 上次中途停止）→ 跳过，靠复制追平
    match shard.raft.is_initialized().await {
        Ok(true) => {
            tracing::info!(shard_id, "已初始化，跳过自动组建");
            return;
        }
        Ok(false) => {}
        Err(e) => {
            tracing::warn!(shard_id, "is_initialized 失败：{e}");
            return;
        }
    }

    // formation delay：错开各节点同一分片的 initialize
    let delay_ms = (Uuid::new_v4().as_u128() % u128::from(FORMATION_DELAY_MS + 1)) as u64;
    sleep(Duration::from_millis(delay_ms)).await;

    // delay 期间可能已被其它节点的 initialize 复制过来
    if let Ok(true) = shard.raft.is_initialized().await {
        tracing::info!(shard_id, "formation delay 期间已被初始化，跳过");
        return;
    }

    match probe(&clients, &members, self_id, shard_id).await {
        ProbeOutcome::ClusterExists { at, voters } => {
            if !same_ids(&voters, members.keys()) {
                tracing::warn!(
                    shard_id,
                    "peer {at} 已形成集群，但其 voter_ids {voters:?} 与本节点配置 peers 不一致——疑似配置不一致或成员已变更；放弃自举，等复制加入"
                );
            } else {
                tracing::info!(shard_id, "peer {at} 已有集群，放弃自举，等待日志复制");
            }
        }
        ProbeOutcome::NoCluster => {
            try_initialize(&shard, shard_id, &members).await;
        }
        ProbeOutcome::Unreachable(unreachable) => {
            tracing::warn!(
                shard_id,
                "peer {unreachable:?} 探测不可达，无法判定是否已有集群；同配置下并发 initialize 安全，继续自举"
            );
            try_initialize(&shard, shard_id, &members).await;
        }
    }
}

/// 完整成员 initialize 一步到位，错误分类处理。
async fn try_initialize(shard: &Arc<Shard>, shard_id: u64, members: &BTreeMap<u64, BasicNode>) {
    for attempt in 0..INIT_ATTEMPTS {
        match shard.raft.initialize(members.clone()).await {
            Ok(()) => {
                tracing::info!(
                    shard_id,
                    "已用完整成员列表初始化（{} 个投票成员）",
                    members.len()
                );
                return;
            }
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => {
                // 竞态：探测与 initialize 之间被其它节点初始化（含日志复制到达）
                tracing::info!(shard_id, "initialize 时已被其它节点初始化，视为成功");
                return;
            }
            Err(RaftError::APIError(InitializeError::NotInMembers(e))) => {
                // Bootstrap 模式下本不该发生（decide_mode 保证自己在 peers 中），兜底
                tracing::error!(shard_id, "本节点不在配置成员内，保持 Learner 等待加入：{e}");
                return;
            }
            Err(e) => {
                tracing::warn!(shard_id, "initialize 失败（第 {} 次）：{e}", attempt + 1);
                sleep(INIT_BACKOFF).await;
            }
        }
    }
    tracing::error!(
        shard_id,
        "多次 initialize 失败，集群未组建；节点以 Learner 运行，请经 RaftAdmin 手动接管"
    );
}

/// 探测所有 peer（排除自己）：任一已初始化 → ClusterExists。
///
/// 判定依据 GetRaftState 的 last_log_index / voter_ids（空节点两者皆空）。
async fn probe(
    clients: &Arc<BTreeMap<u64, RaftAdminClient<Channel>>>,
    members: &BTreeMap<u64, BasicNode>,
    self_id: u64,
    shard_id: u64,
) -> ProbeOutcome {
    let peers: Vec<u64> = members.keys().copied().filter(|&id| id != self_id).collect();

    let mut unreachable_peers = Vec::new();
    for _ in 0..PROBE_ATTEMPTS {
        unreachable_peers.clear();

        for &pid in &peers {
            let mut client = match clients.get(&pid) {
                Some(c) => c.clone(), // Channel 是 Arc，克隆廉价
                None => continue,
            };
            match client.get_raft_state(GetRaftStateRequest { shard_id }).await {
                Ok(resp) => {
                    let r = resp.into_inner();
                    if (r.has_last_log_index && r.last_log_index > 0) || !r.voter_ids.is_empty() {
                        return ProbeOutcome::ClusterExists { at: pid, voters: r.voter_ids };
                    }
                    // 该 peer 未初始化，继续看下一个
                }
                // 未就绪（含分片未注册的 NotFound）与"未初始化"是两回事，重试
                Err(_) => unreachable_peers.push(pid),
            }
        }

        if unreachable_peers.is_empty() {
            return ProbeOutcome::NoCluster; // 全部 peer 可达且均未初始化
        }
        sleep(PROBE_BACKOFF).await;
    }
    ProbeOutcome::Unreachable(unreachable_peers)
}

/// 判断探测到的 voter_ids 与本节点配置的 peers 是否一致（配置漂移告警用）。
fn same_ids<'a>(voters: &[u64], config_ids: impl Iterator<Item = &'a u64>) -> bool {
    let mut v: Vec<u64> = voters.to_vec();
    v.sort_unstable();
    let mut c: Vec<u64> = config_ids.copied().collect();
    c.sort_unstable();
    v == c
}

/// 等待所有地址（host:port）TCP 可连接。
async fn wait_peers_ready(addrs: &[String], timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    // 地址可能是裸地址或带 http:// 前缀，统一去掉 scheme 供 TcpStream 使用
    let hosts: Vec<String> = addrs
        .iter()
        .map(|a| {
            normalize_endpoint(a)
                .trim_start_matches("http://")
                .trim_start_matches("https://")
                .to_string()
        })
        .collect();

    loop {
        let mut all_ready = true;
        for h in &hosts {
            if tokio::net::TcpStream::connect(h.as_str()).await.is_err() {
                all_ready = false;
                break;
            }
        }
        if all_ready {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// 为每个 peer 构建惰性 gRPC 管理客户端。
///
/// decide_mode 已校验地址合法；https 目标按信任策略装配 TLS（缺省跳过校验）。
/// 返回 Err 的场景：地址非法（decide_mode 后不应发生）或 CA PEM 解析失败。
fn build_clients(
    members: &BTreeMap<u64, BasicNode>,
    tls: Option<&TlsClientConfig>,
) -> Result<BTreeMap<u64, RaftAdminClient<Channel>>, String> {
    let mut out = BTreeMap::new();
    for (&id, node) in members {
        let uri = normalize_endpoint(&node.addr);
        let endpoint = tonic::transport::Endpoint::from_shared(uri.clone())
            .map_err(|e| format!("节点 {id} 地址 {uri} 非法：{e}"))?;
        let endpoint = apply_endpoint_tls(endpoint, tls)
            .map_err(|e| format!("节点 {id} TLS 配置失败（{uri}）: {e}"))?;
        out.insert(id, RaftAdminClient::new(endpoint.connect_lazy()));
    }
    Ok(out)
}

/// 根据配置决定自举模式（纯函数，可单测）。
///
/// - peers 为空 → Disabled（保持现状）
/// - peers 重复 id / 非法 addr → Err（fail-closed，静默去重会改变语义）
/// - peers 不含自己 → WaitOnly
/// - 其余 → Bootstrap（地址统一 normalize 后写入 membership）
fn decide_mode(config: &Config) -> Result<BootstrapMode, String> {
    let peers = &config.node.peers;
    if peers.is_empty() {
        return Ok(BootstrapMode::Disabled);
    }

    let mut seen = HashSet::new();
    let mut members = BTreeMap::new();
    for p in peers {
        if !seen.insert(p.id) {
            return Err(format!("node.peers 含重复节点 id {}", p.id));
        }
        let uri = normalize_endpoint(&p.addr);
        tonic::transport::Endpoint::from_shared(uri.clone())
            .map_err(|e| format!("node.peers 中节点 {} 地址 {uri} 非法：{e}", p.id))?;
        members.insert(p.id, BasicNode { addr: uri });
    }

    if !members.contains_key(&config.node.id) {
        return Ok(BootstrapMode::WaitOnly);
    }
    Ok(BootstrapMode::Bootstrap(members))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with_peers(peers: Vec<(u64, &str)>) -> Config {
        Config {
            node: crate::config::NodeConfig {
                id: 1,
                listen_addr: "127.0.0.1:50051".to_string(),
                peers: peers
                    .into_iter()
                    .map(|(id, addr)| crate::config::PeerConfig {
                        id,
                        addr: addr.to_string(),
                    })
                    .collect(),
            },
            storage: crate::config::StorageConfig {
                data_dir: std::path::PathBuf::from("./data"),
            },
            shards: crate::config::ShardConfig { num_shards: 1 },
            snapshot: Default::default(),
            tls: None,
            limits: Default::default(),
        }
    }

    #[test]
    fn peers_empty_disabled() {
        let cfg = config_with_peers(vec![]);
        assert!(matches!(decide_mode(&cfg), Ok(BootstrapMode::Disabled)));
    }

    #[test]
    fn duplicate_peer_id_rejected() {
        let cfg = config_with_peers(vec![(1, "127.0.0.1:50051"), (1, "127.0.0.1:50052")]);
        let err = decide_mode(&cfg).expect_err("重复 id 应报错");
        assert!(err.contains("重复"), "错误信息应说明原因：{err}");
    }

    #[test]
    fn invalid_addr_rejected() {
        let cfg = config_with_peers(vec![(1, "")]);
        assert!(decide_mode(&cfg).is_err(), "空地址应报错");
    }

    #[test]
    fn self_not_in_peers_wait_only() {
        // 本节点 id=1，peers 不含 1
        let cfg = config_with_peers(vec![(2, "127.0.0.1:50052")]);
        assert!(matches!(decide_mode(&cfg), Ok(BootstrapMode::WaitOnly)));
    }

    #[test]
    fn self_in_peers_bootstrap_with_normalized_addr() {
        let cfg = config_with_peers(vec![
            (1, "127.0.0.1:50051"),
            (2, "http://127.0.0.1:50052"),
        ]);
        match decide_mode(&cfg) {
            Ok(BootstrapMode::Bootstrap(members)) => {
                assert_eq!(members.len(), 2);
                // 裸地址被 normalize 为 http:// 前缀，与网络层回连规则一致
                assert_eq!(members[&1].addr, "http://127.0.0.1:50051");
                assert_eq!(members[&2].addr, "http://127.0.0.1:50052");
            }
            other => panic!("应进入 Bootstrap 模式：{other:?}"),
        }
    }

    #[tokio::test]
    async fn build_clients_bad_ca_non_blocking() {
        let members = BTreeMap::from([(1u64, BasicNode { addr: "https://127.0.0.1:1".into() })]);
        // tonic 的 Certificate::from_pem 构造时不解析，CA 解析延迟到握手时
        // （TlsConnector::new）——构建阶段不报错；握手失败由 es-proto tls 测试覆盖
        let tls = TlsClientConfig::Ca(vec![b'x'; 8]);
        assert!(build_clients(&members, Some(&tls)).is_ok(), "坏 CA 应延迟到握手报错");
    }

    #[tokio::test]
    async fn build_clients_https_no_policy_skip_verify() {
        let members = BTreeMap::from([(1u64, BasicNode { addr: "https://127.0.0.1:1".into() })]);
        // 无信任策略：https 端点默认跳过校验，构建成功（connect_lazy 无 I/O）
        let clients = build_clients(&members, None).expect("应成功");
        assert_eq!(clients.len(), 1);
    }

    #[tokio::test]
    async fn run_tls_invalid_skips_bootstrap() {
        // ca_file 指向不存在文件 → client_trust 失败 → 打 error 日志后跳过组建（不 panic、不降级）
        let mut cfg = config_with_peers(vec![(1, "127.0.0.1:50051")]);
        cfg.tls = Some(crate::config::TlsConfig {
            cert_file: Some(std::path::PathBuf::from("/nonexistent/cert.pem")),
            key_file: Some(std::path::PathBuf::from("/nonexistent/key.pem")),
            ca_file: Some(std::path::PathBuf::from("/nonexistent/ca.pem")),
        });
        let sm = Arc::new(ShardManager::new(1, 1));
        // 不 panic 即通过；ca 读取失败绝不静默降级为跳过校验
        crate::bootstrap::run(&cfg, sm).await;
    }
}
