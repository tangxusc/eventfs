//! 服务器配置。

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 服务器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// 节点配置
    pub node: NodeConfig,

    /// 存储配置
    pub storage: StorageConfig,

    /// 分片放置配置（每个 shard 的 raft 成员与节点承载关系）
    #[serde(default)]
    pub placement: PlacementConfig,

    /// 快照配置（可选，可整体缺省）
    #[serde(default)]
    pub snapshot: SnapshotSection,

    /// TLS 配置（可选）。配置即启用 TLS 监听（cert_file+key_file 成对）；
    /// 节点间 RPC 与客户端 API 对 https:// 地址应用信任策略：
    /// ca_file 配置时严格校验，否则默认跳过校验（自签友好）。
    #[serde(default)]
    pub tls: Option<TlsConfig>,

    /// 请求大小限制（可选，可整体缺省）
    #[serde(default)]
    pub limits: LimitsSection,
}

/// 请求大小限制配置（[limits] 段）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsSection {
    /// 单事件 data+metadata 上限（字节），默认 1MiB。
    ///
    /// 一条 append 批在 raft 里是一条日志条目；openraft 对单条超限的
    /// AppendEntries 没有拆小路径，必须从源头限制单事件大小。
    pub max_event_bytes: u64,
    /// 单次 append 请求上限（字节，proto 编码后精确值），默认 7MiB。
    ///
    /// 8MB 传输上限减去 1MiB 余量（逐事件 proto 头 + gRPC 信封），
    /// 保证「总和达标」的请求不会在传输层被拒。
    pub max_append_batch_bytes: u64,
}

impl Default for LimitsSection {
    fn default() -> Self {
        Self {
            max_event_bytes: es_core::limits::MAX_EVENT_PAYLOAD_BYTES as u64,
            max_append_batch_bytes: es_core::limits::MAX_APPEND_BATCH_BYTES as u64,
        }
    }
}

/// 快照配置（[snapshot] 段）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SnapshotSection {
    /// 快照压缩算法：zstd / lz4 / none
    pub compression: es_storage::snapshot::Compression,
    /// 保留历史快照数（含最新），build/install 后清理超出部分
    pub keep: usize,
    /// 快照目录；缺省为 {data_dir}/snapshots
    pub dir: Option<PathBuf>,
    /// 快照分块大小上限（字节），默认 3MiB 与 openraft 一致。
    ///
    /// openraft 0.9.25 对超限快照块直接放弃传输（无拆小路径），
    /// 此值受 [es_core::limits::MAX_SNAPSHOT_CHUNK_BYTES]（6MiB）约束。
    pub max_chunk_size: u64,
}

impl Default for SnapshotSection {
    fn default() -> Self {
        Self {
            compression: Default::default(), // zstd
            keep: 3,
            dir: None,
            max_chunk_size: 3 * 1024 * 1024,
        }
    }
}

/// TLS 配置
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TlsConfig {
    /// 服务端证书（PEM）
    pub cert_file: Option<PathBuf>,

    /// 服务端私钥（PEM）
    pub key_file: Option<PathBuf>,

    /// 客户端信任的 CA（PEM，可含多张证书）；缺省 = 跳过对端证书校验
    pub ca_file: Option<PathBuf>,
}

impl TlsConfig {
    /// 启动期校验：cert/key 必须成对、三个文件存在且非空。失败即 fail-fast。
    pub fn validate(&self) -> Result<(), String> {
        let missing: Vec<&str> = [("cert_file", &self.cert_file), ("key_file", &self.key_file)]
            .into_iter()
            .filter(|(_, f)| f.is_none())
            .map(|(name, _)| name)
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "[tls] cert_file/key_file 必须成对配置，缺少: {missing:?}"
            ));
        }
        for (name, path) in [
            ("cert_file", self.cert_file.as_ref().unwrap()),
            ("key_file", self.key_file.as_ref().unwrap()),
        ] {
            let bytes = std::fs::read(path)
                .map_err(|e| format!("[tls] {name} 读取失败（{:?}）: {e}", path))?;
            if bytes.is_empty() {
                return Err(format!("[tls] {name} 为空文件（{:?}）", path));
            }
        }
        if let Some(ca) = &self.ca_file {
            let bytes = std::fs::read(ca)
                .map_err(|e| format!("[tls] ca_file 读取失败（{:?}）: {e}", ca))?;
            if bytes.is_empty() {
                return Err(format!("[tls] ca_file 为空文件（{:?}）", ca));
            }
        }
        Ok(())
    }

    /// 客户端信任策略：ca_file → 严格校验该 CA；否则跳过校验。
    ///
    /// ca_file 读取失败必须返回 Err——绝不静默降级为跳过校验。
    pub fn client_trust(&self) -> Result<es_proto::tls::TlsClientConfig, String> {
        match &self.ca_file {
            Some(ca) => {
                let pem = std::fs::read(ca)
                    .map_err(|e| format!("[tls] ca_file 读取失败（{:?}）: {e}", ca))?;
                Ok(es_proto::tls::TlsClientConfig::Ca(pem))
            }
            None => Ok(es_proto::tls::TlsClientConfig::SkipVerify),
        }
    }
}

impl Config {
    /// 启动期校验。失败即 fail-fast。
    ///
    /// 分片放置不变式（违规会导致跨节点 membership 不一致或数据丢失）：
    /// - 节点 id 不重复，且 ∈ node.peers ∪ {本节点}
    /// - primary 分区：不同节点的 primary 列表互不相交；每个 shard 恰在一个 primary 中
    /// - 同一节点 primary 与 replica 不相交
    /// - 每个 shard 的承载节点数（primary + replica 合计）恰等于 replication_factor
    pub fn validate(&self) -> Result<(), String> {
        if self.placement.nodes.is_empty() {
            return Err(
                "[placement] nodes 不能为空：必须显式列出每个节点承载的 shards".to_string(),
            );
        }
        if self.placement.replication_factor == 0 {
            return Err("[placement] replication_factor 必须 ≥ 1".to_string());
        }

        let mut node_ids = std::collections::HashSet::new();
        let mut primary_owner: std::collections::HashMap<u64, u64> =
            std::collections::HashMap::new();
        // shard -> 承载节点数（primary + replica 合计）
        let mut holders: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();

        for n in &self.placement.nodes {
            if !node_ids.insert(n.id) {
                return Err(format!("[placement] 节点 id {} 重复", n.id));
            }
            let peer_ids: std::collections::HashSet<u64> =
                self.node.peers.iter().map(|p| p.id).collect();
            if n.id != self.node.id && !peer_ids.contains(&n.id) {
                return Err(format!(
                    "[placement] 节点 {} 不在 node.peers 中（放置表节点须 ∈ peers ∪ 本节点）",
                    n.id
                ));
            }

            for &s in &n.primary {
                if primary_owner.insert(s, n.id).is_some() {
                    return Err(format!("[placement] shard {s} 出现在多个节点的 primary 中"));
                }
            }
            for &s in &n.replica {
                if n.primary.contains(&s) {
                    return Err(format!(
                        "[placement] 节点 {} 的 shard {s} 同时出现在 primary 与 replica",
                        n.id
                    ));
                }
            }
            for &s in n.primary.iter().chain(n.replica.iter()) {
                *holders.entry(s).or_insert(0) += 1;
            }
        }

        // 每个 shard 的承载数必须等于 rf（primary 已保证每个 shard 至少一个 owner）
        for (shard, count) in &holders {
            if *count != self.placement.replication_factor {
                return Err(format!(
                    "[placement] shard {shard} 承载节点数 {count} != replication_factor {}",
                    self.placement.replication_factor
                ));
            }
        }

        if !(1 * 1024 * 1024..=16 * 1024 * 1024).contains(&self.storage.memtable_arena_bytes) {
            return Err(format!(
                "[storage] memtable_arena_bytes 必须 ∈ [1048576, 16777216]，当前 {}",
                self.storage.memtable_arena_bytes
            ));
        }

        if self.snapshot.keep == 0 {
            return Err("[snapshot] keep 必须 ≥ 1（keep=0 会删光全部快照）".to_string());
        }
        if self.limits.max_event_bytes == 0 {
            return Err("[limits] max_event_bytes 必须 ≥ 1".to_string());
        }
        if self.limits.max_append_batch_bytes == 0
            || self.limits.max_append_batch_bytes > es_core::limits::MAX_APPEND_BATCH_BYTES as u64
        {
            return Err(format!(
                "[limits] max_append_batch_bytes 必须 ∈ [1, {}]（8MB 传输上限减去余量）",
                es_core::limits::MAX_APPEND_BATCH_BYTES
            ));
        }
        if self.limits.max_event_bytes > self.limits.max_append_batch_bytes {
            return Err("[limits] max_event_bytes 不能大于 max_append_batch_bytes".to_string());
        }
        if self.snapshot.max_chunk_size == 0
            || self.snapshot.max_chunk_size > es_core::limits::MAX_SNAPSHOT_CHUNK_BYTES as u64
        {
            return Err(format!(
                "[snapshot] max_chunk_size 必须 ∈ [1, {}]（超限快照块 openraft 直接放弃传输）",
                es_core::limits::MAX_SNAPSHOT_CHUNK_BYTES
            ));
        }
        if let Some(internal_addr) = &self.node.internal_listen_addr {
            internal_addr
                .parse::<std::net::SocketAddr>()
                .map_err(|e| format!("[node] internal_listen_addr 非法（{internal_addr}）：{e}"))?;
            if internal_addr == &self.node.listen_addr {
                return Err("[node] internal_listen_addr 不能与 listen_addr 相同".to_string());
            }
        }
        for peer in &self.node.peers {
            if let Some(internal_addr) = &peer.internal_addr {
                let uri = es_raft::normalize_endpoint(internal_addr);
                tonic::transport::Endpoint::from_shared(uri.clone()).map_err(|e| {
                    format!("node.peers 中节点 {} 内部地址 {uri} 非法：{e}", peer.id)
                })?;
            }
        }
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        Ok(())
    }

    /// 本节点承载的 shards（primary ∪ replica，排序去重）。
    /// 运行期配置热更新后，动态创建流程用它对比新旧集合求差。
    pub fn local_shards(&self) -> Vec<u64> {
        let mut out: Vec<u64> = self
            .placement
            .nodes
            .iter()
            .find(|n| n.id == self.node.id)
            .map(|n| n.primary.iter().chain(n.replica.iter()).copied().collect())
            .unwrap_or_default();
        out.sort_unstable();
        out.dedup();
        out
    }

    /// 某 shard 的 raft 成员节点集（承载它的全部节点，排序）。
    /// 所有节点对同一 shard 的计算结果必须一致——这是 bootstrap
    /// membership 与双集群防护的基础。
    pub fn shard_members(&self, shard_id: u64) -> Vec<u64> {
        let mut out: Vec<u64> = self
            .placement
            .nodes
            .iter()
            .filter(|n| n.primary.contains(&shard_id) || n.replica.contains(&shard_id))
            .map(|n| n.id)
            .collect();
        out.sort_unstable();
        out
    }

    /// 集群 shard 总数 = 最大 shard_id + 1（放置表可稀疏，如动态扩容后）。
    pub fn shard_count(&self) -> u64 {
        self.placement
            .nodes
            .iter()
            .flat_map(|n| n.primary.iter().chain(n.replica.iter()))
            .copied()
            .max()
            .map_or(0, |m| m + 1)
    }
}

/// 节点配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// 节点 ID
    pub id: u64,

    /// gRPC 监听地址
    pub listen_addr: String,

    /// 仅节点间内部订阅 RPC 的监听地址。
    ///
    /// 该端口必须由网络策略限制为集群节点可访问，避免将 shard 与 position
    /// 等内部实现细节暴露给客户端。
    #[serde(default)]
    pub internal_listen_addr: Option<String>,

    /// Raft 集群节点列表 (node_id -> addr)。
    ///
    /// 可省略（单节点部署或手动组建路径）：缺省为空，不触发自动组建。
    #[serde(default)]
    pub peers: Vec<PeerConfig>,
}

/// 对等节点配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConfig {
    /// 节点 ID
    pub id: u64,

    /// gRPC 地址
    pub addr: String,

    /// 对等节点内部订阅 RPC 地址。
    #[serde(default)]
    pub internal_addr: Option<String>,
}

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 数据目录
    pub data_dir: PathBuf,

    /// 每个 shard 的 surrealkv LSM memtable arena（字节），范围 [1MiB, 16MiB]，默认 4MiB。
    ///
    /// surrealkv 默认 100MB/实例且打开即预分配；per-shard 多实例布局下
    /// 必须调小，否则 N 个 shard 就是 N×100MB 内存。
    #[serde(default = "default_memtable_arena_bytes")]
    pub memtable_arena_bytes: usize,
}

/// memtable arena 默认值：4MiB（[`StorageConfig::memtable_arena_bytes`]）
const fn default_memtable_arena_bytes() -> usize {
    4 * 1024 * 1024
}

/// 分片放置配置（[placement] 段）
///
/// 显式表达每个节点承载哪些 shards（primary = 主承载，replica = 副本承载）。
/// primary/replica 同为该 shard 的 raft 投票成员，primary 仅是管理偏好标签
/// （leader 仍由 raft 选举产生）。配置变更（加节点/加 shards）由节点
/// watch 配置热加载后运行期生效，无需重启。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PlacementConfig {
    /// 每个 shard 的 raft 投票成员数（primary + replica 合计），默认 2。
    /// 须 ≥ 1；变更需重启（不做运行期 rf 调整）。
    pub replication_factor: u64,

    /// 每节点承载的 shards 列表
    pub nodes: Vec<PlacementNode>,
}

/// 单个节点的承载列表
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementNode {
    /// 节点 ID
    pub id: u64,

    /// 主承载的 shards
    #[serde(default)]
    pub primary: Vec<u64>,

    /// 副本承载的 shards
    #[serde(default)]
    pub replica: Vec<u64>,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        // 与 Config::default（单节点 rf=1）一致，避免误导；
        // 缺 [placement] 段时 nodes 为空，validate 必然失败（fail-fast）
        Self {
            replication_factor: 1,
            nodes: Vec::new(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                id: 1,
                listen_addr: "127.0.0.1:50051".to_string(),
                internal_listen_addr: None,
                peers: Vec::new(),
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("./data"),
                memtable_arena_bytes: default_memtable_arena_bytes(),
            },
            // 默认单节点单 shard（rf=1）：无 peers 时的最小可运行布局
            placement: PlacementConfig {
                replication_factor: 1,
                nodes: vec![PlacementNode {
                    id: 1,
                    primary: vec![0],
                    replica: Vec::new(),
                }],
            },
            snapshot: SnapshotSection::default(),
            tls: None,
            limits: LimitsSection::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_cert_key_must_be_paired() {
        let tls = TlsConfig {
            cert_file: None,
            key_file: None,
            ca_file: None,
        };
        let err = tls.validate().expect_err("缺 cert/key 应报错");
        assert!(err.contains("cert_file"), "错误应说明缺少的字段: {err}");
    }

    #[test]
    fn tls_missing_files_errors() {
        let tls = TlsConfig {
            cert_file: Some(PathBuf::from("/nonexistent/cert.pem")),
            key_file: Some(PathBuf::from("/nonexistent/key.pem")),
            ca_file: None,
        };
        assert!(tls.validate().is_err(), "文件不存在应报错");
    }

    #[test]
    fn tls_client_trust_two_branches() {
        // 无 ca_file → 跳过校验
        let tls = TlsConfig {
            cert_file: None,
            key_file: None,
            ca_file: None,
        };
        assert!(matches!(
            tls.client_trust(),
            Ok(es_proto::tls::TlsClientConfig::SkipVerify)
        ));

        // ca_file 存在 → 严格校验（PEM 内容透传）
        // 注意：TempDir 必须持有到断言完成，否则文件被删
        let dir = tempfile::tempdir().expect("临时目录");
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, b"ca-pem").expect("写 CA");
        let tls = TlsConfig {
            cert_file: None,
            key_file: None,
            ca_file: Some(ca_path),
        };
        match tls.client_trust().expect("读取 CA 信任策略") {
            es_proto::tls::TlsClientConfig::Ca(pem) => assert_eq!(pem, b"ca-pem"),
            es_proto::tls::TlsClientConfig::SkipVerify => {
                panic!("配置 CA 后必须启用严格校验")
            }
        }
    }

    #[test]
    fn tls_ca_read_fail_no_downgrade() {
        let tls = TlsConfig {
            cert_file: None,
            key_file: None,
            ca_file: Some(PathBuf::from("/nonexistent/ca.pem")),
        };
        let err = tls.client_trust().expect_err("ca 读取失败应报错");
        assert!(err.contains("ca_file"), "错误应说明 ca_file: {err}");
    }

    #[test]
    fn tls_empty_cert_or_key_errors() {
        let dir = tempfile::tempdir().expect("临时目录");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        std::fs::write(&cert, b"").expect("写空 cert");
        std::fs::write(&key, b"non-empty-key").expect("写 key");
        let tls = TlsConfig {
            cert_file: Some(cert.clone()),
            key_file: Some(key.clone()),
            ca_file: None,
        };
        let err = tls.validate().expect_err("空 cert 应报错");
        assert!(err.contains("cert_file"), "错误应说明 cert_file: {err}");
    }

    #[test]
    fn tls_empty_ca_errors() {
        let dir = tempfile::tempdir().expect("临时目录");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        let ca = dir.path().join("ca.pem");
        std::fs::write(&cert, b"c").expect("写 cert");
        std::fs::write(&key, b"k").expect("写 key");
        std::fs::write(&ca, b"").expect("写空 ca");
        let tls = TlsConfig {
            cert_file: Some(cert),
            key_file: Some(key),
            ca_file: Some(ca),
        };
        let err = tls.validate().expect_err("空 ca 应报错");
        assert!(err.contains("ca_file"), "错误应说明 ca_file: {err}");
    }

    #[test]
    fn tls_valid_files_pass() {
        let dir = tempfile::tempdir().expect("临时目录");
        let cert = dir.path().join("cert.pem");
        let key = dir.path().join("key.pem");
        let ca = dir.path().join("ca.pem");
        std::fs::write(&cert, b"c").expect("写 cert");
        std::fs::write(&key, b"k").expect("写 key");
        let skip_verify = TlsConfig {
            cert_file: Some(cert.clone()),
            key_file: Some(key.clone()),
            ca_file: None,
        };
        assert!(skip_verify.validate().is_ok());

        std::fs::write(&ca, b"ca").expect("写 CA");
        let strict = TlsConfig {
            cert_file: Some(cert),
            key_file: Some(key),
            ca_file: Some(ca),
        };
        assert!(strict.validate().is_ok());
    }

    /// 构造一个合法放置表的配置（3 节点 rf=2 环形，6 个 shard 各由 2 节点承载）
    fn valid_config() -> Config {
        Config {
            node: NodeConfig {
                id: 1,
                listen_addr: "127.0.0.1:50051".to_string(),
                internal_listen_addr: None,
                peers: vec![
                    PeerConfig {
                        id: 1,
                        addr: "127.0.0.1:50051".into(),
                        internal_addr: None,
                    },
                    PeerConfig {
                        id: 2,
                        addr: "127.0.0.1:50052".into(),
                        internal_addr: None,
                    },
                    PeerConfig {
                        id: 3,
                        addr: "127.0.0.1:50053".into(),
                        internal_addr: None,
                    },
                ],
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("./data"),
                memtable_arena_bytes: default_memtable_arena_bytes(),
            },
            placement: PlacementConfig {
                replication_factor: 2,
                nodes: vec![
                    PlacementNode {
                        id: 1,
                        primary: vec![0, 1],
                        replica: vec![2, 3],
                    },
                    PlacementNode {
                        id: 2,
                        primary: vec![2, 3],
                        replica: vec![4, 5],
                    },
                    PlacementNode {
                        id: 3,
                        primary: vec![4, 5],
                        replica: vec![0, 1],
                    },
                ],
            },
            snapshot: Default::default(),
            tls: None,
            limits: Default::default(),
        }
    }

    #[test]
    fn placement_nodes_empty_rejected() {
        let mut config = valid_config();
        config.placement.nodes = Vec::new();
        let err = config.validate().expect_err("空放置表应报错");
        assert!(err.contains("nodes"), "错误应说明 nodes: {err}");
    }

    #[test]
    fn placement_rf_zero_rejected() {
        let mut config = valid_config();
        config.placement.replication_factor = 0;
        let err = config.validate().expect_err("rf=0 应报错");
        assert!(err.contains("replication_factor"), "错误应说明 rf: {err}");
    }

    #[test]
    fn placement_holder_count_mismatch_rejected() {
        // shard 6 被三个节点 replica 各承载一次（3 holder），rf=2 → 拒绝
        let mut config = valid_config();
        config.placement.nodes[0].replica.push(6);
        config.placement.nodes[1].replica.push(6);
        config.placement.nodes[2].replica.push(6);
        let err = config.validate().expect_err("承载数 != rf 应报错");
        assert!(err.contains("shard 6"), "错误应说明 shard 6: {err}");
    }

    #[test]
    fn placement_primary_overlap_rejected() {
        // 两个节点 primary 都含 shard 0 → 拒绝
        let mut config = valid_config();
        config.placement.nodes[1].primary.push(0);
        let err = config.validate().expect_err("primary 重叠应报错");
        assert!(err.contains("primary"), "错误应说明 primary: {err}");
    }

    #[test]
    fn placement_primary_replica_overlap_rejected() {
        let mut config = valid_config();
        config.placement.nodes[0].replica.push(1); // node1 primary 已有 1
        let err = config.validate().expect_err("primary/replica 重叠应报错");
        assert!(err.contains("primary"), "错误应说明 primary: {err}");
    }

    #[test]
    fn placement_unknown_node_rejected() {
        let mut config = valid_config();
        config.placement.nodes.push(PlacementNode {
            id: 99,
            primary: vec![8],
            replica: Vec::new(),
        });
        let err = config.validate().expect_err("未知节点应报错");
        assert!(err.contains("peers"), "错误应说明 peers: {err}");
    }

    #[test]
    fn placement_duplicate_node_id_rejected() {
        let mut config = valid_config();
        config.placement.nodes.push(PlacementNode {
            id: 1,
            primary: Vec::new(),
            replica: Vec::new(),
        });
        let err = config.validate().expect_err("重复节点 id 应报错");
        assert!(err.contains("重复"), "错误应说明重复: {err}");
    }

    #[test]
    fn memtable_arena_out_of_range_rejected() {
        for bad in [0, 1024 * 1024 - 1, 16 * 1024 * 1024 + 1] {
            let mut config = valid_config();
            config.storage.memtable_arena_bytes = bad;
            let err = config.validate().expect_err("arena 越界应报错");
            assert!(
                err.contains("memtable_arena_bytes"),
                "错误应说明字段: {err}"
            );
        }
    }

    #[test]
    fn valid_placement_passes() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn local_shards_primary_union_replica() {
        let config = valid_config();
        // node1: primary [0,1] + replica [2,3]
        assert_eq!(config.local_shards(), vec![0, 1, 2, 3]);
        // 换视角：node2
        let mut node2 = config.clone();
        node2.node.id = 2;
        assert_eq!(node2.local_shards(), vec![2, 3, 4, 5]);
        let mut node3 = config.clone();
        node3.node.id = 3;
        assert_eq!(node3.local_shards(), vec![0, 1, 4, 5]);
    }

    #[test]
    fn shard_members_consistent_across_nodes() {
        let config = valid_config();
        // shard 0：node1 primary + node3 replica
        assert_eq!(config.shard_members(0), vec![1, 3]);
        // shard 5：node2 replica + node3 primary
        assert_eq!(config.shard_members(5), vec![2, 3]);
        // 未承载的 shard → 空
        assert!(config.shard_members(9).is_empty());
    }

    #[test]
    fn shard_count_from_placement() {
        let config = valid_config();
        assert_eq!(config.shard_count(), 6); // max shard id 5 → 6
    }

    #[test]
    fn local_shards_unknown_node_empty() {
        let config = valid_config();
        let mut cfg = config.clone();
        cfg.node.id = 42;
        assert!(cfg.local_shards().is_empty());
    }

    #[test]
    fn snapshot_keep_zero_rejected() {
        let config = Config {
            snapshot: SnapshotSection {
                keep: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("keep=0 应报错");
        assert!(err.contains("keep"), "错误应说明 keep: {err}");
    }

    #[test]
    fn limits_event_zero_rejected() {
        let config = Config {
            limits: LimitsSection {
                max_event_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("max_event_bytes=0 应报错");
        assert!(
            err.contains("max_event_bytes"),
            "错误应说明 max_event_bytes: {err}"
        );
    }

    #[test]
    fn limits_batch_over_cap_rejected() {
        let config = Config {
            limits: LimitsSection {
                max_append_batch_bytes: es_core::limits::MAX_APPEND_BATCH_BYTES as u64 + 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("超出上限应报错");
        assert!(
            err.contains("max_append_batch_bytes"),
            "错误应说明 max_append_batch_bytes: {err}"
        );
    }

    #[test]
    fn limits_batch_zero_rejected() {
        let config = Config {
            limits: LimitsSection {
                max_append_batch_bytes: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("batch=0 应报错");
        assert!(
            err.contains("max_append_batch_bytes"),
            "错误应说明 max_append_batch_bytes: {err}"
        );
    }

    #[test]
    fn limits_event_greater_than_batch_rejected() {
        let config = Config {
            limits: LimitsSection {
                max_event_bytes: 4096,
                max_append_batch_bytes: 2048,
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("单事件大于批次上限应报错");
        assert!(
            err.contains("max_event_bytes"),
            "错误应说明 max_event_bytes: {err}"
        );
    }

    #[test]
    fn limits_valid_passes() {
        let config = Config {
            limits: LimitsSection {
                max_event_bytes: 1024,
                max_append_batch_bytes: 4096,
            },
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn snapshot_chunk_over_cap_rejected() {
        let config = Config {
            snapshot: SnapshotSection {
                max_chunk_size: es_core::limits::MAX_SNAPSHOT_CHUNK_BYTES as u64 + 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("chunk 超出上限应报错");
        assert!(
            err.contains("max_chunk_size"),
            "错误应说明 max_chunk_size: {err}"
        );
    }

    #[test]
    fn snapshot_chunk_zero_rejected() {
        let config = Config {
            snapshot: SnapshotSection {
                max_chunk_size: 0,
                ..Default::default()
            },
            ..Default::default()
        };
        let err = config.validate().expect_err("chunk=0 应报错");
        assert!(
            err.contains("max_chunk_size"),
            "错误应说明 max_chunk_size: {err}"
        );
    }

    #[test]
    fn snapshot_chunk_default_passes() {
        let config = Config {
            snapshot: SnapshotSection {
                max_chunk_size: 3 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn internal_listen_addr_must_be_distinct_and_valid() {
        let mut config = valid_config();
        config.node.internal_listen_addr = Some(config.node.listen_addr.clone());
        let err = config.validate().expect_err("内部与公共端口相同必须拒绝");
        assert!(
            err.contains("internal_listen_addr"),
            "错误应说明字段: {err}"
        );

        config.node.internal_listen_addr = Some("not-an-address".into());
        let err = config.validate().expect_err("非法内部监听地址必须拒绝");
        assert!(
            err.contains("internal_listen_addr"),
            "错误应说明字段: {err}"
        );
    }

    #[test]
    fn limits_section_deserializes_defaults() {
        // [limits] 段整体缺省时使用默认值(1MiB / 7MiB)
        let config: Config = toml::from_str(
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"

[storage]
data_dir = "./data"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]
"#,
        )
        .expect("配置解析");
        assert_eq!(
            config.limits.max_event_bytes,
            es_core::limits::MAX_EVENT_PAYLOAD_BYTES as u64
        );
        assert_eq!(
            config.limits.max_append_batch_bytes,
            es_core::limits::MAX_APPEND_BATCH_BYTES as u64
        );
        assert_eq!(config.snapshot.max_chunk_size, 3 * 1024 * 1024);
    }

    #[test]
    fn snapshot_section_deserializes_defaults() {
        // [snapshot] 段整体缺省时使用默认值（zstd / keep=3 / 无目录覆盖）
        let config: Config = toml::from_str(
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"

[storage]
data_dir = "./data"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]
"#,
        )
        .expect("配置解析");
        assert_eq!(config.snapshot.compression, Default::default());
        assert_eq!(config.snapshot.keep, 3);
        assert!(config.snapshot.dir.is_none());
    }

    #[test]
    fn snapshot_section_custom_values() {
        let config: Config = toml::from_str(
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"

[storage]
data_dir = "./data"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]

[snapshot]
compression = "lz4"
keep = 5
dir = "./snapshots"
"#,
        )
        .expect("配置解析");
        assert_eq!(
            config.snapshot.compression,
            es_storage::snapshot::Compression::Lz4
        );
        assert_eq!(config.snapshot.keep, 5);
        assert_eq!(config.snapshot.dir, Some(PathBuf::from("./snapshots")));
    }
}
