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

    /// 分片配置
    pub shards: ShardConfig,

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
            return Err(format!("[tls] cert_file/key_file 必须成对配置，缺少: {missing:?}"));
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
    /// `num_shards = 0` 会让路由取模除零 panic（es-core::routing::route），
    /// 必须在启动时拦截，而不是让分片一个都不建、留到请求时崩溃。
    pub fn validate(&self) -> Result<(), String> {
        if self.shards.num_shards == 0 {
            return Err("[shards] num_shards 必须 ≥ 1".to_string());
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
        if let Some(tls) = &self.tls {
            tls.validate()?;
        }
        Ok(())
    }
}

/// 节点配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    /// 节点 ID
    pub id: u64,

    /// gRPC 监听地址
    pub listen_addr: String,

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
}

/// 存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 数据目录
    pub data_dir: PathBuf,
}

/// 分片配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    /// 分片总数
    pub num_shards: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            node: NodeConfig {
                id: 1,
                listen_addr: "127.0.0.1:50051".to_string(),
                peers: Vec::new(),
            },
            storage: StorageConfig {
                data_dir: PathBuf::from("./data"),
            },
            shards: ShardConfig { num_shards: 8 },
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
        assert!(matches!(
            tls.client_trust(),
            Ok(es_proto::tls::TlsClientConfig::Ca(ref pem)) if pem == b"ca-pem"
        ));
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
        std::fs::write(&cert, b"c").expect("写 cert");
        std::fs::write(&key, b"k").expect("写 key");
        let tls = TlsConfig {
            cert_file: Some(cert),
            key_file: Some(key),
            ca_file: None,
        };
        assert!(tls.validate().is_ok());
    }

    #[test]
    fn num_shards_zero_rejected() {
        // 路由取模除零的启动期拦截（es-core::routing::route 对 0 会 panic）
        let config = Config {
            shards: ShardConfig { num_shards: 0 },
            ..Default::default()
        };
        let err = config.validate().expect_err("num_shards=0 应报错");
        assert!(err.contains("num_shards"), "错误应说明 num_shards: {err}");
    }

    #[test]
    fn num_shards_valid_passes() {
        let config = Config {
            shards: ShardConfig { num_shards: 8 },
            ..Default::default()
        };
        assert!(config.validate().is_ok());
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
        assert!(err.contains("max_event_bytes"), "错误应说明 max_event_bytes: {err}");
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
    fn limits_section_deserializes_defaults() {
        // [limits] 段整体缺省时使用默认值(1MiB / 7MiB)
        let config: Config = toml::from_str(
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"

[storage]
data_dir = "./data"

[shards]
num_shards = 8
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

[shards]
num_shards = 8
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

[shards]
num_shards = 8

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
        assert_eq!(
            config.snapshot.dir,
            Some(PathBuf::from("./snapshots"))
        );
    }
}
