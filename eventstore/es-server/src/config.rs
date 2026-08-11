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
        }
    }
}
