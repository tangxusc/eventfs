//! 分片管理器：管理多个 Raft 分片。

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::shard::Shard;
use es_core::{Error, Result};

/// 分片管理器
///
/// 负责管理所有 Raft 分片，提供路由与统一访问接口。
pub struct ShardManager {
    /// 所有分片，按 shard_id 索引
    shards: Arc<RwLock<HashMap<u64, Arc<Shard>>>>,

    /// 分片总数（固定，初始化时确定）
    num_shards: u64,

    /// 本节点 ID
    node_id: u64,
}

impl ShardManager {
    /// 创建分片管理器
    pub fn new(node_id: u64, num_shards: u64) -> Self {
        Self {
            shards: Arc::new(RwLock::new(HashMap::new())),
            num_shards,
            node_id,
        }
    }

    /// 获取节点 ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// 获取分片总数
    pub fn num_shards(&self) -> u64 {
        self.num_shards
    }

    /// 注册一个分片
    pub async fn register_shard(&self, shard: Arc<Shard>) -> Result<()> {
        let shard_id = shard.id();
        if shard_id >= self.num_shards {
            return Err(Error::InvalidInput(format!(
                "shard_id {} >= num_shards {}",
                shard_id, self.num_shards
            )));
        }

        let mut shards = self.shards.write().await;
        if shards.contains_key(&shard_id) {
            return Err(Error::InvalidInput(format!(
                "shard {} already registered",
                shard_id
            )));
        }

        shards.insert(shard_id, shard);
        tracing::info!("Registered shard {}", shard_id);
        Ok(())
    }

    /// 获取指定分片
    pub async fn get_shard(&self, shard_id: u64) -> Result<Arc<Shard>> {
        let shards = self.shards.read().await;
        shards
            .get(&shard_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("shard {} not found", shard_id)))
    }

    /// 根据 stream_id 路由到分片
    pub async fn route_shard(&self, stream_id: &str) -> Result<Arc<Shard>> {
        let shard_id = es_core::routing::route(stream_id, self.num_shards);
        self.get_shard(shard_id).await
    }

    /// 获取所有已注册的分片 ID
    pub async fn shard_ids(&self) -> Vec<u64> {
        let shards = self.shards.read().await;
        shards.keys().copied().collect()
    }
}
