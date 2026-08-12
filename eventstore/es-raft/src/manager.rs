//! 分片管理器：管理多个 Raft 分片。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::RwLock;

use crate::shard::Shard;
use es_core::{Error, Result};

/// 分片管理器
///
/// 负责管理所有 Raft 分片，提供路由与统一访问接口。
pub struct ShardManager {
    /// 所有分片，按 shard_id 索引
    shards: Arc<RwLock<HashMap<u64, Arc<Shard>>>>,

    /// 分片范围上限 = 已见到的最大 shard_id + 1（启动时为放置表派生值，
    /// 运行期动态扩容新增 shard 时自动扩展——不再有固定上界）
    num_shards: AtomicU64,

    /// 本节点 ID
    node_id: u64,
}

impl ShardManager {
    /// 创建分片管理器
    pub fn new(node_id: u64, num_shards: u64) -> Self {
        Self {
            shards: Arc::new(RwLock::new(HashMap::new())),
            num_shards: AtomicU64::new(num_shards),
            node_id,
        }
    }

    /// 获取节点 ID
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// 获取分片范围上限（= 已注册最大 shard_id + 1，动态扩展）
    pub fn num_shards(&self) -> u64 {
        self.num_shards.load(Ordering::Relaxed)
    }

    /// 注册一个分片。
    ///
    /// 动态扩容语义：shard_id 超过当前范围时自动扩展（运行期新增 shard
    /// 不再被启动时的上界拒绝）；重复注册仍拒绝。
    pub async fn register_shard(&self, shard: Arc<Shard>) -> Result<()> {
        let shard_id = shard.id();

        let mut shards = self.shards.write().await;
        if shards.contains_key(&shard_id) {
            return Err(Error::InvalidInput(format!(
                "shard {} already registered",
                shard_id
            )));
        }

        // 自动扩展范围：动态扩容后 shard_id 可超过启动值
        self.num_shards
            .fetch_max(shard_id + 1, Ordering::Relaxed);

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

    /// 根据 stream_id 路由到分片（哈希提示用；写路径权威是路由表）
    pub async fn route_shard(&self, stream_id: &str) -> Result<Arc<Shard>> {
        let shard_id = es_core::routing::route(stream_id, self.num_shards());
        self.get_shard(shard_id).await
    }

    /// 获取所有已注册的分片 ID
    pub async fn shard_ids(&self) -> Vec<u64> {
        let shards = self.shards.read().await;
        shards.keys().copied().collect()
    }
}
