//! 单个 Raft 分片的封装。

use openraft::Raft;
use std::sync::Arc;

use es_storage::{EsStorage, TypeConfig};

/// 单个 Raft 分片
///
/// 封装一个 Raft 节点实例及其存储。
pub struct Shard {
    /// 分片 ID
    pub shard_id: u64,

    /// Raft 节点实例
    pub raft: Raft<TypeConfig>,

    /// 存储层（已被 Raft 持有，这里保留引用方便访问）
    pub storage: Arc<EsStorage>,
}

impl Shard {
    /// 创建新分片
    pub fn new(shard_id: u64, raft: Raft<TypeConfig>, storage: Arc<EsStorage>) -> Self {
        Self {
            shard_id,
            raft,
            storage,
        }
    }

    /// 获取分片 ID
    pub fn id(&self) -> u64 {
        self.shard_id
    }
}
