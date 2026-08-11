//! EventStore 存储实现：基于 surrealkv 的 RaftLogStorage 与 RaftStateMachine。

use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::{RaftLogReader, StorageIOError};
use surrealkv::LSMIterator;
use tokio::sync::RwLock;

use crate::key;
use crate::raft_type::TypeConfig;
use es_core::{Error, Event, Result};

/// EventStore 存储：单个分片的 Raft 日志与状态机
///
/// 多个分片共享同一个 `Arc<surrealkv::Tree>`，通过 key 前缀隔离。
/// 快照与业务数据分离：存于独立快照目录（snapshot_store）。
#[derive(Clone)]
pub struct EsStorage {
    shard_id: u64,
    tree: Arc<surrealkv::Tree>,
    /// 状态机内存缓存。持久化真值在 tree 中，此处用于快速读取。
    sm_cache: Arc<RwLock<SmCache>>,
    /// 事件广播通道：apply 成功后发送新事件，供 Subscribe 订阅
    event_tx: tokio::sync::broadcast::Sender<Event>,
    /// 快照文件存储（独立目录，与业务数据分离）
    snapshot_store: crate::snapshot::SnapshotStore,
}

/// 状态机缓存
pub(crate) struct SmCache {
    /// 最后应用的日志 ID
    pub(crate) last_applied: Option<openraft::LogId<u64>>,
    /// 当前 membership
    pub(crate) membership: openraft::StoredMembership<u64, openraft::BasicNode>,
}

impl EsStorage {
    /// 创建或打开存储。
    ///
    /// 注意：调用方需保证同一个 `tree` 上不同分片的 shard_id 互不相同，
    /// 否则 key 前缀会冲突。`snapshot` 描述快照目录/压缩/保留策略。
    pub fn new(
        shard_id: u64,
        tree: Arc<surrealkv::Tree>,
        snapshot: crate::snapshot::SnapshotConfig,
    ) -> Result<Self> {
        // 创建事件广播通道，容量 1000（订阅者慢了会收到 Lagged 错误）
        let (event_tx, _rx) = tokio::sync::broadcast::channel(1000);
        let snapshot_store = crate::snapshot::SnapshotStore::new(snapshot, shard_id)
            .map_err(|e| Error::Storage(format!("快照目录初始化失败: {e}")))?;

        Ok(Self {
            shard_id,
            tree,
            sm_cache: Arc::new(RwLock::new(SmCache {
                last_applied: None,
                membership: Default::default(),
            })),
            event_tx,
            snapshot_store,
        })
    }

    /// 获取快照存储（目录/保留/传输句柄）
    pub fn snapshot_store(&self) -> &crate::snapshot::SnapshotStore {
        &self.snapshot_store
    }

    /// 获取分片 ID
    pub fn shard_id(&self) -> u64 {
        self.shard_id
    }

    /// 订阅新事件（用于 Subscribe RPC）
    ///
    /// 返回的 Receiver 会接收到 apply 后广播的所有新事件。
    /// 订阅者慢了（落后超过 1000 条）会收到 `RecvError::Lagged`。
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<Event> {
        self.event_tx.subscribe()
    }

    /// 获取底层 tree
    pub fn tree(&self) -> &Arc<surrealkv::Tree> {
        &self.tree
    }

    pub(crate) fn sm_cache(&self) -> &Arc<RwLock<SmCache>> {
        &self.sm_cache
    }

    pub(crate) fn event_tx(&self) -> &tokio::sync::broadcast::Sender<Event> {
        &self.event_tx
    }

    /// 关闭底层存储，释放 LOCK 文件。
    ///
    /// 必须显式调用：`Tree::close` 是 async 而 `Drop` 是同步的，析构函数走不了
    /// 同一条异步关闭路径，因此仅靠 drop 不保证释放锁，下次打开同一目录会报
    /// "already locked by another process"。
    ///
    /// 先 `flush_wal(true)` 落盘再关闭，保证已提交事务的持久性。
    pub async fn close(&self) -> Result<()> {
        self.tree
            .flush_wal(true)
            .map_err(|e| Error::Storage(format!("flush_wal 失败: {e}")))?;
        self.tree
            .close()
            .await
            .map_err(|e| Error::Storage(format!("close 失败: {e}")))
    }

    /// 读取单个 key。
    pub(crate) fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn = self
            .tree
            .begin()
            .map_err(|e| Error::Storage(format!("begin 失败: {e}")))?;
        txn.get(k.to_vec())
            .map_err(|e| Error::Storage(format!("get 失败: {e}")))
    }

    /// 写入单个 key。commit 是 async，直接 await，不使用 block_in_place。
    pub(crate) async fn set(&self, k: &[u8], v: &[u8]) -> Result<()> {
        let mut txn = self
            .tree
            .begin()
            .map_err(|e| Error::Storage(format!("begin 失败: {e}")))?;
        txn.set(k.to_vec(), v.to_vec())
            .map_err(|e| Error::Storage(format!("set 失败: {e}")))?;
        txn.commit()
            .await
            .map_err(|e| Error::Storage(format!("commit 失败: {e}")))
    }

    /// 收集某 key 区间内的所有 key（半开区间 `[start, end)`）。
    ///
    /// surrealkv 的 `range` 返回游标式迭代器，需用 `seek_first`/`next` 驱动。
    pub(crate) fn collect_keys(&self, start: Vec<u8>, end: Vec<u8>) -> Result<Vec<Vec<u8>>> {
        // start >= end 时 surrealkv 行为未定义，提前返回空
        if start >= end {
            return Ok(Vec::new());
        }
        let txn = self
            .tree
            .begin()
            .map_err(|e| Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| Error::Storage(format!("range 失败: {e}")))?;

        let mut keys = Vec::new();
        it.seek_first()
            .map_err(|e| Error::Storage(format!("seek_first 失败: {e}")))?;
        while it.valid() {
            keys.push(it.key().user_key().to_vec());
            it.next()
                .map_err(|e| Error::Storage(format!("next 失败: {e}")))?;
        }
        Ok(keys)
    }

    /// 批量删除给定 key，单事务提交。
    ///
    /// 用 `delete`（硬删除全部版本）而非 `soft_delete`：Raft 的
    /// truncate/purge 语义是永久移除日志，不需要保留墓碑。
    pub(crate) async fn delete_batch(&self, keys: &[Vec<u8>]) -> Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        let mut txn = self
            .tree
            .begin()
            .map_err(|e| Error::Storage(format!("begin 失败: {e}")))?;
        for k in keys {
            txn.delete(k.clone())
                .map_err(|e| Error::Storage(format!("delete 失败: {e}")))?;
        }
        txn.commit()
            .await
            .map_err(|e| Error::Storage(format!("commit 失败: {e}")))
    }

    /// 把 `RangeBounds<u64>`（日志 index 区间）映射为字节 key 的半开区间。
    ///
    /// 返回 `None` 表示区间为空。关键点：
    /// - `Excluded(start)` 需 +1；start 为 `u64::MAX` 时区间必空
    /// - `Included(end)` 需 +1 转为排他；end 为 `u64::MAX` 时用日志区上界，
    ///   否则 `u64::MAX + 1` 溢出会漏掉最后一条
    /// - `Unbounded` 上界用 `raft_log_upper`（即 vote key），左闭右开恰好排除
    pub(crate) fn log_range_keys<RB: RangeBounds<u64>>(
        &self,
        range: &RB,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        let start_idx: u64 = match range.start_bound() {
            Bound::Included(&i) => i,
            Bound::Excluded(&i) => i.checked_add(1)?, // MAX 时区间空
            Bound::Unbounded => 0,
        };

        let start = key::raft_log_entry(self.shard_id, start_idx);

        let end = match range.end_bound() {
            Bound::Excluded(&i) => {
                if i <= start_idx {
                    return None; // 空区间
                }
                key::raft_log_entry(self.shard_id, i)
            }
            Bound::Included(&i) => {
                if i < start_idx {
                    return None;
                }
                match i.checked_add(1) {
                    Some(next) => key::raft_log_entry(self.shard_id, next),
                    // i == u64::MAX：用日志区上界，避免溢出且不漏最后一条
                    None => key::raft_log_upper(self.shard_id),
                }
            }
            Bound::Unbounded => key::raft_log_upper(self.shard_id),
        };

        Some((start, end))
    }

    /// 读取指定 index 区间的日志条目。
    pub(crate) fn read_log_entries<RB: RangeBounds<u64>>(
        &self,
        range: &RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>> {
        let Some((start, end)) = self.log_range_keys(range) else {
            return Ok(Vec::new());
        };
        if start >= end {
            return Ok(Vec::new());
        }

        let txn = self
            .tree
            .begin()
            .map_err(|e| Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| Error::Storage(format!("range 失败: {e}")))?;

        let mut entries = Vec::new();
        it.seek_first()
            .map_err(|e| Error::Storage(format!("seek_first 失败: {e}")))?;
        while it.valid() {
            let v = it
                .value()
                .map_err(|e| Error::Storage(format!("value 失败: {e}")))?;
            let entry: openraft::Entry<TypeConfig> = serde_json::from_slice(&v)
                .map_err(|e| Error::Serde(format!("日志条目反序列化失败: {e}")))?;
            entries.push(entry);
            it.next()
                .map_err(|e| Error::Storage(format!("next 失败: {e}")))?;
        }
        Ok(entries)
    }

    /// 反向迭代取该分片最大 index 的日志条目的 LogId。
    ///
    /// 用 `seek_last` 定位到日志区末尾，比正向全扫描省一个数量级。
    pub(crate) fn read_last_log_id(&self) -> Result<Option<openraft::LogId<u64>>> {
        let start = key::raft_log_prefix(self.shard_id);
        let end = key::raft_log_upper(self.shard_id);

        let txn = self
            .tree
            .begin()
            .map_err(|e| Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| Error::Storage(format!("range 失败: {e}")))?;

        it.seek_last()
            .map_err(|e| Error::Storage(format!("seek_last 失败: {e}")))?;
        if !it.valid() {
            return Ok(None);
        }
        let v = it
            .value()
            .map_err(|e| Error::Storage(format!("value 失败: {e}")))?;
        let entry: openraft::Entry<TypeConfig> = serde_json::from_slice(&v)
            .map_err(|e| Error::Serde(format!("日志条目反序列化失败: {e}")))?;
        Ok(Some(entry.log_id))
    }
}

/// 把领域错误转成 openraft 的读日志错误。
pub(crate) fn read_logs_err(e: es_core::Error) -> StorageIOError<u64> {
    StorageIOError::read_logs(&std::io::Error::other(e.to_string()))
}

impl RaftLogReader<TypeConfig> for EsStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<openraft::Entry<TypeConfig>>, openraft::StorageError<u64>> {
        let entries = self.read_log_entries(&range).map_err(read_logs_err)?;
        Ok(entries)
    }
}
