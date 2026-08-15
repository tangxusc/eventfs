//! RaftLogStorage trait 实现。

use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{LogId, LogState, StorageError, StorageIOError, Vote};

use super::EsStorage;
use super::storage::read_logs_err;
use crate::key;
use crate::raft_type::TypeConfig;

/// 序列化辅助，错误统一转为 io 错误描述。
/// 存储值统一 bincode 编码（见 crate::encode：JSON 的 base64 膨胀曾限制
/// 写入吞吐，2026-08 迁移）
fn to_bytes<T: serde::Serialize>(v: &T) -> std::result::Result<Vec<u8>, std::io::Error> {
    crate::encode::encode(v).map_err(std::io::Error::other)
}

fn from_bytes<T: serde::de::DeserializeOwned>(b: &[u8]) -> std::result::Result<T, std::io::Error> {
    crate::encode::decode(b).map_err(std::io::Error::other)
}

impl RaftLogStorage<TypeConfig> for EsStorage {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<u64>> {
        let last_purged = self.read_last_purged().map_err(read_logs_err)?;
        let last_in_log = self.read_last_log_id().map_err(read_logs_err)?;

        // openraft 契约：日志为空时 last_log_id 回落到 last_purged
        let last_log_id = match last_in_log {
            Some(x) => Some(x),
            None => last_purged,
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let k = key::raft_committed(self.shard_id());
        let v = to_bytes(&committed).map_err(|e| StorageIOError::write(&e))?;
        self.set(&k, &v)
            .await
            .map_err(|e| StorageIOError::write(&std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        let k = key::raft_committed(self.shard_id());
        match self.get(&k).map_err(read_logs_err)? {
            None => Ok(None),
            Some(bytes) => {
                let committed: Option<LogId<u64>> =
                    from_bytes(&bytes).map_err(|e| StorageIOError::read(&e))?;
                Ok(committed)
            }
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        let k = key::raft_vote(self.shard_id());
        let v = to_bytes(vote).map_err(|e| StorageIOError::write_vote(&e))?;
        // openraft 要求 save_vote 返回前 vote 必须已落盘，commit 已保证
        self.set(&k, &v)
            .await
            .map_err(|e| StorageIOError::write_vote(&std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        let k = key::raft_vote(self.shard_id());
        match self
            .get(&k)
            .map_err(|e| StorageIOError::read_vote(&std::io::Error::other(e.to_string())))?
        {
            None => Ok(None),
            Some(bytes) => {
                let vote: Vote<u64> =
                    from_bytes(&bytes).map_err(|e| StorageIOError::read_vote(&e))?;
                Ok(Some(vote))
            }
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>>,
    {
        let mut txn = self
            .tree()
            .begin()
            .map_err(|e| StorageIOError::write_logs(&std::io::Error::other(e.to_string())))?;

        for entry in entries {
            let log_id = entry.log_id;
            let k = key::raft_log_entry(self.shard_id(), log_id.index);
            let v = to_bytes(&entry).map_err(|e| StorageIOError::write_log_entry(log_id, &e))?;
            txn.set(k, v).map_err(|e| {
                StorageIOError::write_log_entry(log_id, &std::io::Error::other(e.to_string()))
            })?;
        }

        // 单事务提交，保证这批 entry 要么全可见要么全不可见
        txn.commit()
            .await
            .map_err(|e| StorageIOError::write_logs(&std::io::Error::other(e.to_string())))?;

        // 落盘完成后再通知 openraft
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        // 删除 [log_id.index, +oo) 的日志
        let start = key::raft_log_entry(self.shard_id(), log_id.index);
        let end = key::raft_log_upper(self.shard_id());

        let keys = self.collect_keys(start, end).map_err(read_logs_err)?;
        self.delete_batch(&keys)
            .await
            .map_err(|e| StorageIOError::write_logs(&std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        // 先记录 last_purged，再删日志。
        // 顺序很重要：若先删日志后写 meta 时崩溃，重启后 get_log_state
        // 会同时读不到日志也读不到 purged 标记，openraft 会误认为日志从未存在。
        // 注意编码为 Option<LogId>：与 restore 写回的格式一致（JSON 时代
        // Option 无序列化标记碰巧兼容，bincode 严格后必须统一）
        let k = key::raft_last_purged(self.shard_id());
        let v = to_bytes(&Some(log_id)).map_err(|e| StorageIOError::write(&e))?;
        self.set(&k, &v)
            .await
            .map_err(|e| StorageIOError::write(&std::io::Error::other(e.to_string())))?;

        // 删除 (-oo, log_id.index] 的日志
        let start = key::raft_log_prefix(self.shard_id());
        let end = match log_id.index.checked_add(1) {
            Some(next) => key::raft_log_entry(self.shard_id(), next),
            None => key::raft_log_upper(self.shard_id()), // index == u64::MAX
        };

        let keys = self.collect_keys(start, end).map_err(read_logs_err)?;
        self.delete_batch(&keys)
            .await
            .map_err(|e| StorageIOError::write_logs(&std::io::Error::other(e.to_string())))?;
        Ok(())
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

impl EsStorage {
    /// 读取 last_purged_log_id
    ///
    /// 损坏检测：写入方（purge/restore）只写 `Some`，key 存在但值解码
    /// 为 `None`（bincode 单字节 0x00，只可能是存储清零/截断）必须响亮
    /// 报错——静默当作「从未 purge」会让 openraft 认为日志从 0 开始，
    /// 重放已删除的日志。
    pub(crate) fn read_last_purged(&self) -> es_core::Result<Option<LogId<u64>>> {
        let k = key::raft_last_purged(self.shard_id());
        match self.get(&k)? {
            None => Ok(None),
            Some(bytes) => {
                let log_id: Option<LogId<u64>> = from_bytes(&bytes)
                    .map_err(|e| es_core::Error::Serde(format!("last_purged 反序列化失败: {e}")))?;
                match log_id {
                    Some(id) => Ok(Some(id)),
                    None => Err(es_core::Error::Serde(format!(
                        "last_purged 损坏：key 存在但值编码为 None（写入方只写 Some）"
                    ))),
                }
            }
        }
    }
}
