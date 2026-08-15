//! RaftStateMachine trait 实现。
//!
//! apply 的核心约束（设计文档 5.3）：
//! 1. `expected_version` 校验必须在 apply 内做，这里是单个 Raft group 的串行执行点，
//!    只有在此处「读当前版本 → 比对 → 写入」才是原子的
//! 2. 事件、StreamMeta、position 指针、next_position、幂等索引、last_applied
//!    六者必须同一个 surrealkv 事务提交，否则崩溃会留下版本号回退等不一致状态
//! 3. 因为在 apply 内持久化状态，快照不要求落盘即可保证正确性

use std::io::Write;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use surrealkv::LSMIterator;

use super::EsStorage;
use crate::key;
use crate::raft_type::TypeConfig;
use crate::snapshot;
use crate::{EsRequest, EsResponse};
use es_core::{
    AggregateCatalog, AggregateEvent, AggregateGroupCatalog, AggregateGroupPartition,
    AggregateMeta, AggregateState, AggregateStateDocument, Event, EventSetId,
    ExpectedAggregateVersion, ExpectedStateRevision, ExpectedVersion, Hlc, NewAggregateEvent,
    StreamMeta,
};

fn sm_read_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::read_state_machine(&std::io::Error::other(e.to_string())).into()
}

fn sm_write_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::write_state_machine(&std::io::Error::other(e.to_string())).into()
}

/// 反序列化事件
fn decode_event(bytes: &[u8]) -> es_core::Result<Event> {
    crate::encode::decode(bytes)
        .map_err(|e| es_core::Error::Serde(format!("Event 反序列化失败: {e}")))
}

fn decode_aggregate_event(bytes: &[u8]) -> es_core::Result<AggregateEvent> {
    crate::encode::decode(bytes)
        .map_err(|error| es_core::Error::Serde(format!("AggregateEvent 反序列化失败: {error}")))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AggregateIdempotencyRecord {
    fingerprint: u128,
    result: es_core::AggregateAppendResult,
}

impl EsStorage {
    /// 扫描 key 区间 `[start, end)` 的所有值。
    ///
    /// `desc` 为 true 时从末尾倒扫（`seek_last` + `prev`）。
    /// `limit` 为 0 表示不限量。
    ///
    /// 独立成同步函数：surrealkv 迭代器不是 `Send`，不能跨 await 存活。
    pub(crate) fn scan_values(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        desc: bool,
        limit: u64,
    ) -> es_core::Result<Vec<Vec<u8>>> {
        if start >= end {
            return Ok(Vec::new());
        }
        let txn = self
            .tree()
            .begin()
            .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| es_core::Error::Storage(format!("range 失败: {e}")))?;

        if desc {
            it.seek_last()
                .map_err(|e| es_core::Error::Storage(format!("seek_last 失败: {e}")))?;
        } else {
            it.seek_first()
                .map_err(|e| es_core::Error::Storage(format!("seek_first 失败: {e}")))?;
        }

        let mut out = Vec::new();
        while it.valid() {
            let v = it
                .value()
                .map_err(|e| es_core::Error::Storage(format!("value 失败: {e}")))?;
            out.push(v.to_vec());
            if limit != 0 && out.len() as u64 >= limit {
                break;
            }
            let moved = if desc {
                it.prev()
                    .map_err(|e| es_core::Error::Storage(format!("prev 失败: {e}")))?
            } else {
                it.next()
                    .map_err(|e| es_core::Error::Storage(format!("next 失败: {e}")))?
            };
            if !moved {
                break;
            }
        }
        Ok(out)
    }

    /// 扫描 key 区间内的 (key, value) 对，用于需要 key 的场景（如枚举流名）
    pub(crate) fn scan_kv(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> es_core::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start >= end {
            return Ok(Vec::new());
        }
        let txn = self
            .tree()
            .begin()
            .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| es_core::Error::Storage(format!("range 失败: {e}")))?;

        let mut out = Vec::new();
        it.seek_first()
            .map_err(|e| es_core::Error::Storage(format!("seek_first 失败: {e}")))?;
        while it.valid() {
            let k = it.key().user_key().to_vec();
            let v = it
                .value()
                .map_err(|e| es_core::Error::Storage(format!("value 失败: {e}")))?
                .to_vec();
            out.push((k, v));
            if !it
                .next()
                .map_err(|e| es_core::Error::Storage(format!("next 失败: {e}")))?
            {
                break;
            }
        }
        Ok(out)
    }
}

/// 已应用状态的持久化形态
///
/// pub(crate)：离线 restore（snapshot.rs）需写回同格式的 applied 状态。
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct AppliedState {
    pub(crate) last_applied: Option<LogId<u64>>,
    pub(crate) membership: StoredMembership<u64, openraft::BasicNode>,
}

impl EsStorage {
    /// 从持久化状态恢复 last_applied 与 membership。
    ///
    /// openraft 在启动时调用 `applied_state`，必须返回真实落盘的值，
    /// 否则会从错误的位置重放日志。**必须在第一次调用 `apply` 前调用。**
    ///
    /// 顺带执行快照文件化的启动清理：删除旧版快照 key（树内单 key 格式已废弃，
    /// 快照是可重建缓存，不迁移）与 incoming 残留临时文件。
    pub async fn restore_applied_state(&self) -> es_core::Result<()> {
        let k = key::sm_applied_state(self.shard_id());
        if let Some(bytes) = self.get(&k)? {
            let st: AppliedState = crate::encode::decode(&bytes)
                .map_err(|e| es_core::Error::Serde(format!("已应用状态反序列化失败: {e}")))?;
            let mut cache = self.sm_cache().write().await;
            cache.last_applied = st.last_applied;
            cache.membership = st.membership;
        }
        self.cleanup_legacy_snapshot().await?;
        Ok(())
    }

    /// 启动清理：删除旧版 `snapshot_current` key + incoming 残留临时文件。
    ///
    /// 快照是状态机的缓存，旧格式数据可直接丢弃（openraft 会按需重建）；
    /// 残留临时文件是上次传输中断留下的，启动时无进行中的传输，安全删除。
    async fn cleanup_legacy_snapshot(&self) -> es_core::Result<()> {
        let k = key::snapshot_current(self.shard_id());
        if self.get(&k)?.is_some() {
            self.delete_batch(&[k]).await?;
            tracing::info!("已清理旧版快照 key（快照已改为独立文件格式）");
        }
        let n = self
            .snapshot_store()
            .cleanup_incoming()
            .map_err(|e| es_core::Error::Storage(format!("清理快照临时文件失败: {e}")))?;
        if n > 0 {
            tracing::info!("已清理 {n} 个残留快照临时文件");
        }
        Ok(())
    }

    /// 读取流当前元数据。流不存在时返回 None。
    pub fn read_stream_meta(&self, stream_id: &str) -> es_core::Result<Option<StreamMeta>> {
        let k = key::sm_stream_meta(self.shard_id(), stream_id);
        match self.get(&k)? {
            None => Ok(None),
            Some(bytes) => {
                let meta: StreamMeta = crate::encode::decode(&bytes)
                    .map_err(|e| es_core::Error::Serde(format!("StreamMeta 反序列化失败: {e}")))?;
                Ok(Some(meta))
            }
        }
    }

    /// 读取单条事件
    pub fn read_event(&self, stream_id: &str, version: u64) -> es_core::Result<Option<Event>> {
        let k = key::sm_event(self.shard_id(), stream_id, version);
        match self.get(&k)? {
            None => Ok(None),
            Some(bytes) => {
                let ev: Event = crate::encode::decode(&bytes)
                    .map_err(|e| es_core::Error::Serde(format!("Event 反序列化失败: {e}")))?;
                Ok(Some(ev))
            }
        }
    }

    /// 读取某流的事件区间 `[from, from+limit)`。limit 为 0 表示不限量。
    pub fn read_stream_events(
        &self,
        stream_id: &str,
        from: u64,
        limit: u64,
    ) -> es_core::Result<Vec<Event>> {
        let prefix = key::sm_event_prefix(self.shard_id(), stream_id);
        let start = key::sm_event(self.shard_id(), stream_id, from);
        // 上界用前缀后继，避免 version == u64::MAX 被漏掉（设计文档 4.2）
        let end = match key::successor(&prefix) {
            Some(e) => e,
            None => return Ok(Vec::new()),
        };
        let raw = self.scan_values(start, end, false, limit)?;
        raw.iter().map(|v| decode_event(v)).collect()
    }

    /// 反向读取某流：从 `from` 开始按 version **递减**返回。
    ///
    /// `from` 传 `u64::MAX` 表示「从该流最新一条开始倒读」。
    pub fn read_stream_events_backward(
        &self,
        stream_id: &str,
        from: u64,
        limit: u64,
    ) -> es_core::Result<Vec<Event>> {
        let start = key::sm_event_prefix(self.shard_id(), stream_id);
        // 上界必须「包含 from 自身」，且 from 可能是 u64::MAX，
        // 此时不能用 successor（会进位越出本 stream 段），改用追加 0x00
        let end = key::upper_including(&key::sm_event(self.shard_id(), stream_id, from));
        let raw = self.scan_values(start, end, true, limit)?;
        raw.iter().map(|v| decode_event(v)).collect()
    }

    /// 读取分片内 $all 流：按 position（提交序）读取所有事件。
    ///
    /// 通过 position 指针（`sm_position_ptr`）定位到 (stream_id, version)，
    /// 再读取实际事件。这样能保证读取顺序与提交顺序严格一致。
    ///
    /// - `from_position`：起始 position（含）
    /// - `limit`：最多读取多少条，0 表示不限量
    pub fn read_all_events(&self, from_position: u64, limit: u64) -> es_core::Result<Vec<Event>> {
        let shard = self.shard_id();
        let start = key::sm_position_ptr(shard, from_position);
        let end = key::successor(&key::sm_position_prefix(shard))
            .ok_or_else(|| es_core::Error::Internal("position 指针区前缀无后继".into()))?;
        let ptrs = self.scan_values(start, end, false, limit)?;
        self.resolve_position_ptrs(&ptrs)
    }

    /// 反向读取分片内 $all 流：从 `from_position` 开始按 position **递减**返回。
    ///
    /// `from_position` 传 `u64::MAX` 表示「从该分片最新一条开始倒读」。
    pub fn read_all_events_backward(
        &self,
        from_position: u64,
        limit: u64,
    ) -> es_core::Result<Vec<Event>> {
        let shard = self.shard_id();
        let start = key::sm_position_prefix(shard);
        // 同 read_stream_events_backward：上界须含 from_position 自身，
        // 且它可能是 u64::MAX，故不能用 successor
        let end = key::upper_including(&key::sm_position_ptr(shard, from_position));
        let ptrs = self.scan_values(start, end, true, limit)?;
        self.resolve_position_ptrs(&ptrs)
    }

    /// 把 position 指针批量解析为实际事件。
    ///
    /// 指针内容是 `(stream_id, version)`，需再查一次事件本体。
    fn resolve_position_ptrs(&self, ptrs: &[Vec<u8>]) -> es_core::Result<Vec<Event>> {
        let shard = self.shard_id();
        let mut out = Vec::with_capacity(ptrs.len());
        for raw in ptrs {
            let (stream_id, version): (String, u64) = crate::encode::decode(raw)
                .map_err(|e| es_core::Error::Serde(format!("position 指针反序列化失败: {e}")))?;
            if let Some(bytes) = self.get(&key::sm_event(shard, &stream_id, version))? {
                out.push(decode_event(&bytes)?);
            }
        }
        Ok(out)
    }

    /// 枚举本分片内的全部流名与当前版本。
    ///
    /// 扫 StreamMeta 区即可，不需要遍历事件。
    pub fn list_streams(&self) -> es_core::Result<Vec<(String, StreamMeta)>> {
        let prefix = key::sm_stream_meta_prefix(self.shard_id());
        let end = key::successor(&prefix)
            .ok_or_else(|| es_core::Error::Internal("StreamMeta 区前缀无后继".into()))?;
        let kvs = self.scan_kv(prefix, end)?;

        let mut out = Vec::with_capacity(kvs.len());
        for (k, v) in kvs {
            let name = key::decode_stream_meta_key(&k)
                .ok_or_else(|| es_core::Error::Serde(format!("StreamMeta key 解码失败: {k:?}")))?;
            let meta: StreamMeta = crate::encode::decode(&v)
                .map_err(|e| es_core::Error::Serde(format!("StreamMeta 反序列化失败: {e}")))?;
            out.push((name, meta));
        }
        Ok(out)
    }

    /// 读取单个聚合实例的当前事件版本元数据。
    ///
    /// - `event_set`、`partition_id`、`aggregate_id`：完整实例定位。
    /// - 返回：实例不存在时为 `None`，否则返回当前聚合版本。
    /// - 错误：底层读取或反序列化失败。
    pub fn read_aggregate_meta(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateMeta>> {
        let key = key::sm_aggregate_meta(self.shard_id(), event_set, partition_id, aggregate_id);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateMeta 反序列化失败: {error}"))
                })
            })
            .transpose()
    }

    /// 按聚合版本读取单条聚合事件。
    pub fn read_aggregate_event(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        aggregate_id: &str,
        aggregate_version: u64,
    ) -> es_core::Result<Option<AggregateEvent>> {
        let key = key::sm_aggregate_event(
            self.shard_id(),
            event_set,
            partition_id,
            aggregate_id,
            aggregate_version,
        );
        self.get(&key)?
            .map(|bytes| decode_aggregate_event(&bytes))
            .transpose()
    }

    /// 顺序读取单个聚合实例的事件。
    ///
    /// `from_version` 为包含式起点，`limit == 0` 表示不限制数量。
    pub fn read_aggregate_events(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        aggregate_id: &str,
        from_version: u64,
        limit: u64,
    ) -> es_core::Result<Vec<AggregateEvent>> {
        let prefix =
            key::sm_aggregate_event_prefix(self.shard_id(), event_set, partition_id, aggregate_id);
        let start = key::sm_aggregate_event(
            self.shard_id(),
            event_set,
            partition_id,
            aggregate_id,
            from_version,
        );
        let Some(end) = key::successor(&prefix) else {
            return Ok(Vec::new());
        };
        self.scan_values(start, end, false, limit)?
            .iter()
            .map(|bytes| decode_aggregate_event(bytes))
            .collect()
    }

    /// 按服务端分配的分区位置读取一个虚拟事件分区。
    ///
    /// 事件集和分区由调用方从权威 catalog 获得；返回顺序仅在该分区内稳定。
    pub fn read_aggregate_partition_events(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        from_position: u64,
        limit: u64,
    ) -> es_core::Result<Vec<AggregateEvent>> {
        let shard = self.shard_id();
        let prefix = key::sm_aggregate_partition_index_prefix(shard, event_set, partition_id);
        let start =
            key::sm_aggregate_partition_index(shard, event_set, partition_id, from_position);
        let end = key::successor(&prefix)
            .ok_or_else(|| es_core::Error::Internal("聚合分区索引前缀无后继".into()))?;
        let pointers = self.scan_values(start, end, false, limit)?;
        let mut events = Vec::with_capacity(pointers.len());
        for pointer in pointers {
            let (aggregate_id, aggregate_version): (String, u64) = crate::encode::decode(&pointer)
                .map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区索引反序列化失败: {error}"))
                })?;
            let event = self
                .read_aggregate_event(
                    event_set,
                    partition_id,
                    &aggregate_id,
                    aggregate_version,
                )?
                .ok_or_else(|| {
                    es_core::Error::Storage(format!(
                        "聚合分区索引指向缺失事件: {event_set}/{partition_id}/{aggregate_id}/{aggregate_version}"
                    ))
                })?;
            events.push(event);
        }
        Ok(events)
    }

    /// 读取聚合实例的业务状态文档。
    pub fn read_aggregate_state(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateState>> {
        let key = key::sm_aggregate_state(self.shard_id(), event_set, partition_id, aggregate_id);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateState 反序列化失败: {error}"))
                })
            })
            .transpose()
    }

    /// 在同一存储快照中读取状态内容与最后提交时间。
    ///
    /// 旧状态缺少独立时间 key 时返回 Unix epoch 对应的零 HLC。
    pub fn read_aggregate_state_document(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateStateDocument>> {
        let state_key =
            key::sm_aggregate_state(self.shard_id(), event_set, partition_id, aggregate_id);
        let modified_key = key::sm_aggregate_state_modified(
            self.shard_id(),
            event_set,
            partition_id,
            aggregate_id,
        );
        let txn = self
            .tree()
            .begin()
            .map_err(|error| es_core::Error::Storage(format!("begin 失败: {error}")))?;
        let Some(state_bytes) = txn
            .get(state_key)
            .map_err(|error| es_core::Error::Storage(format!("读取聚合状态失败: {error}")))?
        else {
            return Ok(None);
        };
        let state: AggregateState = crate::encode::decode(&state_bytes).map_err(|error| {
            es_core::Error::Serde(format!("AggregateState 反序列化失败: {error}"))
        })?;
        let modified_hlc = txn
            .get(modified_key)
            .map_err(|error| es_core::Error::Storage(format!("读取状态修改时间失败: {error}")))?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("状态修改时间反序列化失败: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(Hlc {
                wall: 0,
                logical: 0,
            });
        Ok(Some(AggregateStateDocument {
            revision: state.revision,
            data: state.data,
            modified_hlc,
        }))
    }

    /// 按聚合实例 ID 词典序分页读取单个虚拟分区的状态元数据。
    ///
    /// - `after_aggregate_id`：排他起点；`None` 从分区首项开始。
    /// - `limit`：最大返回数，零表示不限量。
    /// - 返回：`(aggregate_id, state)`，严格按实例 ID 升序。
    /// - 错误：存储扫描、key 解码或状态反序列化失败。
    pub fn list_aggregate_partition_states(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        after_aggregate_id: Option<&str>,
        limit: u64,
    ) -> es_core::Result<Vec<(String, AggregateStateDocument)>> {
        let prefix = key::sm_aggregate_state_prefix(self.shard_id(), event_set, partition_id);
        let start = match after_aggregate_id {
            Some(aggregate_id) => key::upper_including(&key::sm_aggregate_state(
                self.shard_id(),
                event_set,
                partition_id,
                aggregate_id,
            )),
            None => prefix.clone(),
        };
        let end = key::successor(&prefix)
            .ok_or_else(|| es_core::Error::Internal("聚合状态前缀无后继".into()))?;
        let txn = self
            .tree()
            .begin()
            .map_err(|error| es_core::Error::Storage(format!("begin 失败: {error}")))?;
        let mut iterator = txn
            .range(start, end)
            .map_err(|error| es_core::Error::Storage(format!("range 失败: {error}")))?;
        let mut encoded_states = Vec::new();
        iterator
            .seek_first()
            .map_err(|error| es_core::Error::Storage(format!("seek_first 失败: {error}")))?;
        while iterator.valid() && (limit == 0 || encoded_states.len() < limit as usize) {
            let key_bytes = iterator.key().user_key().to_vec();
            let value = iterator
                .value()
                .map_err(|error| es_core::Error::Storage(format!("value 失败: {error}")))?
                .to_vec();
            encoded_states.push((key_bytes, value));
            if !iterator
                .next()
                .map_err(|error| es_core::Error::Storage(format!("next 失败: {error}")))?
            {
                break;
            }
        }
        drop(iterator);

        encoded_states
            .into_iter()
            .map(|(key_bytes, value)| {
                let aggregate_id =
                    key::decode_aggregate_state_key(&key_bytes).ok_or_else(|| {
                        es_core::Error::Serde(format!("聚合状态 key 损坏: {key_bytes:?}"))
                    })?;
                let state: AggregateState = crate::encode::decode(&value).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateState 反序列化失败: {error}"))
                })?;
                let modified_key = key::sm_aggregate_state_modified(
                    self.shard_id(),
                    event_set,
                    partition_id,
                    &aggregate_id,
                );
                let modified_hlc = txn
                    .get(modified_key)
                    .map_err(|error| {
                        es_core::Error::Storage(format!("读取状态修改时间失败: {error}"))
                    })?
                    .map(|bytes| {
                        crate::encode::decode(&bytes).map_err(|error| {
                            es_core::Error::Serde(format!("状态修改时间反序列化失败: {error}"))
                        })
                    })
                    .transpose()?
                    .unwrap_or(Hlc {
                        wall: 0,
                        logical: 0,
                    });
                Ok((
                    aggregate_id,
                    AggregateStateDocument {
                        revision: state.revision,
                        data: state.data,
                        modified_hlc,
                    },
                ))
            })
            .collect()
    }

    /// 读取虚拟事件分区的下一个可用位置，供 `Now` 游标捕获 head。
    pub fn read_aggregate_partition_head(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
    ) -> es_core::Result<u64> {
        let key = key::sm_aggregate_next_position(self.shard_id(), event_set, partition_id);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区 next_position 反序列化失败: {error}"))
                })
            })
            .transpose()
            .map(|position| position.unwrap_or(0))
    }

    /// 读取控制 Shard 上的聚合事件集 catalog；尚未创建时返回空 catalog。
    pub fn read_aggregate_catalog(&self) -> es_core::Result<AggregateCatalog> {
        let key = key::sm_aggregate_catalog(self.shard_id());
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateCatalog 反序列化失败: {error}"))
                })
            })
            .transpose()
            .map(|catalog| catalog.unwrap_or_default())
    }

    /// 读取控制 Shard 上的聚合消费者组 catalog；尚未创建时返回空 catalog。
    pub fn read_aggregate_group_catalog(&self) -> es_core::Result<AggregateGroupCatalog> {
        let key = key::sm_aggregate_group_catalog(self.shard_id());
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateGroupCatalog 反序列化失败: {error}"))
                })
            })
            .transpose()
            .map(|catalog| catalog.unwrap_or_default())
    }

    /// 读取数据 Shard 上单个组分区的 checkpoint、lease 与重试状态。
    pub fn read_aggregate_group_partition(
        &self,
        event_set: &EventSetId,
        partition_id: u16,
        group_name: &str,
    ) -> es_core::Result<Option<AggregateGroupPartition>> {
        let key =
            key::sm_aggregate_group_partition(self.shard_id(), event_set, partition_id, group_name);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateGroupPartition 反序列化失败: {error}"))
                })
            })
            .transpose()
    }
}

/// apply 过程中在单个事务内累积的写入。
///
/// 不直接写 txn 而先攒在这里的原因：同一批 entry 里可能有多条针对同一个 stream
/// 的 Append，后一条必须看到前一条的版本号。surrealkv 事务内的读不保证能看到
/// 同事务未提交的写，因此版本号与 next_position 用内存态串接。
/// 批内操作（有序）：put 与 delete 必须按 apply 顺序交错执行——
/// 同一 key 在同批先写后删（Append 后 DeleteStream）应删掉，先删后写应保留。
#[derive(Debug)]
enum ApplyOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

impl ApplyBatch {
    /// 压入 Delete：先移除批内同 key 的既有 Put（键级后操作覆盖——
    /// Delete 在后 ⇒ 前面的 Put 作废），保证「同批先写后删」语义，
    /// 且批内 Append 的写入（事件/指针/幂等索引）无需显式收集删除。
    fn push_delete(&mut self, key: Vec<u8>) {
        self.ops
            .retain(|op| !matches!(op, ApplyOp::Put(k, _) if *k == key));
        self.ops.push(ApplyOp::Delete(key));
    }
}

struct ApplyBatch {
    /// 待执行的有序操作
    ops: Vec<ApplyOp>,
    /// stream_id -> 该批次内的最新版本号（None 表示流仍不存在）
    stream_versions: std::collections::HashMap<String, Option<u64>>,
    /// 本批已删除的流（Delete 排队未提交时，存储里的旧 meta 会误导
    /// batch_stream_version——deleted 优先判定流不存在）
    deleted: std::collections::HashSet<String>,
    /// 分片内下一个可用 position
    next_position: u64,
    /// 本批次新产生的事件（用于 commit 后广播）
    new_events: Vec<Event>,
    /// 本批控制 Shard 的归属 catalog；首次使用时从存储加载。
    ownership_catalog: Option<es_core::OwnershipCatalog>,
    /// 本批已读取或写入的 Stream fencing 代次。
    ownership_fences: std::collections::HashMap<String, u64>,
    /// 本批已读取或修改的持久化订阅组；None 表示已删除或不存在。
    persistent_groups: std::collections::BTreeMap<String, Option<es_core::PersistentGroup>>,
    /// 本批已读取或修改的聚合实例当前版本。
    aggregate_versions: std::collections::HashMap<(EventSetId, u16, String), Option<u64>>,
    /// 本批各虚拟事件分区的下一个可用位置。
    aggregate_partition_positions: std::collections::HashMap<(EventSetId, u16), u64>,
    /// 本批已读取或修改的业务状态文档。
    aggregate_states: std::collections::HashMap<(EventSetId, u16, String), Option<AggregateState>>,
    /// 本批已读取或创建的聚合追加幂等记录。
    aggregate_idempotency:
        std::collections::HashMap<(EventSetId, u16, uuid::Uuid), AggregateIdempotencyRecord>,
    /// 本批已读取或安装的虚拟事件分区 fence。
    aggregate_partition_fences: std::collections::HashMap<(EventSetId, u16), u64>,
    /// 本批控制 Shard 的聚合事件集 catalog。
    aggregate_catalog: Option<AggregateCatalog>,
    /// 本批控制 Shard 的聚合消费者组 catalog。
    aggregate_group_catalog: Option<AggregateGroupCatalog>,
    /// 本批已读取或修改的聚合消费者组分区状态。
    aggregate_group_partitions:
        std::collections::HashMap<(EventSetId, u16, String), AggregateGroupPartition>,
    /// 本批新产生的聚合事件，事务提交后广播。
    new_aggregate_events: Vec<AggregateEvent>,
}

impl EsStorage {
    /// 读取持久化订阅组；仅控制 Shard 上的数据有权威意义。
    pub fn read_persistent_group(
        &self,
        name: &str,
    ) -> es_core::Result<Option<es_core::PersistentGroup>> {
        let key = key::sm_persistent_group(self.shard_id(), name);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("持久化订阅组反序列化失败: {error}"))
                })
            })
            .transpose()
    }

    /// 枚举全部持久化订阅组，按组名排序。
    pub fn list_persistent_groups(&self) -> es_core::Result<Vec<es_core::PersistentGroup>> {
        let prefix = key::sm_persistent_group_prefix(self.shard_id());
        let end = key::successor(&prefix)
            .ok_or_else(|| es_core::Error::Internal("持久化订阅 key 前缀无后继".into()))?;
        self.scan_kv(prefix, end)?
            .into_iter()
            .map(|(key_bytes, value)| {
                key::decode_persistent_group_key(&key_bytes).ok_or_else(|| {
                    es_core::Error::Serde(format!("持久化订阅 key 损坏: {key_bytes:?}"))
                })?;
                crate::encode::decode(&value).map_err(|error| {
                    es_core::Error::Serde(format!("持久化订阅组反序列化失败: {error}"))
                })
            })
            .collect()
    }

    fn batch_persistent_group(
        &self,
        batch: &mut ApplyBatch,
        name: &str,
    ) -> es_core::Result<Option<es_core::PersistentGroup>> {
        if let Some(group) = batch.persistent_groups.get(name) {
            return Ok(group.clone());
        }
        let group = self.read_persistent_group(name)?;
        batch
            .persistent_groups
            .insert(name.to_string(), group.clone());
        Ok(group)
    }

    fn persist_group(
        &self,
        batch: &mut ApplyBatch,
        name: &str,
        group: Option<es_core::PersistentGroup>,
    ) -> es_core::Result<()> {
        let key = key::sm_persistent_group(self.shard_id(), name);
        batch.ops.retain(|op| match op {
            ApplyOp::Put(existing, _) | ApplyOp::Delete(existing) => *existing != key,
        });
        match &group {
            Some(group) => {
                let bytes = crate::encode::encode(group).map_err(|error| {
                    es_core::Error::Serde(format!("持久化订阅组序列化失败: {error}"))
                })?;
                batch.ops.push(ApplyOp::Put(key, bytes));
            }
            None => batch.ops.push(ApplyOp::Delete(key)),
        }
        batch.persistent_groups.insert(name.to_string(), group);
        Ok(())
    }

    fn apply_persistent_subscription(
        &self,
        batch: &mut ApplyBatch,
        command: crate::PersistentSubscriptionCommand,
    ) -> es_core::Result<EsResponse> {
        use crate::{
            PersistentSubscriptionCommand as Command, PersistentSubscriptionResponse as R,
        };

        let response = match command {
            Command::Create { group } => {
                if let Some(existing) = self.batch_persistent_group(batch, &group.name)? {
                    R::Conflict {
                        actual_revision: existing.revision,
                    }
                } else {
                    let name = group.name.clone();
                    self.persist_group(batch, &name, Some(group.clone()))?;
                    R::Group(group)
                }
            }
            Command::Replace {
                name,
                expected_revision,
                group,
            } => match self.batch_persistent_group(batch, &name)? {
                None => R::NotFound,
                Some(existing) if existing.revision != expected_revision => R::Conflict {
                    actual_revision: existing.revision,
                },
                Some(_) if group.name != name => R::Invalid {
                    reason: "更新后的组名必须保持不变".into(),
                },
                Some(_) => {
                    self.persist_group(batch, &name, Some(group.clone()))?;
                    R::Group(group)
                }
            },
            Command::Delete {
                name,
                expected_revision,
            } => match self.batch_persistent_group(batch, &name)? {
                None => R::NotFound,
                Some(existing) if existing.revision != expected_revision => R::Conflict {
                    actual_revision: existing.revision,
                },
                Some(_) => {
                    self.persist_group(batch, &name, None)?;
                    R::Deleted
                }
            },
            Command::EnsureStreams { name, streams } => {
                let Some(mut group) = self.batch_persistent_group(batch, &name)? else {
                    return Ok(EsResponse::PersistentSubscription(R::NotFound));
                };
                group.ensure_streams(streams);
                self.persist_group(batch, &name, Some(group.clone()))?;
                R::Group(group)
            }
            Command::Claim {
                name,
                consumer_id,
                now_ms,
                deadline_ms,
                candidates,
            } => {
                let Some(mut group) = self.batch_persistent_group(batch, &name)? else {
                    return Ok(EsResponse::PersistentSubscription(R::NotFound));
                };
                let claimed = group.claim(&consumer_id, now_ms, deadline_ms, candidates);
                self.persist_group(batch, &name, Some(group))?;
                R::Claimed(claimed)
            }
            Command::Settle {
                name,
                consumer_id,
                group_epoch,
                now_ms,
                settlements,
            } => {
                let Some(mut group) = self.batch_persistent_group(batch, &name)? else {
                    return Ok(EsResponse::PersistentSubscription(R::NotFound));
                };
                let settled = group.settle(&consumer_id, group_epoch, now_ms, &settlements);
                self.persist_group(batch, &name, Some(group))?;
                R::Settled(settled)
            }
            Command::Expire { name, now_ms } => {
                let Some(mut group) = self.batch_persistent_group(batch, &name)? else {
                    return Ok(EsResponse::PersistentSubscription(R::NotFound));
                };
                let count = group.expire(now_ms) as u64;
                self.persist_group(batch, &name, Some(group))?;
                R::Count(count)
            }
            Command::ReplayParked { name, now_ms } => {
                let Some(mut group) = self.batch_persistent_group(batch, &name)? else {
                    return Ok(EsResponse::PersistentSubscription(R::NotFound));
                };
                let count = group.replay_parked(now_ms) as u64;
                self.persist_group(batch, &name, Some(group))?;
                R::Count(count)
            }
            Command::ReconcileOwnership { name, generations } => {
                let Some(mut group) = self.batch_persistent_group(batch, &name)? else {
                    return Ok(EsResponse::PersistentSubscription(R::NotFound));
                };
                group.reconcile_ownership(generations);
                self.persist_group(batch, &name, Some(group.clone()))?;
                R::Group(group)
            }
        };
        Ok(EsResponse::PersistentSubscription(response))
    }

    /// 取某流在本批次内的当前版本，优先用批内累积值
    fn batch_stream_version(
        &self,
        batch: &ApplyBatch,
        stream_id: &str,
    ) -> es_core::Result<Option<u64>> {
        // 本批已删除的流：即使存储里还有旧 meta（Delete 排队未提交），
        // 也判定为不存在（先删后写同批语义）
        if batch.deleted.contains(stream_id) {
            return Ok(None);
        }
        if let Some(v) = batch.stream_versions.get(stream_id) {
            return Ok(*v);
        }
        Ok(self.read_stream_meta(stream_id)?.map(|m| m.current_version))
    }

    /// 校验乐观并发期望版本。
    ///
    /// `current` 为 None 表示流不存在。返回 Err 表示冲突。
    fn check_expected_version(
        expected: ExpectedVersion,
        current: Option<u64>,
    ) -> std::result::Result<(), u64> {
        // 冲突时回报的「实际版本」：流不存在时用 0 表示
        let actual = current.unwrap_or(0);
        match expected {
            ExpectedVersion::Any => Ok(()),
            ExpectedVersion::NoStream => {
                if current.is_none() {
                    Ok(())
                } else {
                    Err(actual)
                }
            }
            ExpectedVersion::StreamExists => {
                if current.is_some() {
                    Ok(())
                } else {
                    Err(actual)
                }
            }
            ExpectedVersion::Exact(v) => match current {
                Some(cur) if cur == v => Ok(()),
                _ => Err(actual),
            },
        }
    }

    /// 处理 DeleteStream：同事务删除该流全部数据（事件、StreamMeta、
    /// 幂等索引、position 指针）。删除不存在的流 = no-op（幂等，迁移清尾可重跑）。
    ///
    /// position 指针在 apply 内同步扫描删除（DeleteStream 是低频操作，
    /// 全 ptr 区扫描可接受），保证与事件删除同一事务、崩溃一致；
    /// 不残留孤儿指针（即使残留，resolve_position_ptrs 对缺失事件静默跳过）。
    fn apply_delete(&self, batch: &mut ApplyBatch, stream_id: &str) -> es_core::Result<EsResponse> {
        let shard = self.shard_id();

        // 判据：本批已 Append 过（批内写入尚未提交，存储扫描不到）
        // 或存储中已有该流数据。批内 Append 的 Put 由后续 Delete op 覆盖
        // （同 key 后操作生效），无需显式收集其事件 key。
        let in_batch = batch.stream_versions.contains_key(stream_id);

        // 1. 扫描该流全部事件：收集事件 key 与 event_id（幂等索引删除用）。
        //    多删无害：幂等索引是加速索引，不存在的事件 id 删除是 no-op。
        let prefix = key::sm_event_prefix(shard, stream_id);
        let end = match key::successor(&prefix) {
            Some(e) => e,
            None => return Ok(EsResponse::DeleteOk),
        };
        let mut deleted_any = false;
        let mut idem_keys: Vec<Vec<u8>> = Vec::new();
        let mut event_keys: Vec<Vec<u8>> = Vec::new();
        for (k, v) in self.scan_kv(prefix, end)? {
            deleted_any = true;
            event_keys.push(k);
            if let Ok(ev) = decode_event(&v) {
                idem_keys.push(key::sm_idempotency(shard, &ev.event_id));
            }
        }
        // 批内 Append 的幂等索引 key（事件在 batch.new_events，存储还没有）
        for ev in &batch.new_events {
            if ev.stream_id == stream_id {
                idem_keys.push(key::sm_idempotency(shard, &ev.event_id));
            }
        }

        if !in_batch && !deleted_any {
            // 2a. 流不存在（批内外都没有）：no-op（幂等）
            batch.stream_versions.remove(stream_id);
            return Ok(EsResponse::DeleteOk);
        }

        // 2b. 扫描 position 指针区：删除指向该流的全部指针
        let pstart = key::sm_position_prefix(shard);
        let pend = match key::successor(&pstart) {
            Some(e) => e,
            None => return Ok(EsResponse::DeleteOk),
        };
        let mut ptr_keys: Vec<Vec<u8>> = Vec::new();
        for (k, v) in self.scan_kv(pstart, pend)? {
            if let Ok((s, _v)) = crate::encode::decode::<(String, u64)>(&v) {
                if s == stream_id {
                    ptr_keys.push(k);
                }
            }
        }
        // 批内 Put 的事件/指针 key（本批 Append 写入、存储中还没有）：
        // 事件值可解码为 Event，指针值为 (String, u64)
        for op in &batch.ops {
            if let ApplyOp::Put(k, v) = op {
                if let Ok(ev) = decode_event(v) {
                    if ev.stream_id == stream_id {
                        event_keys.push(k.clone());
                    }
                } else if let Ok((s, _)) = crate::encode::decode::<(String, u64)>(v) {
                    if s == stream_id {
                        ptr_keys.push(k.clone());
                    }
                }
            }
        }

        // 3. 删除全部（push_delete 键级覆盖：批内同 key 的 Put 一并作废）
        for k in event_keys {
            batch.push_delete(k);
        }
        for k in idem_keys {
            batch.push_delete(k);
        }
        for k in ptr_keys {
            batch.push_delete(k);
        }
        batch.push_delete(key::sm_stream_meta(shard, stream_id));
        // 批内版本缓存清掉：后续同批 Append 重新从存储读；
        // deleted 标记使 batch_stream_version 判定流不存在
        batch.stream_versions.remove(stream_id);
        batch.deleted.insert(stream_id.to_string());

        Ok(EsResponse::DeleteOk)
    }

    /// 处理一条 Append 请求，把写入累积到 batch。
    fn apply_append(
        &self,
        batch: &mut ApplyBatch,
        stream_id: &str,
        expected: ExpectedVersion,
        events: &[es_core::NewEvent],
        hlc: es_core::Hlc,
        ownership_generation: Option<u64>,
    ) -> es_core::Result<EsResponse> {
        let shard = self.shard_id();

        // 幂等去重：若首条事件的 event_id 已存在，说明整批已应用过。
        // 客户端重试（网络超时但实际已提交）会走到这里，必须返回原结果而非重复追加。
        if let Some(first) = events.first() {
            let idem_k = key::sm_idempotency(shard, &first.event_id);
            if let Some(bytes) = self.get(&idem_k)? {
                let (v0, p0): (u64, u64) = crate::encode::decode(&bytes)
                    .map_err(|e| es_core::Error::Serde(format!("幂等索引反序列化失败: {e}")))?;
                let n = events.len() as u64;
                return Ok(EsResponse::AppendOk {
                    next_expected_version: v0 + n - 1,
                    first_position: p0,
                    last_position: p0 + n - 1,
                });
            }
        }

        if let Some(generation) = ownership_generation {
            let current = self.batch_ownership_fence(batch, stream_id)?;
            if current != generation {
                return Ok(EsResponse::OwnershipFenced {
                    current_generation: current,
                });
            }
        }

        let current = self.batch_stream_version(batch, stream_id)?;

        // 乐观并发校验必须在此处：这里是 Raft group 的串行点
        if let Err(actual) = Self::check_expected_version(expected, current) {
            return Ok(EsResponse::OptimisticConflict {
                actual_version: actual,
            });
        }

        // 空事件列表：校验通过但无写入
        if events.is_empty() {
            let v = current.unwrap_or(0);
            return Ok(EsResponse::AppendOk {
                next_expected_version: v,
                first_position: batch.next_position,
                last_position: batch.next_position,
            });
        }

        // 版本号从 0 起：流不存在时首条为 0，否则为 current+1
        let mut version = match current {
            None => 0,
            Some(cur) => cur + 1,
        };
        let first_position = batch.next_position;

        for ev in events {
            let position = batch.next_position;
            let stored = Event {
                stream_id: stream_id.to_string(),
                version,
                event_id: ev.event_id,
                event_type: ev.event_type.clone(),
                data: ev.data.clone(),
                metadata: ev.metadata.clone(),
                hlc,
                position,
            };
            let ev_bytes = crate::encode::encode(&stored)
                .map_err(|e| es_core::Error::Serde(format!("Event 序列化失败: {e}")))?;
            batch.ops.push(ApplyOp::Put(
                key::sm_event(shard, stream_id, version),
                ev_bytes,
            ));

            // position 指针，供分片内 $all 流按提交序读取
            let ptr = crate::encode::encode(&(stream_id, version))
                .map_err(|e| es_core::Error::Serde(format!("position 指针序列化失败: {e}")))?;
            batch
                .ops
                .push(ApplyOp::Put(key::sm_position_ptr(shard, position), ptr));

            // 记录新事件（用于 commit 后广播）
            batch.new_events.push(stored);

            batch.next_position += 1;
            version += 1;
        }

        let last_version = version - 1;
        let last_position = batch.next_position - 1;

        // 幂等索引：以首条 event_id 记录 (起始版本, 起始位置)
        let idem_v = crate::encode::encode(&(
            match current {
                None => 0u64,
                Some(cur) => cur + 1,
            },
            first_position,
        ))
        .map_err(|e| es_core::Error::Serde(format!("幂等索引序列化失败: {e}")))?;
        batch.ops.push(ApplyOp::Put(
            key::sm_idempotency(shard, &events[0].event_id),
            idem_v,
        ));

        // StreamMeta
        let meta = StreamMeta {
            current_version: last_version,
        };
        let meta_bytes = crate::encode::encode(&meta)
            .map_err(|e| es_core::Error::Serde(format!("StreamMeta 序列化失败: {e}")))?;
        batch.ops.push(ApplyOp::Put(
            key::sm_stream_meta(shard, stream_id),
            meta_bytes,
        ));

        // 批内累积版本号，供同批后续 Append 看到；
        // 重建流（先删后写）清除 deleted 标记
        batch
            .stream_versions
            .insert(stream_id.to_string(), Some(last_version));
        batch.deleted.remove(stream_id);

        Ok(EsResponse::AppendOk {
            next_expected_version: last_version,
            first_position,
            last_position,
        })
    }

    fn batch_aggregate_version(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<u64>> {
        let identity = (event_set.clone(), partition_id, aggregate_id.to_string());
        if let Some(version) = batch.aggregate_versions.get(&identity) {
            return Ok(*version);
        }
        let version = self
            .read_aggregate_meta(event_set, partition_id, aggregate_id)?
            .map(|meta| meta.current_version);
        batch.aggregate_versions.insert(identity, version);
        Ok(version)
    }

    fn batch_aggregate_state(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateState>> {
        let identity = (event_set.clone(), partition_id, aggregate_id.to_string());
        if let Some(state) = batch.aggregate_states.get(&identity) {
            return Ok(state.clone());
        }
        let state = self.read_aggregate_state(event_set, partition_id, aggregate_id)?;
        batch.aggregate_states.insert(identity, state.clone());
        Ok(state)
    }

    fn batch_aggregate_partition_position(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
    ) -> es_core::Result<u64> {
        let identity = (event_set.clone(), partition_id);
        if let Some(position) = batch.aggregate_partition_positions.get(&identity) {
            return Ok(*position);
        }
        let key = key::sm_aggregate_next_position(self.shard_id(), event_set, partition_id);
        let position = self
            .get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区 next_position 反序列化失败: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(0);
        batch
            .aggregate_partition_positions
            .insert(identity, position);
        Ok(position)
    }

    fn batch_aggregate_partition_fence(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
    ) -> es_core::Result<u64> {
        let identity = (event_set.clone(), partition_id);
        if let Some(generation) = batch.aggregate_partition_fences.get(&identity) {
            return Ok(*generation);
        }
        let key = key::sm_aggregate_partition_fence(self.shard_id(), event_set, partition_id);
        let generation = self
            .get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区 fence 反序列化失败: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(0);
        batch
            .aggregate_partition_fences
            .insert(identity, generation);
        Ok(generation)
    }

    fn check_expected_aggregate_version(
        expected: ExpectedAggregateVersion,
        current: Option<u64>,
    ) -> bool {
        match expected {
            ExpectedAggregateVersion::Any => true,
            ExpectedAggregateVersion::NoAggregate => current.is_none(),
            ExpectedAggregateVersion::AggregateExists => current.is_some(),
            ExpectedAggregateVersion::Exact(expected) => current == Some(expected),
        }
    }

    fn apply_aggregate_append(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
        partition_generation: u64,
        aggregate_id: &str,
        expected_version: ExpectedAggregateVersion,
        event: &NewAggregateEvent,
        hlc: es_core::Hlc,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = event_set.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(error) = es_core::validate_aggregate_identifier("aggregate_id", aggregate_id) {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if event.event_type.is_empty() {
            return Ok(EsResponse::AggregateInvalid {
                reason: "event_type 不能为空".into(),
            });
        }
        let shard = self.shard_id();
        let idempotency_identity = (event_set.clone(), partition_id, event.event_id);
        let fingerprint =
            es_core::aggregate_append_fingerprint(event_set, aggregate_id, expected_version, event);
        let existing = if let Some(record) = batch.aggregate_idempotency.get(&idempotency_identity)
        {
            Some(record.clone())
        } else {
            let key =
                key::sm_aggregate_idempotency(shard, event_set, partition_id, &event.event_id);
            let record: Option<AggregateIdempotencyRecord> = self
                .get(&key)?
                .map(|bytes| {
                    crate::encode::decode(&bytes).map_err(|error| {
                        es_core::Error::Serde(format!("聚合事件幂等记录反序列化失败: {error}"))
                    })
                })
                .transpose()?;
            if let Some(record) = &record {
                batch
                    .aggregate_idempotency
                    .insert(idempotency_identity.clone(), record.clone());
            }
            record
        };
        if let Some(existing) = existing {
            return if existing.fingerprint == fingerprint {
                Ok(EsResponse::AggregateAppendOk {
                    aggregate_version: existing.result.aggregate_version,
                    partition_position: existing.result.partition_position,
                })
            } else {
                Ok(EsResponse::AggregateIdempotencyConflict)
            };
        }

        let current_fence = self.batch_aggregate_partition_fence(batch, event_set, partition_id)?;
        if current_fence != partition_generation {
            return Ok(EsResponse::AggregatePartitionFenced {
                current_generation: current_fence,
            });
        }

        let current = self.batch_aggregate_version(batch, event_set, partition_id, aggregate_id)?;
        if !Self::check_expected_aggregate_version(expected_version, current) {
            return Ok(EsResponse::AggregateOptimisticConflict {
                actual_version: current,
            });
        }
        let aggregate_version = match current {
            None => 0,
            Some(version) => match version.checked_add(1) {
                Some(next) => next,
                None => {
                    return Ok(EsResponse::AggregateInvalid {
                        reason: "聚合版本已耗尽".into(),
                    });
                }
            },
        };
        let partition_position =
            self.batch_aggregate_partition_position(batch, event_set, partition_id)?;
        let Some(next_partition_position) = partition_position.checked_add(1) else {
            return Ok(EsResponse::AggregateInvalid {
                reason: "分区位置已耗尽".into(),
            });
        };

        let stored = AggregateEvent {
            event_set: event_set.clone(),
            partition_id,
            aggregate_id: aggregate_id.to_string(),
            aggregate_version,
            event_id: event.event_id,
            event_type: event.event_type.clone(),
            data: event.data.clone(),
            metadata: event.metadata.clone(),
            hlc,
            partition_position,
        };
        let bytes = crate::encode::encode(&stored).map_err(|error| {
            es_core::Error::Serde(format!("AggregateEvent 序列化失败: {error}"))
        })?;
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_event(
                shard,
                event_set,
                partition_id,
                aggregate_id,
                aggregate_version,
            ),
            bytes,
        ));
        let pointer = crate::encode::encode(&(aggregate_id, aggregate_version))
            .map_err(|error| es_core::Error::Serde(format!("聚合分区索引序列化失败: {error}")))?;
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_partition_index(shard, event_set, partition_id, partition_position),
            pointer,
        ));
        let meta = AggregateMeta {
            current_version: aggregate_version,
        };
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_meta(shard, event_set, partition_id, aggregate_id),
            crate::encode::encode(&meta).map_err(|error| {
                es_core::Error::Serde(format!("AggregateMeta 序列化失败: {error}"))
            })?,
        ));
        let result = es_core::AggregateAppendResult {
            aggregate_version,
            partition_position,
        };
        let idempotency = AggregateIdempotencyRecord {
            fingerprint,
            result,
        };
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_idempotency(shard, event_set, partition_id, &event.event_id),
            crate::encode::encode(&idempotency).map_err(|error| {
                es_core::Error::Serde(format!("聚合事件幂等记录序列化失败: {error}"))
            })?,
        ));

        batch.aggregate_versions.insert(
            (event_set.clone(), partition_id, aggregate_id.to_string()),
            Some(aggregate_version),
        );
        batch
            .aggregate_partition_positions
            .insert((event_set.clone(), partition_id), next_partition_position);
        batch
            .aggregate_idempotency
            .insert(idempotency_identity, idempotency);
        batch.new_aggregate_events.push(stored);
        Ok(EsResponse::AggregateAppendOk {
            aggregate_version,
            partition_position,
        })
    }

    fn apply_put_aggregate_state(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
        partition_generation: u64,
        aggregate_id: &str,
        expected_revision: ExpectedStateRevision,
        data: &[u8],
        hlc: Hlc,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = event_set.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(error) = es_core::validate_aggregate_identifier("aggregate_id", aggregate_id) {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        let current_fence = self.batch_aggregate_partition_fence(batch, event_set, partition_id)?;
        if current_fence != partition_generation {
            return Ok(EsResponse::AggregatePartitionFenced {
                current_generation: current_fence,
            });
        }
        if self
            .batch_aggregate_version(batch, event_set, partition_id, aggregate_id)?
            .is_none()
        {
            return Ok(EsResponse::AggregateNotFound);
        }
        let current = self.batch_aggregate_state(batch, event_set, partition_id, aggregate_id)?;
        let matches = match expected_revision {
            ExpectedStateRevision::Absent => current.is_none(),
            ExpectedStateRevision::Exact(expected) => current
                .as_ref()
                .is_some_and(|state| state.revision == expected),
        };
        if !matches {
            return Ok(EsResponse::AggregateStateConflict {
                actual_revision: current.as_ref().map(|state| state.revision),
            });
        }
        let revision = match current {
            None => 0,
            Some(state) => match state.revision.checked_add(1) {
                Some(next) => next,
                None => {
                    return Ok(EsResponse::AggregateInvalid {
                        reason: "状态 revision 已耗尽".into(),
                    });
                }
            },
        };
        let state = AggregateState {
            revision,
            data: data.to_vec(),
        };
        let key = key::sm_aggregate_state(self.shard_id(), event_set, partition_id, aggregate_id);
        let bytes = crate::encode::encode(&state).map_err(|error| {
            es_core::Error::Serde(format!("AggregateState 序列化失败: {error}"))
        })?;
        batch.ops.push(ApplyOp::Put(key, bytes));
        let modified_key = key::sm_aggregate_state_modified(
            self.shard_id(),
            event_set,
            partition_id,
            aggregate_id,
        );
        let modified_bytes = crate::encode::encode(&hlc)
            .map_err(|error| es_core::Error::Serde(format!("状态修改时间序列化失败: {error}")))?;
        batch.ops.push(ApplyOp::Put(modified_key, modified_bytes));
        batch.aggregate_states.insert(
            (event_set.clone(), partition_id, aggregate_id.to_string()),
            Some(state.clone()),
        );
        Ok(EsResponse::AggregateStateStored {
            state: AggregateStateDocument {
                revision: state.revision,
                data: state.data,
                modified_hlc: hlc,
            },
        })
    }

    fn apply_aggregate_partition_fence(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
        generation: u64,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = event_set.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if generation == 0 {
            return Ok(EsResponse::AggregateInvalid {
                reason: "聚合分区 generation 必须大于 0".into(),
            });
        }
        let current = self.batch_aggregate_partition_fence(batch, event_set, partition_id)?;
        let installed = current.max(generation);
        if installed != current {
            let key = key::sm_aggregate_partition_fence(self.shard_id(), event_set, partition_id);
            let bytes = crate::encode::encode(&installed).map_err(|error| {
                es_core::Error::Serde(format!("聚合分区 fence 序列化失败: {error}"))
            })?;
            batch.ops.push(ApplyOp::Put(key, bytes));
            batch
                .aggregate_partition_fences
                .insert((event_set.clone(), partition_id), installed);
        }
        Ok(EsResponse::AggregatePartitionFenceInstalled {
            generation: installed,
        })
    }

    fn apply_aggregate_catalog_command(
        &self,
        batch: &mut ApplyBatch,
        command: es_core::AggregateCatalogCommand,
    ) -> es_core::Result<EsResponse> {
        if batch.aggregate_catalog.is_none() {
            batch.aggregate_catalog = Some(self.read_aggregate_catalog()?);
        }
        let catalog = batch.aggregate_catalog.as_mut().expect("上方已初始化");
        let applied = catalog.apply(command);
        let changed = matches!(
            &applied.outcome,
            es_core::AggregateCatalogOutcome::EventSet { changed: true, .. }
        );
        if changed {
            let bytes = crate::encode::encode(catalog).map_err(|error| {
                es_core::Error::Serde(format!("AggregateCatalog 序列化失败: {error}"))
            })?;
            batch.ops.push(ApplyOp::Put(
                key::sm_aggregate_catalog(self.shard_id()),
                bytes,
            ));
        }
        Ok(EsResponse::AggregateCatalogApplied(applied))
    }

    fn apply_aggregate_group_catalog_command(
        &self,
        batch: &mut ApplyBatch,
        command: es_core::AggregateGroupCatalogCommand,
    ) -> es_core::Result<EsResponse> {
        if batch.aggregate_group_catalog.is_none() {
            batch.aggregate_group_catalog = Some(self.read_aggregate_group_catalog()?);
        }
        let catalog = batch
            .aggregate_group_catalog
            .as_mut()
            .expect("上方已初始化");
        let previous_revision = catalog.revision;
        let applied = catalog.apply(command);
        if catalog.revision != previous_revision {
            let bytes = crate::encode::encode(catalog).map_err(|error| {
                es_core::Error::Serde(format!("AggregateGroupCatalog 序列化失败: {error}"))
            })?;
            batch.ops.push(ApplyOp::Put(
                key::sm_aggregate_group_catalog(self.shard_id()),
                bytes,
            ));
        }
        Ok(EsResponse::AggregateGroupCatalogApplied(applied))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_aggregate_group_partition(
        &self,
        batch: &mut ApplyBatch,
        event_set: &EventSetId,
        partition_id: u16,
        partition_generation: u64,
        group_name: &str,
        group_epoch: u64,
        start_position: u64,
        settings: &es_core::AggregateGroupSettings,
        command: crate::AggregateGroupPartitionCommand,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = event_set.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(error) = es_core::validate_aggregate_identifier("group_name", group_name) {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(reason) = settings.validate() {
            return Ok(EsResponse::AggregateInvalid { reason });
        }
        let current_fence = self.batch_aggregate_partition_fence(batch, event_set, partition_id)?;
        if current_fence != partition_generation {
            return Ok(EsResponse::AggregatePartitionFenced {
                current_generation: current_fence,
            });
        }
        let identity = (event_set.clone(), partition_id, group_name.to_string());
        let mut state = match batch.aggregate_group_partitions.get(&identity) {
            Some(state) => state.clone(),
            None => self
                .read_aggregate_group_partition(event_set, partition_id, group_name)?
                .unwrap_or_else(|| AggregateGroupPartition::new(group_epoch, start_position)),
        };
        if state.epoch > group_epoch {
            return Ok(EsResponse::AggregateGroupStaleEpoch {
                current_epoch: state.epoch,
            });
        }
        if state.epoch < group_epoch {
            state.reset(group_epoch, start_position);
        }
        let response = match command {
            crate::AggregateGroupPartitionCommand::Claim {
                consumer_id,
                now_ms,
                deadline_ms,
                max_claim,
                max_bytes,
                candidates,
            } => EsResponse::AggregateGroupClaimed(state.claim(
                &consumer_id,
                now_ms,
                deadline_ms,
                settings,
                max_claim,
                max_bytes,
                candidates,
            )),
            crate::AggregateGroupPartitionCommand::Settle {
                consumer_id,
                now_ms,
                settlements,
            } => EsResponse::AggregateGroupSettled(state.settle(
                &consumer_id,
                group_epoch,
                now_ms,
                settings,
                &settlements,
            )),
            crate::AggregateGroupPartitionCommand::Renew {
                consumer_id,
                deadline_ms,
                delivery_ids,
            } => EsResponse::AggregateGroupRenewed(state.renew(
                &consumer_id,
                group_epoch,
                deadline_ms,
                &delivery_ids,
            )),
            crate::AggregateGroupPartitionCommand::Expire { now_ms } => {
                EsResponse::AggregateGroupExpired(state.expire(now_ms, settings) as u64)
            }
        };
        let key =
            key::sm_aggregate_group_partition(self.shard_id(), event_set, partition_id, group_name);
        let bytes = crate::encode::encode(&state).map_err(|error| {
            es_core::Error::Serde(format!("AggregateGroupPartition 序列化失败: {error}"))
        })?;
        batch.ops.retain(|operation| match operation {
            ApplyOp::Put(existing, _) | ApplyOp::Delete(existing) => *existing != key,
        });
        batch.ops.push(ApplyOp::Put(key, bytes));
        batch.aggregate_group_partitions.insert(identity, state);
        Ok(response)
    }

    fn batch_ownership_fence(
        &self,
        batch: &mut ApplyBatch,
        stream_id: &str,
    ) -> es_core::Result<u64> {
        if let Some(generation) = batch.ownership_fences.get(stream_id) {
            return Ok(*generation);
        }
        let generation = match self.get(&key::sm_ownership_fence(self.shard_id(), stream_id))? {
            Some(bytes) => crate::encode::decode(&bytes)
                .map_err(|e| es_core::Error::Serde(format!("归属 fencing 反序列化失败: {e}")))?,
            None => 0,
        };
        batch
            .ownership_fences
            .insert(stream_id.to_string(), generation);
        Ok(generation)
    }

    fn apply_ownership_command(
        &self,
        batch: &mut ApplyBatch,
        command: es_core::OwnershipCommand,
    ) -> es_core::Result<EsResponse> {
        if batch.ownership_catalog.is_none() {
            let catalog = match self.get(&key::sm_ownership_catalog(self.shard_id()))? {
                Some(bytes) => crate::encode::decode(&bytes).map_err(|e| {
                    es_core::Error::Serde(format!("归属 catalog 反序列化失败: {e}"))
                })?,
                None => es_core::OwnershipCatalog::default(),
            };
            batch.ownership_catalog = Some(catalog);
        }
        let catalog = batch.ownership_catalog.as_mut().expect("上方已初始化");
        let applied = catalog.apply(command);
        let bytes = crate::encode::encode(catalog)
            .map_err(|e| es_core::Error::Serde(format!("归属 catalog 序列化失败: {e}")))?;
        batch.ops.push(ApplyOp::Put(
            key::sm_ownership_catalog(self.shard_id()),
            bytes,
        ));
        Ok(EsResponse::OwnershipApplied(applied))
    }

    fn apply_ownership_fence(
        &self,
        batch: &mut ApplyBatch,
        stream_id: &str,
        generation: u64,
    ) -> es_core::Result<EsResponse> {
        if stream_id.is_empty() || generation == 0 {
            return Err(es_core::Error::Internal(
                "归属 fencing 要求非空 Stream 且 generation > 0".into(),
            ));
        }
        let current = self.batch_ownership_fence(batch, stream_id)?;
        let installed = current.max(generation);
        if installed != current {
            let bytes = crate::encode::encode(&installed)
                .map_err(|e| es_core::Error::Serde(format!("归属 fencing 序列化失败: {e}")))?;
            batch.ops.push(ApplyOp::Put(
                key::sm_ownership_fence(self.shard_id(), stream_id),
                bytes,
            ));
            batch
                .ownership_fences
                .insert(stream_id.to_string(), installed);
        }
        Ok(EsResponse::OwnershipFenceInstalled {
            generation: installed,
        })
    }

    /// 本分片状态机区的 key 区间
    fn sm_range(&self) -> es_core::Result<(Vec<u8>, Vec<u8>)> {
        let mut start = Vec::with_capacity(10);
        start.push(0x02u8); // TAG_SM
        start.extend_from_slice(&self.shard_id().to_be_bytes());
        let end = key::successor(&start)
            .ok_or_else(|| es_core::Error::Internal("状态机区前缀无后继".into()))?;
        Ok((start, end))
    }

    /// 扫出本分片状态机区的全部 kv。
    ///
    /// 独立成同步函数：surrealkv 的迭代器不是 `Send`，不能跨 await 存活，
    /// 否则整个 async fn 的 future 失去 Send，无法满足 openraft 的 trait 约束。
    fn collect_sm_entries(&self) -> es_core::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (start, end) = self.sm_range()?;
        let txn = self
            .tree()
            .begin()
            .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| es_core::Error::Storage(format!("range 失败: {e}")))?;
        let mut out = Vec::new();
        it.seek_first()
            .map_err(|e| es_core::Error::Storage(format!("seek_first 失败: {e}")))?;
        while it.valid() {
            let k = it.key().user_key().to_vec();
            let v = it
                .value()
                .map_err(|e| es_core::Error::Storage(format!("value 失败: {e}")))?
                .to_vec();
            out.push((k, v));
            it.next()
                .map_err(|e| es_core::Error::Storage(format!("next 失败: {e}")))?;
        }
        Ok(out)
    }

    /// 读取分片内 next_position 计数器
    fn read_next_position(&self) -> es_core::Result<u64> {
        let k = key::sm_next_position(self.shard_id());
        match self.get(&k)? {
            None => Ok(0),
            Some(bytes) => crate::encode::decode(&bytes)
                .map_err(|e| es_core::Error::Serde(format!("next_position 反序列化失败: {e}"))),
        }
    }
}

impl RaftStateMachine<TypeConfig> for EsStorage {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let sm = self.sm_cache().read().await;
        Ok((sm.last_applied, sm.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<EsResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let shard = self.shard_id();
        let mut batch = ApplyBatch {
            ops: Vec::new(),
            stream_versions: std::collections::HashMap::new(),
            deleted: std::collections::HashSet::new(),
            next_position: self.read_next_position().map_err(sm_read_err)?,
            new_events: Vec::new(),
            ownership_catalog: None,
            ownership_fences: std::collections::HashMap::new(),
            persistent_groups: std::collections::BTreeMap::new(),
            aggregate_versions: std::collections::HashMap::new(),
            aggregate_partition_positions: std::collections::HashMap::new(),
            aggregate_states: std::collections::HashMap::new(),
            aggregate_idempotency: std::collections::HashMap::new(),
            aggregate_partition_fences: std::collections::HashMap::new(),
            aggregate_catalog: None,
            aggregate_group_catalog: None,
            aggregate_group_partitions: std::collections::HashMap::new(),
            new_aggregate_events: Vec::new(),
        };

        let mut responses = Vec::new();
        let mut last_applied = None;
        let mut membership: Option<StoredMembership<u64, openraft::BasicNode>> = None;

        for entry in entries {
            last_applied = Some(entry.log_id);

            match entry.payload {
                // Blank 是 leader 上任时的空日志，仅推进 last_applied
                EntryPayload::Blank => {
                    responses.push(EsResponse::AppendOk {
                        next_expected_version: 0,
                        first_position: 0,
                        last_position: 0,
                    });
                }
                EntryPayload::Normal(ref req) => match req {
                    EsRequest::Append {
                        stream_id,
                        expected_version,
                        events,
                        hlc,
                    } => {
                        let resp = self
                            .apply_append(
                                &mut batch,
                                stream_id,
                                *expected_version,
                                events,
                                *hlc,
                                None,
                            )
                            .map_err(sm_write_err)?;
                        responses.push(resp);
                    }
                    EsRequest::AppendOwned {
                        stream_id,
                        ownership_generation,
                        expected_version,
                        events,
                        hlc,
                    } => {
                        let resp = self
                            .apply_append(
                                &mut batch,
                                stream_id,
                                *expected_version,
                                events,
                                *hlc,
                                Some(*ownership_generation),
                            )
                            .map_err(sm_write_err)?;
                        responses.push(resp);
                    }
                    EsRequest::DeleteStream { stream_id } => {
                        let resp = self
                            .apply_delete(&mut batch, stream_id)
                            .map_err(sm_write_err)?;
                        responses.push(resp);
                    }
                    EsRequest::CommitOwnership { command } => {
                        let resp = self
                            .apply_ownership_command(&mut batch, command.clone())
                            .map_err(sm_write_err)?;
                        responses.push(resp);
                    }
                    EsRequest::InstallOwnershipFence {
                        stream_id,
                        generation,
                    } => {
                        let resp = self
                            .apply_ownership_fence(&mut batch, stream_id, *generation)
                            .map_err(sm_write_err)?;
                        responses.push(resp);
                    }
                    EsRequest::PersistentSubscription { command } => {
                        let resp = self
                            .apply_persistent_subscription(&mut batch, command.clone())
                            .map_err(sm_write_err)?;
                        responses.push(resp);
                    }
                    EsRequest::AggregateAppend {
                        event_set,
                        partition_id,
                        partition_generation,
                        aggregate_id,
                        expected_version,
                        event,
                        hlc,
                    } => {
                        let response = self
                            .apply_aggregate_append(
                                &mut batch,
                                event_set,
                                *partition_id,
                                *partition_generation,
                                aggregate_id,
                                *expected_version,
                                event,
                                *hlc,
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::PutAggregateState {
                        event_set,
                        partition_id,
                        partition_generation,
                        aggregate_id,
                        expected_revision,
                        data,
                        hlc,
                    } => {
                        let response = self
                            .apply_put_aggregate_state(
                                &mut batch,
                                event_set,
                                *partition_id,
                                *partition_generation,
                                aggregate_id,
                                *expected_revision,
                                data,
                                *hlc,
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::InstallAggregatePartitionFence {
                        event_set,
                        partition_id,
                        generation,
                    } => {
                        let response = self
                            .apply_aggregate_partition_fence(
                                &mut batch,
                                event_set,
                                *partition_id,
                                *generation,
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::CommitAggregateCatalog { command } => {
                        let response = self
                            .apply_aggregate_catalog_command(&mut batch, command.clone())
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::CommitAggregateGroupCatalog { command } => {
                        let response = self
                            .apply_aggregate_group_catalog_command(&mut batch, command.clone())
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::AggregateGroupPartition {
                        event_set,
                        partition_id,
                        partition_generation,
                        group_name,
                        group_epoch,
                        start_position,
                        settings,
                        command,
                    } => {
                        let response = self
                            .apply_aggregate_group_partition(
                                &mut batch,
                                event_set,
                                *partition_id,
                                *partition_generation,
                                group_name,
                                *group_epoch,
                                *start_position,
                                settings,
                                command.clone(),
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    membership = Some(StoredMembership::new(Some(entry.log_id), mem.clone()));
                    responses.push(EsResponse::AppendOk {
                        next_expected_version: 0,
                        first_position: 0,
                        last_position: 0,
                    });
                }
            }
        }

        // next_position 计数器
        let np = crate::encode::encode(&batch.next_position).map_err(sm_write_err)?;
        batch
            .ops
            .push(ApplyOp::Put(key::sm_next_position(shard), np));

        for ((event_set, partition_id), next_position) in &batch.aggregate_partition_positions {
            let bytes = crate::encode::encode(next_position).map_err(sm_write_err)?;
            batch.ops.push(ApplyOp::Put(
                key::sm_aggregate_next_position(shard, event_set, *partition_id),
                bytes,
            ));
        }

        // 已应用状态：与业务数据同事务提交，保证重启后 last_applied 与数据一致
        let mut cache = self.sm_cache().write().await;
        let new_last_applied = last_applied.or(cache.last_applied);
        let new_membership = membership
            .clone()
            .unwrap_or_else(|| cache.membership.clone());
        let applied = AppliedState {
            last_applied: new_last_applied,
            membership: new_membership.clone(),
        };
        let applied_bytes = crate::encode::encode(&applied).map_err(sm_write_err)?;
        batch
            .ops
            .push(ApplyOp::Put(key::sm_applied_state(shard), applied_bytes));

        // 单事务按序提交全部操作（put/delete 交错，保批内语义）
        let mut txn = self.tree().begin().map_err(sm_write_err)?;
        for op in &batch.ops {
            match op {
                ApplyOp::Put(k, v) => txn.set(k.clone(), v.clone()).map_err(sm_write_err)?,
                ApplyOp::Delete(k) => txn.delete(k.clone()).map_err(sm_write_err)?,
            }
        }
        txn.commit().await.map_err(sm_write_err)?;

        // 提交成功后才更新内存缓存，失败时缓存不被污染
        cache.last_applied = new_last_applied;
        cache.membership = new_membership;

        // 广播新事件（供 Subscribe 订阅）
        for event in batch.new_events {
            // 忽略发送错误（无订阅者时 send 返回 Err，正常情况）
            let _ = self.event_tx().send(event);
        }
        for event in batch.new_aggregate_events {
            let _ = self.aggregate_event_tx().send(event);
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<u64>>
    {
        // 传输数据写入 incoming 临时文件而非内存：大快照分块到达时逐块落盘
        let store = self.snapshot_store();
        store.ensure_dirs().map_err(sm_write_err)?;
        let sf = snapshot::SnapshotFile::create_temp(&store.incoming_dir())
            .await
            .map_err(|e| sm_write_err(&e))?;
        Ok(Box::new(sf))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> std::result::Result<(), StorageError<u64>> {
        // 关 tokio 句柄转 std 句柄：安装是同步段（解压流是 std::io::Read），
        // 且不再持有传输句柄后可对临时文件做转正 rename。
        // 注意：此处返回的 path/temp 已从 SnapshotFile 中取出（mem::take），
        // 原对象 Drop 时不会删除已转正的文件。
        let mut snapshot = snapshot;
        // 防御：shutdown 刷出 tokio File 内部缓冲（tokio 1.53 的 File 写有
        // 用户态缓冲，write_all 返回不代表落盘）。openraft Chunked 在传输
        // 完成时已调用 shutdown（snapshot_transport.rs done 分支），此处
        // 幂等兜底，防未来链路变化丢数据。
        {
            use tokio::io::AsyncWriteExt as _;
            snapshot.shutdown().await.map_err(sm_write_err)?;
        }
        let (mut file, path, is_temp) = snapshot.into_std_file().map_err(sm_read_err)?;
        // RAII 兜底：错误路径（读/解压/提交失败）删除残留的 temp 文件。
        // 成功后由转正 rename 接管（guard 置 None），否则文件常驻磁盘
        // 直到下次启动清理。
        struct TempGuard(Option<std::path::PathBuf>);
        impl Drop for TempGuard {
            fn drop(&mut self) {
                if let Some(p) = &self.0 {
                    let _ = std::fs::remove_file(p);
                }
            }
        }
        let mut temp_guard = TempGuard(if is_temp { Some(path.clone()) } else { None });

        // 头部校验：magic/version/压缩 tag 不符即报错；snapshot_id 必须与
        // openraft 请求一致（防错文件被当作目标快照安装）
        let (header, file_meta) = snapshot::read_header(&mut file).map_err(sm_read_err)?;
        if file_meta.snapshot_id != meta.snapshot_id {
            return Err(sm_read_err(&std::io::Error::other(format!(
                "快照文件 snapshot_id 不一致：文件 {} vs 请求 {}",
                file_meta.snapshot_id, meta.snapshot_id
            ))));
        }

        // 定位到 payload 起点（头部与 meta 未压缩）
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(32 + header.meta_len))
            .map_err(sm_read_err)?;
        let mut reader = header.compression.reader(file).map_err(sm_read_err)?;

        let shard = self.shard_id();

        // 先清掉本分片状态机区的全部现有数据，再灌入快照内容。
        // 不清空会残留快照里已不存在的 key（例如被 purge 掉的事件）。
        let (sm_start, sm_end) = self.sm_range().map_err(sm_write_err)?;
        let old_keys = self.collect_keys(sm_start, sm_end).map_err(sm_write_err)?;

        let mut cache = self.sm_cache().write().await;

        let mut txn = self.tree().begin().map_err(sm_write_err)?;
        for k in &old_keys {
            txn.delete(k.clone()).map_err(sm_write_err)?;
        }
        // 流式解压逐条灌入（不把整个快照载入内存；txn 缓冲上限见 docs/snapshot.md）
        let read_bytes = snapshot::for_each_record(&mut reader, |k, v| {
            txn.set(k, v)
                .map_err(|e| std::io::Error::other(format!("set 失败: {e}")))?;
            Ok(())
        })
        .map_err(sm_read_err)?;
        if read_bytes != header.payload_len {
            return Err(sm_read_err(&std::io::Error::other(format!(
                "快照 payload 长度不符：实读 {read_bytes} vs 声明 {}",
                header.payload_len
            ))));
        }
        // 已应用状态随快照一起写，保持原子
        let applied = AppliedState {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
        };
        txn.set(
            key::sm_applied_state(shard),
            crate::encode::encode(&applied).map_err(sm_write_err)?,
        )
        .map_err(sm_write_err)?;
        txn.commit().await.map_err(sm_write_err)?;

        cache.last_applied = meta.last_log_id;
        cache.membership = meta.last_membership.clone();

        // 转正：incoming 临时文件 → 规范名。先提交后转正：提交后转正失败
        // 只损失文件缓存（SM 数据已就位）；反过来会留下"文件在而数据不在"。
        if is_temp {
            let final_path = self.snapshot_store().final_path(meta.last_log_id);
            if let Err(e) = std::fs::rename(&path, &final_path) {
                tracing::warn!("快照文件转正失败（仅损失文件缓存，SM 数据已提交）: {e}");
            } else {
                temp_guard.0 = None; // 转正成功，guard 不再删除
            }
        } else {
            temp_guard.0 = None;
        }
        // 保留清理：装快照也计入历史保留
        let store = self.snapshot_store();
        if let Err(e) = store.retain(store.keep()) {
            tracing::warn!("快照保留清理失败: {e}");
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let store = self.snapshot_store();
        // 过滤领先于 applied 的快照：restore/崩溃残留的更新文件与状态机不一致
        let applied = self.sm_cache().read().await.last_applied;
        let Some(path) = store.latest(applied).map_err(sm_read_err)? else {
            return Ok(None);
        };
        // 读文件头取该文件自己的完整 meta（含 last_membership，供 follower 安装）
        let mut f = std::fs::File::open(&path).map_err(sm_read_err)?;
        let (_, meta) = snapshot::read_header(&mut f).map_err(sm_read_err)?;
        let sf = snapshot::SnapshotFile::open(path)
            .await
            .map_err(|e| sm_read_err(&e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(sf),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for EsStorage {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let shard = self.shard_id();

        // 在同一个锁作用域内取元数据与数据，避免与并发 apply 交错产生撕裂快照。
        // 迭代器在这个块内创建并销毁，不跨 await。
        let (last_applied, membership, entries) = {
            let cache = self.sm_cache().read().await;
            let entries = self.collect_sm_entries().map_err(sm_read_err)?;
            (cache.last_applied, cache.membership.clone(), entries)
        };

        // snapshot_id 需在同一分片内唯一且可比较。用 last_applied 的
        // term/index 拼接，避免依赖墙上时钟（Date::now 在确定性回放里不可用）。
        let snapshot_id = match last_applied {
            Some(l) => format!("{}-{}-{}", shard, l.leader_id.term, l.index),
            None => format!("{shard}-empty"),
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        // 写临时文件 → 原子 rename 转正：崩溃不会留下半写的正式快照文件。
        // 头部与 meta 未压缩（裸文件写入），payload 按配置压缩（docs/snapshot.md）。
        let store = self.snapshot_store();
        store.ensure_dirs().map_err(sm_write_err)?;
        let tmp = store.tmp_path();
        let header = snapshot::SnapshotHeader {
            version: snapshot::SNAP_VERSION,
            compression: store.compression(),
            shard_id: shard,
            // meta_len 必须与实际写 header 的编码一致（write_header 保留 serde_json）
            meta_len: serde_json::to_vec(&meta).map_err(sm_write_err)?.len() as u64,
            payload_len: snapshot::payload_len_for(&entries),
        };
        // 写段失败时清理 tmp（不调 finish 的 zstd Encoder Drop 不补帧尾，
        // 残留的半写文件由本处删除与启动清理兜底）
        let write_result = (|| -> std::result::Result<(), StorageError<u64>> {
            let mut f = std::fs::File::create(&tmp).map_err(sm_write_err)?;
            snapshot::write_header(&mut f, &header, &meta).map_err(sm_write_err)?;
            let mut w = store.compression().writer(f).map_err(sm_write_err)?;
            w.write_all(&(entries.len() as u64).to_le_bytes())
                .map_err(sm_write_err)?;
            for (k, v) in &entries {
                snapshot::write_record(&mut w, k, v).map_err(sm_write_err)?;
            }
            snapshot::write_end_marker(&mut w).map_err(sm_write_err)?;
            w.finish().map_err(sm_write_err)?;
            Ok(())
        })();
        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        let final_path = store.final_path(last_applied);
        std::fs::rename(&tmp, &final_path).map_err(sm_write_err)?;

        // 保留清理：删除超出 keep 的旧快照
        let removed = store.retain(store.keep()).map_err(sm_write_err)?;
        for p in &removed {
            tracing::info!("快照保留策略删除旧快照: {}", p.display());
        }

        let sf = snapshot::SnapshotFile::open(final_path)
            .await
            .map_err(|e| sm_write_err(&e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(sf),
        })
    }
}
