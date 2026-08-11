//! RaftStateMachine trait 实现。
//!
//! apply 的核心约束（设计文档 5.3）：
//! 1. `expected_version` 校验必须在 apply 内做，这里是单个 Raft group 的串行执行点，
//!    只有在此处「读当前版本 → 比对 → 写入」才是原子的
//! 2. 事件、StreamMeta、position 指针、next_position、幂等索引、last_applied
//!    六者必须同一个 surrealkv 事务提交，否则崩溃会留下版本号回退等不一致状态
//! 3. 因为在 apply 内持久化状态，快照不要求落盘即可保证正确性

use std::io::Cursor;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use surrealkv::LSMIterator;

use super::EsStorage;
use crate::key;
use crate::raft_type::TypeConfig;
use crate::{EsRequest, EsResponse};
use es_core::{Event, ExpectedVersion, StreamMeta};

/// 快照载荷：状态机的完整可序列化形态
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SnapshotPayload {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, openraft::BasicNode>,
    /// 该分片状态机区的全部 kv（key 与 value 均为原始字节）
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

fn sm_read_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::read_state_machine(&std::io::Error::other(e.to_string())).into()
}

fn sm_write_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::write_state_machine(&std::io::Error::other(e.to_string())).into()
}

/// 反序列化事件
fn decode_event(bytes: &[u8]) -> es_core::Result<Event> {
    serde_json::from_slice(bytes)
        .map_err(|e| es_core::Error::Serde(format!("Event 反序列化失败: {e}")))
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
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct AppliedState {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, openraft::BasicNode>,
}

impl EsStorage {
    /// 从持久化状态恢复 last_applied 与 membership。
    ///
    /// openraft 在启动时调用 `applied_state`，必须返回真实落盘的值，
    /// 否则会从错误的位置重放日志。**必须在第一次调用 `apply` 前调用。**
    pub async fn restore_applied_state(&self) -> es_core::Result<()> {
        let k = key::sm_applied_state(self.shard_id());
        if let Some(bytes) = self.get(&k)? {
            let st: AppliedState = serde_json::from_slice(&bytes)
                .map_err(|e| es_core::Error::Serde(format!("已应用状态反序列化失败: {e}")))?;
            let mut cache = self.sm_cache().write().await;
            cache.last_applied = st.last_applied;
            cache.membership = st.membership;
        }
        Ok(())
    }

    /// 读取流当前元数据。流不存在时返回 None。
    pub fn read_stream_meta(&self, stream_id: &str) -> es_core::Result<Option<StreamMeta>> {
        let k = key::sm_stream_meta(self.shard_id(), stream_id);
        match self.get(&k)? {
            None => Ok(None),
            Some(bytes) => {
                let meta: StreamMeta = serde_json::from_slice(&bytes)
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
                let ev: Event = serde_json::from_slice(&bytes)
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
    pub fn read_all_events(
        &self,
        from_position: u64,
        limit: u64,
    ) -> es_core::Result<Vec<Event>> {
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
            let (stream_id, version): (String, u64) = serde_json::from_slice(raw)
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
            let name = key::decode_stream_meta_key(&k).ok_or_else(|| {
                es_core::Error::Serde(format!("StreamMeta key 解码失败: {k:?}"))
            })?;
            let meta: StreamMeta = serde_json::from_slice(&v)
                .map_err(|e| es_core::Error::Serde(format!("StreamMeta 反序列化失败: {e}")))?;
            out.push((name, meta));
        }
        Ok(out)
    }
}

/// apply 过程中在单个事务内累积的写入。
///
/// 不直接写 txn 而先攒在这里的原因：同一批 entry 里可能有多条针对同一个 stream
/// 的 Append，后一条必须看到前一条的版本号。surrealkv 事务内的读不保证能看到
/// 同事务未提交的写，因此版本号与 next_position 用内存态串接。
struct ApplyBatch {
    /// 待写入的 kv
    puts: Vec<(Vec<u8>, Vec<u8>)>,
    /// stream_id -> 该批次内的最新版本号（None 表示流仍不存在）
    stream_versions: std::collections::HashMap<String, Option<u64>>,
    /// 分片内下一个可用 position
    next_position: u64,
    /// 本批次新产生的事件（用于 commit 后广播）
    new_events: Vec<Event>,
}

impl EsStorage {
    /// 取某流在本批次内的当前版本，优先用批内累积值
    fn batch_stream_version(
        &self,
        batch: &ApplyBatch,
        stream_id: &str,
    ) -> es_core::Result<Option<u64>> {
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

    /// 处理一条 Append 请求，把写入累积到 batch。
    fn apply_append(
        &self,
        batch: &mut ApplyBatch,
        stream_id: &str,
        expected: ExpectedVersion,
        events: &[es_core::NewEvent],
        hlc: es_core::Hlc,
    ) -> es_core::Result<EsResponse> {
        let shard = self.shard_id();

        // 幂等去重：若首条事件的 event_id 已存在，说明整批已应用过。
        // 客户端重试（网络超时但实际已提交）会走到这里，必须返回原结果而非重复追加。
        if let Some(first) = events.first() {
            let idem_k = key::sm_idempotency(shard, &first.event_id);
            if let Some(bytes) = self.get(&idem_k)? {
                let (v0, p0): (u64, u64) = serde_json::from_slice(&bytes).map_err(|e| {
                    es_core::Error::Serde(format!("幂等索引反序列化失败: {e}"))
                })?;
                let n = events.len() as u64;
                return Ok(EsResponse::AppendOk {
                    next_expected_version: v0 + n - 1,
                    first_position: p0,
                    last_position: p0 + n - 1,
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
            let ev_bytes = serde_json::to_vec(&stored)
                .map_err(|e| es_core::Error::Serde(format!("Event 序列化失败: {e}")))?;
            batch
                .puts
                .push((key::sm_event(shard, stream_id, version), ev_bytes));

            // position 指针，供分片内 $all 流按提交序读取
            let ptr = serde_json::to_vec(&(stream_id, version))
                .map_err(|e| es_core::Error::Serde(format!("position 指针序列化失败: {e}")))?;
            batch
                .puts
                .push((key::sm_position_ptr(shard, position), ptr));

            // 记录新事件（用于 commit 后广播）
            batch.new_events.push(stored);

            batch.next_position += 1;
            version += 1;
        }

        let last_version = version - 1;
        let last_position = batch.next_position - 1;

        // 幂等索引：以首条 event_id 记录 (起始版本, 起始位置)
        let idem_v = serde_json::to_vec(&(
            match current {
                None => 0u64,
                Some(cur) => cur + 1,
            },
            first_position,
        ))
        .map_err(|e| es_core::Error::Serde(format!("幂等索引序列化失败: {e}")))?;
        batch.puts.push((
            key::sm_idempotency(shard, &events[0].event_id),
            idem_v,
        ));

        // StreamMeta
        let meta = StreamMeta {
            current_version: last_version,
        };
        let meta_bytes = serde_json::to_vec(&meta)
            .map_err(|e| es_core::Error::Serde(format!("StreamMeta 序列化失败: {e}")))?;
        batch
            .puts
            .push((key::sm_stream_meta(shard, stream_id), meta_bytes));

        // 批内累积版本号，供同批后续 Append 看到
        batch
            .stream_versions
            .insert(stream_id.to_string(), Some(last_version));

        Ok(EsResponse::AppendOk {
            next_expected_version: last_version,
            first_position,
            last_position,
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
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| es_core::Error::Serde(format!("next_position 反序列化失败: {e}"))),
        }
    }
}

impl RaftStateMachine<TypeConfig> for EsStorage {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, openraft::BasicNode>),
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
            puts: Vec::new(),
            stream_versions: std::collections::HashMap::new(),
            next_position: self.read_next_position().map_err(sm_read_err)?,
            new_events: Vec::new(),
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
                            .apply_append(&mut batch, stream_id, *expected_version, events, *hlc)
                            .map_err(sm_write_err)?;
                        responses.push(resp);
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
        let np = serde_json::to_vec(&batch.next_position).map_err(sm_write_err)?;
        batch.puts.push((key::sm_next_position(shard), np));

        // 已应用状态：与业务数据同事务提交，保证重启后 last_applied 与数据一致
        let mut cache = self.sm_cache().write().await;
        let new_last_applied = last_applied.or(cache.last_applied);
        let new_membership = membership.clone().unwrap_or_else(|| cache.membership.clone());
        let applied = AppliedState {
            last_applied: new_last_applied,
            membership: new_membership.clone(),
        };
        let applied_bytes = serde_json::to_vec(&applied).map_err(sm_write_err)?;
        batch
            .puts
            .push((key::sm_applied_state(shard), applied_bytes));

        // 单事务提交全部写入
        let mut txn = self.tree().begin().map_err(sm_write_err)?;
        for (k, v) in &batch.puts {
            txn.set(k.clone(), v.clone()).map_err(sm_write_err)?;
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

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<u64>>
    {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let payload: SnapshotPayload = serde_json::from_slice(&bytes).map_err(|e| {
            StorageIOError::read_snapshot(Some(meta.signature()), &std::io::Error::other(e.to_string()))
        })?;

        let shard = self.shard_id();

        // 先清掉本分片状态机区的全部现有数据，再灌入快照内容。
        // 不清空会残留快照里已不存在的 key（例如被 purge 掉的事件）。
        let (sm_start, sm_end) = self.sm_range().map_err(sm_write_err)?;
        let old_keys = self
            .collect_keys(sm_start, sm_end)
            .map_err(sm_write_err)?;

        let mut cache = self.sm_cache().write().await;

        let mut txn = self.tree().begin().map_err(sm_write_err)?;
        for k in &old_keys {
            txn.delete(k.clone()).map_err(sm_write_err)?;
        }
        for (k, v) in &payload.entries {
            txn.set(k.clone(), v.clone()).map_err(sm_write_err)?;
        }
        // 已应用状态随快照一起写，保持原子
        let applied = AppliedState {
            last_applied: payload.last_applied,
            membership: payload.membership.clone(),
        };
        txn.set(
            key::sm_applied_state(shard),
            serde_json::to_vec(&applied).map_err(sm_write_err)?,
        )
        .map_err(sm_write_err)?;
        txn.commit().await.map_err(sm_write_err)?;

        cache.last_applied = payload.last_applied;
        cache.membership = payload.membership;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let k = key::snapshot_current(self.shard_id());
        match self.get(&k).map_err(sm_read_err)? {
            None => Ok(None),
            Some(bytes) => {
                let (meta, data): (SnapshotMeta<u64, openraft::BasicNode>, Vec<u8>) =
                    serde_json::from_slice(&bytes).map_err(sm_read_err)?;
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
        }
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

        let payload = SnapshotPayload {
            last_applied,
            membership: membership.clone(),
            entries,
        };
        let data = serde_json::to_vec(&payload).map_err(sm_read_err)?;

        // snapshot_id 需在同一分片内唯一且可比较。用 last_applied 的
        // term/index 拼接，避免依赖墙上时钟（Date::now 在确定性回放里不可用）。
        let snapshot_id = match last_applied {
            Some(l) => format!("{}-{}-{}", shard, l.leader_id, l.index),
            None => format!("{shard}-empty"),
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        // 落盘当前快照，供 get_current_snapshot 返回
        let stored = serde_json::to_vec(&(&meta, &data)).map_err(sm_write_err)?;
        self.set(&key::snapshot_current(shard), &stored)
            .await
            .map_err(sm_write_err)?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}
