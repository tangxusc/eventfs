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
use es_core::{Event, ExpectedVersion, StreamMeta};

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
            let name = key::decode_stream_meta_key(&k).ok_or_else(|| {
                es_core::Error::Serde(format!("StreamMeta key 解码失败: {k:?}"))
            })?;
            let meta: StreamMeta = crate::encode::decode(&v)
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
        self.ops.retain(|op| !matches!(op, ApplyOp::Put(k, _) if *k == key));
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
}

impl EsStorage {
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
    ) -> es_core::Result<EsResponse> {
        let shard = self.shard_id();

        // 幂等去重：若首条事件的 event_id 已存在，说明整批已应用过。
        // 客户端重试（网络超时但实际已提交）会走到这里，必须返回原结果而非重复追加。
        if let Some(first) = events.first() {
            let idem_k = key::sm_idempotency(shard, &first.event_id);
            if let Some(bytes) = self.get(&idem_k)? {
                let (v0, p0): (u64, u64) = crate::encode::decode(&bytes).map_err(|e| {
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
            let ev_bytes = crate::encode::encode(&stored)
                .map_err(|e| es_core::Error::Serde(format!("Event 序列化失败: {e}")))?;
            batch.ops.push(ApplyOp::Put(
                key::sm_event(shard, stream_id, version),
                ev_bytes,
            ));

            // position 指针，供分片内 $all 流按提交序读取
            let ptr = crate::encode::encode(&(stream_id, version))
                .map_err(|e| es_core::Error::Serde(format!("position 指针序列化失败: {e}")))?;
            batch.ops.push(ApplyOp::Put(
                key::sm_position_ptr(shard, position),
                ptr,
            ));

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
        batch.stream_versions.insert(stream_id.to_string(), Some(last_version));
        batch.deleted.remove(stream_id);

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
            ops: Vec::new(),
            stream_versions: std::collections::HashMap::new(),
            deleted: std::collections::HashSet::new(),
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
                    EsRequest::DeleteStream { stream_id } => {
                        let resp = self.apply_delete(&mut batch, stream_id).map_err(sm_write_err)?;
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
        let np = crate::encode::encode(&batch.next_position).map_err(sm_write_err)?;
        batch
            .ops
            .push(ApplyOp::Put(key::sm_next_position(shard), np));

        // 已应用状态：与业务数据同事务提交，保证重启后 last_applied 与数据一致
        let mut cache = self.sm_cache().write().await;
        let new_last_applied = last_applied.or(cache.last_applied);
        let new_membership = membership.clone().unwrap_or_else(|| cache.membership.clone());
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
        let old_keys = self
            .collect_keys(sm_start, sm_end)
            .map_err(sm_write_err)?;

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
                tracing::warn!(
                    "快照文件转正失败（仅损失文件缓存，SM 数据已提交）: {e}"
                );
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
