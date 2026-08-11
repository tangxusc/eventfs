//! crate 内集成测试。
//!
//! 放在 crate 内而非 `tests/` 目录，因为 `LogFlushed::new` 是 `pub(crate)`，
//! 外部集成测试无法构造 append 所需的回调。

mod log_storage_test;
mod state_machine_test;

use std::sync::Arc;

use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

use crate::raft_type::TypeConfig;
use crate::{EsRequest, EsStorage};
use es_core::{ExpectedVersion, NewEvent};

/// 在临时目录建一个存储实例。TempDir 必须由调用方持有，drop 即删目录。
pub(crate) fn new_storage(shard_id: u64) -> (EsStorage, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("建临时目录");
    let tree = surrealkv::TreeBuilder::new()
        .with_path(dir.path().to_path_buf())
        .build()
        .expect("打开 tree");
    let st = EsStorage::new(shard_id, Arc::new(tree)).expect("建存储");
    (st, dir)
}

/// 在同一个 tree 上建多个分片的存储，用于验证 key 前缀隔离
pub(crate) fn new_shared_storages(
    shard_ids: &[u64],
) -> (Vec<EsStorage>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("建临时目录");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("打开 tree"),
    );
    let sts = shard_ids
        .iter()
        .map(|&id| EsStorage::new(id, tree.clone()).expect("建存储"))
        .collect();
    (sts, dir)
}

pub(crate) fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 0), index)
}

/// 构造一条 Append 日志条目
pub(crate) fn entry(term: u64, index: u64, stream: &str) -> Entry<TypeConfig> {
    entry_with(term, index, stream, ExpectedVersion::Any, vec![])
}

/// 造一个新事件
pub(crate) fn new_event(event_type: &str, data: &[u8]) -> NewEvent {
    NewEvent {
        event_id: uuid::Uuid::new_v4(),
        event_type: event_type.to_string(),
        data: data.to_vec(),
        metadata: vec![],
    }
}

/// 构造带具体事件与期望版本的日志条目
pub(crate) fn entry_with(
    term: u64,
    index: u64,
    stream: &str,
    expected: ExpectedVersion,
    events: Vec<NewEvent>,
) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Normal(EsRequest::Append {
            stream_id: stream.to_string(),
            expected_version: expected,
            events,
            hlc: es_core::Hlc { wall: 0, logical: 0 }, // 默认 0，测试里按需覆盖
        }),
    }
}

/// 调 append 并等待落盘。
///
/// `LogFlushed::new` 是 openraft 的 crate 私有构造器，外部无法直接造回调。
/// openraft 为此提供了 `RaftLogStorageExt::blocking_append`，内部自行构造
/// 回调并等待落盘完成，正是测试需要的语义。
pub(crate) async fn do_append(st: &mut EsStorage, entries: Vec<Entry<TypeConfig>>) {
    use openraft::storage::RaftLogStorageExt;
    st.blocking_append(entries)
        .await
        .expect("append 必须成功");
}
