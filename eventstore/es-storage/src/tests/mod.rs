//! crate 内集成测试。
//!
//! 放在 crate 内而非 `tests/` 目录，因为 `LogFlushed::new` 是 `pub(crate)`，
//! 外部集成测试无法构造 append 所需的回调。

mod log_storage_test;
mod state_machine_test;
mod storage_test;

use std::sync::Arc;

use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

use crate::EsStorage;
use crate::raft_type::TypeConfig;
use crate::snapshot::SnapshotConfig;

/// 在临时目录建一个存储实例。TempDir 必须由调用方持有，drop 即删目录。
pub(crate) fn new_storage(shard_id: u64) -> (EsStorage, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("建临时目录");
    new_storage_cfg(
        shard_id,
        SnapshotConfig {
            dir: dir.path().join("snapshots"),
            ..Default::default()
        },
        dir,
    )
}

/// 用指定快照配置建存储（压缩/保留参数化测试用）
pub(crate) fn new_storage_cfg(
    shard_id: u64,
    snap: SnapshotConfig,
    dir: tempfile::TempDir,
) -> (EsStorage, tempfile::TempDir) {
    let tree = surrealkv::TreeBuilder::new()
        .with_path(dir.path().to_path_buf())
        .build()
        .expect("打开 tree");
    let st = EsStorage::new(shard_id, Arc::new(tree), snap).expect("建存储");
    (st, dir)
}

/// 在同一个 tree 上建多个分片的存储，用于验证 key 前缀隔离
pub(crate) fn new_shared_storages(shard_ids: &[u64]) -> (Vec<EsStorage>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("建临时目录");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("打开 tree"),
    );
    let sts = shard_ids
        .iter()
        .map(|&id| {
            let snap = SnapshotConfig {
                dir: dir.path().join("snapshots"),
                ..Default::default()
            };
            EsStorage::new(id, tree.clone(), snap).expect("建存储")
        })
        .collect();
    (sts, dir)
}

pub(crate) fn log_id(term: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, 0), index)
}

/// 构造一条用于日志存储测试的空日志条目。
pub(crate) fn entry(term: u64, index: u64, _label: &str) -> Entry<TypeConfig> {
    Entry {
        log_id: log_id(term, index),
        payload: EntryPayload::Blank,
    }
}

/// 调 append 并等待落盘。
///
/// `LogFlushed::new` 是 openraft 的 crate 私有构造器，外部无法直接造回调。
/// openraft 为此提供了 `RaftLogStorageExt::blocking_append`，内部自行构造
/// 回调并等待落盘完成，正是测试需要的语义。
pub(crate) async fn do_append(st: &mut EsStorage, entries: Vec<Entry<TypeConfig>>) {
    use openraft::storage::RaftLogStorageExt;
    st.blocking_append(entries).await.expect("append 必须成功");
}
