//! RaftLogStorage 语义测试，跑在真实 surrealkv 上。

use openraft::storage::RaftLogStorage;
use openraft::{RaftLogReader, Vote};

use super::*;

#[tokio::test]
async fn empty_storage_log_state_empty() {
    let (mut st, _d) = new_storage(0);
    let state = st.get_log_state().await.expect("读日志状态");
    assert_eq!(state.last_purged_log_id, None);
    assert_eq!(state.last_log_id, None);
}

#[tokio::test]
async fn vote_roundtrip() {
    let (mut st, _d) = new_storage(0);
    assert_eq!(st.read_vote().await.expect("读 vote"), None);

    let v = Vote::new(3, 7);
    st.save_vote(&v).await.expect("写 vote");
    assert_eq!(st.read_vote().await.expect("读 vote"), Some(v));

    // 覆盖写
    let v2 = Vote::new(4, 9);
    st.save_vote(&v2).await.expect("覆盖 vote");
    assert_eq!(st.read_vote().await.expect("读 vote"), Some(v2));
}

#[tokio::test]
async fn committed_roundtrip() {
    let (mut st, _d) = new_storage(0);
    assert_eq!(st.read_committed().await.expect("读 committed"), None);

    st.save_committed(Some(log_id(1, 5)))
        .await
        .expect("写 committed");
    assert_eq!(
        st.read_committed().await.expect("读 committed"),
        Some(log_id(1, 5))
    );
}

#[tokio::test]
async fn append_log_state_reflects_max_index() {
    let (mut st, _d) = new_storage(0);
    do_append(
        &mut st,
        vec![entry(1, 0, "s"), entry(1, 1, "s"), entry(1, 2, "s")],
    )
    .await;

    let state = st.get_log_state().await.expect("读日志状态");
    // seek_last 必须取到 index=2，而非 0 或 None
    assert_eq!(state.last_log_id, Some(log_id(1, 2)));
    assert_eq!(state.last_purged_log_id, None);
}

#[tokio::test]
async fn log_index_cross_byte_boundary_ordered() {
    let (mut st, _d) = new_storage(0);
    // 254/255/256/257 跨越单字节边界，若用小端或变长编码这里会错乱
    let idxs = [254u64, 255, 256, 257, 65535, 65536];
    let entries: Vec<_> = idxs.iter().map(|&i| entry(1, i, "s")).collect();
    do_append(&mut st, entries).await;

    let state = st.get_log_state().await.expect("读日志状态");
    assert_eq!(
        state.last_log_id,
        Some(log_id(1, 65536)),
        "最大 index 应为 65536"
    );

    let got = st.try_get_log_entries(..).await.expect("全量读");
    let got_idxs: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
    assert_eq!(got_idxs, idxs.to_vec(), "扫描结果必须按 index 数值升序");
}

#[tokio::test]
async fn range_read_boundaries() {
    let (mut st, _d) = new_storage(0);
    do_append(&mut st, (0..10).map(|i| entry(1, i, "s")).collect()).await;

    let idxs = |v: Vec<openraft::Entry<crate::TypeConfig>>| -> Vec<u64> {
        v.iter().map(|e| e.log_id.index).collect()
    };

    // 半开区间
    assert_eq!(
        idxs(st.try_get_log_entries(2..5).await.unwrap()),
        vec![2, 3, 4]
    );
    // 闭区间
    assert_eq!(
        idxs(st.try_get_log_entries(2..=5).await.unwrap()),
        vec![2, 3, 4, 5]
    );
    // 起点无界
    assert_eq!(
        idxs(st.try_get_log_entries(..3).await.unwrap()),
        vec![0, 1, 2]
    );
    // 终点无界
    assert_eq!(
        idxs(st.try_get_log_entries(7..).await.unwrap()),
        vec![7, 8, 9]
    );
    // 全量
    assert_eq!(idxs(st.try_get_log_entries(..).await.unwrap()).len(), 10);
    // 空区间
    assert!(st.try_get_log_entries(5..5).await.unwrap().is_empty());
    // 越界区间
    assert!(st.try_get_log_entries(100..200).await.unwrap().is_empty());
}

#[tokio::test]
async fn truncate_removes_index_and_after() {
    let (mut st, _d) = new_storage(0);
    do_append(&mut st, (0..10).map(|i| entry(1, i, "s")).collect()).await;

    // 删除 [4, +oo)
    st.truncate(log_id(1, 4)).await.expect("truncate");

    let got = st.try_get_log_entries(..).await.expect("全量读");
    let got_idxs: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
    assert_eq!(got_idxs, vec![0, 1, 2, 3], "index >= 4 的必须全部删除");

    let state = st.get_log_state().await.expect("读日志状态");
    assert_eq!(
        state.last_log_id,
        Some(log_id(1, 3)),
        "last_log_id 须回退到 3"
    );
}

#[tokio::test]
async fn purge_removes_index_and_before() {
    let (mut st, _d) = new_storage(0);
    do_append(&mut st, (0..10).map(|i| entry(1, i, "s")).collect()).await;

    // 删除 (-oo, 4]
    st.purge(log_id(1, 4)).await.expect("purge");

    let got = st.try_get_log_entries(..).await.expect("全量读");
    let got_idxs: Vec<u64> = got.iter().map(|e| e.log_id.index).collect();
    assert_eq!(got_idxs, vec![5, 6, 7, 8, 9], "index <= 4 的必须全部删除");

    let state = st.get_log_state().await.expect("读日志状态");
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
    assert_eq!(state.last_log_id, Some(log_id(1, 9)));
}

#[tokio::test]
async fn purge_all_last_log_falls_back() {
    let (mut st, _d) = new_storage(0);
    do_append(&mut st, (0..5).map(|i| entry(1, i, "s")).collect()).await;

    st.purge(log_id(1, 4)).await.expect("purge 全部");

    let state = st.get_log_state().await.expect("读日志状态");
    assert!(
        st.try_get_log_entries(..).await.unwrap().is_empty(),
        "日志应已清空"
    );
    // openraft 契约：日志空时 last_log_id 必须回落到 last_purged，
    // 否则 leader 会以为该 follower 从未有过日志而从 0 重发
    assert_eq!(state.last_purged_log_id, Some(log_id(1, 4)));
    assert_eq!(state.last_log_id, Some(log_id(1, 4)));
}

#[tokio::test]
async fn shard_logs_isolated() {
    let (mut sts, _d) = new_shared_storages(&[0, 1]);
    let (mut s1, mut s0) = (sts.pop().unwrap(), sts.pop().unwrap());

    do_append(&mut s0, vec![entry(1, 0, "a"), entry(1, 1, "a")]).await;
    do_append(&mut s1, vec![entry(1, 0, "b")]).await;

    // 共享同一个 tree，仅靠 key 前缀隔离
    assert_eq!(s0.try_get_log_entries(..).await.unwrap().len(), 2);
    assert_eq!(s1.try_get_log_entries(..).await.unwrap().len(), 1);

    // 分片 0 的 truncate 不能影响分片 1
    s0.truncate(log_id(1, 0)).await.expect("truncate s0");
    assert_eq!(s0.try_get_log_entries(..).await.unwrap().len(), 0);
    assert_eq!(
        s1.try_get_log_entries(..).await.unwrap().len(),
        1,
        "分片 1 的日志不应被分片 0 的 truncate 波及"
    );
}

#[tokio::test]
async fn vote_and_log_not_mixed() {
    let (mut st, _d) = new_storage(0);
    // vote key 是日志区的字节序后继，若上界算错会被扫进日志结果
    st.save_vote(&Vote::new(1, 1)).await.expect("写 vote");
    do_append(&mut st, vec![entry(1, 0, "s")]).await;

    let got = st.try_get_log_entries(..).await.expect("全量读");
    assert_eq!(got.len(), 1, "vote 不能被当作日志条目扫出来");

    // truncate 掉所有日志后 vote 必须仍在
    st.truncate(log_id(1, 0)).await.expect("truncate");
    assert!(st.try_get_log_entries(..).await.unwrap().is_empty());
    assert_eq!(
        st.read_vote().await.expect("读 vote"),
        Some(Vote::new(1, 1)),
        "truncate 不能删掉 vote"
    );
}

#[tokio::test]
async fn reopen_keeps_log_and_vote() {
    let dir = tempfile::tempdir().expect("临时目录");
    let path = dir.path().to_path_buf();

    {
        let tree = surrealkv::TreeBuilder::new()
            .with_path(path.clone())
            .build()
            .expect("开 tree");
        let mut st = crate::EsStorage::new(
            0,
            std::sync::Arc::new(tree),
            crate::snapshot::SnapshotConfig {
                dir: dir.path().join("snapshots"),
                ..Default::default()
            },
        )
        .expect("建存储");
        do_append(&mut st, vec![entry(2, 0, "s"), entry(2, 1, "s")]).await;
        st.save_vote(&Vote::new(2, 3)).await.expect("写 vote");
        // 必须显式 close：Tree::close 是 async，drop 不释放 LOCK 文件
        st.close().await.expect("关闭存储");
    }

    // 重新打开同一目录，模拟进程重启
    let tree = surrealkv::TreeBuilder::new()
        .with_path(path)
        .build()
        .expect("重开 tree");
    let mut st = crate::EsStorage::new(
        0,
        std::sync::Arc::new(tree),
        crate::snapshot::SnapshotConfig {
            dir: dir.path().join("snapshots"),
            ..Default::default()
        },
    )
    .expect("建存储");

    let state = st.get_log_state().await.expect("读日志状态");
    assert_eq!(state.last_log_id, Some(log_id(2, 1)), "重启后日志须仍在");
    assert_eq!(
        st.read_vote().await.expect("读 vote"),
        Some(Vote::new(2, 3))
    );
}
