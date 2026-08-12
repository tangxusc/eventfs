//! RaftStateMachine apply 语义测试。

use openraft::storage::RaftStateMachine;

use super::*;
use crate::EsResponse;
use es_core::{ExpectedVersion, Hlc};

fn hlc(wall: u64) -> Hlc {
    Hlc { wall, logical: 0 }
}

/// 构造带事件的 Append entry
fn append_entry(
    index: u64,
    stream: &str,
    expected: ExpectedVersion,
    events: Vec<es_core::NewEvent>,
) -> openraft::Entry<crate::TypeConfig> {
    let mut e = entry_with(1, index, stream, expected, events);
    // entry_with 默认 hlc 为 0，这里按 index 递增，便于断言
    if let openraft::EntryPayload::Normal(crate::EsRequest::Append { hlc: h, .. }) = &mut e.payload
    {
        *h = hlc(1000 + index);
    }
    e
}

#[tokio::test]
async fn first_write_version_zero() {
    let (mut st, _d) = new_storage(0);
    let resp = st
        .apply(vec![append_entry(
            0,
            "s1",
            ExpectedVersion::NoStream,
            vec![new_event("E", b"a"), new_event("E", b"b")],
        )])
        .await
        .expect("apply");

    match &resp[0] {
        EsResponse::AppendOk {
            next_expected_version,
            first_position,
            last_position,
        } => {
            assert_eq!(*next_expected_version, 1, "两条事件后当前版本应为 1");
            assert_eq!(*first_position, 0);
            assert_eq!(*last_position, 1);
        }
        other => panic!("应成功，实际: {other:?}"),
    }

    // 事件真实落盘且版本连续
    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    assert_eq!(evs.len(), 2);
    assert_eq!(evs[0].version, 0);
    assert_eq!(evs[1].version, 1);
    assert_eq!(evs[0].data, b"a");
    assert_eq!(evs[1].data, b"b");
}

#[tokio::test]
async fn consecutive_appends_version_increasing() {
    let (mut st, _d) = new_storage(0);
    for i in 0..5u64 {
        st.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }

    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    let versions: Vec<u64> = evs.iter().map(|e| e.version).collect();
    assert_eq!(versions, vec![0, 1, 2, 3, 4], "版本必须连续无空洞");

    let positions: Vec<u64> = evs.iter().map(|e| e.position).collect();
    assert_eq!(positions, vec![0, 1, 2, 3, 4], "position 必须连续递增");
}

#[tokio::test]
async fn no_stream_conflict_existing() {
    let (mut st, _d) = new_storage(0);
    st.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::NoStream,
        vec![new_event("E", b"a")],
    )])
    .await
    .expect("首次 apply");

    // 再用 NoStream 写同一个流，必须冲突
    let resp = st
        .apply(vec![append_entry(
            1,
            "s1",
            ExpectedVersion::NoStream,
            vec![new_event("E", b"b")],
        )])
        .await
        .expect("apply");

    match &resp[0] {
        EsResponse::OptimisticConflict { actual_version } => {
            assert_eq!(*actual_version, 0, "实际版本应为 0");
        }
        other => panic!("应冲突，实际: {other:?}"),
    }

    // 冲突时不得写入
    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    assert_eq!(evs.len(), 1, "冲突不能产生写入");
}

#[tokio::test]
async fn stream_exists_conflict_missing() {
    let (mut st, _d) = new_storage(0);
    let resp = st
        .apply(vec![append_entry(
            0,
            "nope",
            ExpectedVersion::StreamExists,
            vec![new_event("E", b"a")],
        )])
        .await
        .expect("apply");

    assert!(
        matches!(resp[0], EsResponse::OptimisticConflict { .. }),
        "对不存在的流用 StreamExists 必须冲突"
    );
    assert!(st.read_stream_events("nope", 0, 0).unwrap().is_empty());
}

#[tokio::test]
async fn exact_version_match_and_mismatch() {
    let (mut st, _d) = new_storage(0);
    st.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::NoStream,
        vec![new_event("E", b"a"), new_event("E", b"b")],
    )])
    .await
    .expect("首次");
    // 当前版本为 1

    // Exact(1) 应通过
    let ok = st
        .apply(vec![append_entry(
            1,
            "s1",
            ExpectedVersion::Exact(1),
            vec![new_event("E", b"c")],
        )])
        .await
        .expect("apply");
    assert!(matches!(ok[0], EsResponse::AppendOk { .. }), "Exact(1) 应通过");

    // Exact(0) 现在应冲突（当前版本已是 2）
    let bad = st
        .apply(vec![append_entry(
            2,
            "s1",
            ExpectedVersion::Exact(0),
            vec![new_event("E", b"d")],
        )])
        .await
        .expect("apply");
    match &bad[0] {
        EsResponse::OptimisticConflict { actual_version } => assert_eq!(*actual_version, 2),
        other => panic!("Exact(0) 应冲突，实际: {other:?}"),
    }

    assert_eq!(st.read_stream_events("s1", 0, 0).unwrap().len(), 3);
}

#[tokio::test]
async fn same_event_id_replay_idempotent() {
    let (mut st, _d) = new_storage(0);
    let ev = new_event("E", b"payload");

    let first = st
        .apply(vec![append_entry(
            0,
            "s1",
            ExpectedVersion::Any,
            vec![ev.clone()],
        )])
        .await
        .expect("首次");

    // 同一个 event_id 再来一次，模拟客户端重试
    let second = st
        .apply(vec![append_entry(
            1,
            "s1",
            ExpectedVersion::Any,
            vec![ev.clone()],
        )])
        .await
        .expect("重放");

    assert_eq!(
        format!("{first:?}"),
        format!("{second:?}"),
        "重放必须返回与首次相同的结果"
    );

    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    assert_eq!(evs.len(), 1, "重放不能产生重复事件");
}

#[tokio::test]
async fn batch_appends_version_chained() {
    let (mut st, _d) = new_storage(0);
    // 同一批 entry 里两条针对同一个 stream 的 Append，
    // 后一条必须看到前一条的版本号，否则会覆盖
    let resp = st
        .apply(vec![
            append_entry(0, "s1", ExpectedVersion::NoStream, vec![new_event("E", b"a")]),
            append_entry(1, "s1", ExpectedVersion::Exact(0), vec![new_event("E", b"b")]),
        ])
        .await
        .expect("apply");

    assert!(matches!(resp[0], EsResponse::AppendOk { .. }));
    match &resp[1] {
        EsResponse::AppendOk {
            next_expected_version,
            ..
        } => assert_eq!(*next_expected_version, 1),
        other => panic!("第二条应成功，实际: {other:?}"),
    }

    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    assert_eq!(evs.len(), 2, "同批两条都要落盘");
    assert_eq!(evs[0].data, b"a");
    assert_eq!(evs[1].data, b"b");
}

#[tokio::test]
async fn hlc_persisted_with_event() {
    let (mut st, _d) = new_storage(0);
    st.apply(vec![append_entry(
        7,
        "s1",
        ExpectedVersion::Any,
        vec![new_event("E", b"a")],
    )])
    .await
    .expect("apply");

    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    // append_entry 把 hlc 设为 1000+index
    assert_eq!(evs[0].hlc, hlc(1007), "HLC 必须原样落盘，不能各节点各取本地时钟");
}

#[tokio::test]
async fn applied_state_advances_and_recovers() {
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
            crate::snapshot::SnapshotConfig { dir: dir.path().join("snapshots"), ..Default::default() },
        )
        .expect("建存储");
        st.apply(vec![append_entry(
            3,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", b"a")],
        )])
        .await
        .expect("apply");

        let (la, _) = st.applied_state().await.expect("读已应用状态");
        assert_eq!(la, Some(log_id(1, 3)));
        st.close().await.expect("关闭");
    }

    // 重启后必须能恢复 last_applied，否则 openraft 会从错误位置重放
    let tree = surrealkv::TreeBuilder::new()
        .with_path(path)
        .build()
        .expect("重开 tree");
    let mut st = crate::EsStorage::new(
            0,
            std::sync::Arc::new(tree),
            crate::snapshot::SnapshotConfig { dir: dir.path().join("snapshots"), ..Default::default() },
        )
        .expect("建存储");
    st.restore_applied_state().await.expect("恢复已应用状态");

    let (la, _) = st.applied_state().await.expect("读已应用状态");
    assert_eq!(la, Some(log_id(1, 3)), "重启后 last_applied 须恢复");
    assert_eq!(
        st.read_stream_events("s1", 0, 0).unwrap().len(),
        1,
        "重启后事件须仍在"
    );
}

#[tokio::test]
async fn shard_sm_isolated() {
    let (mut sts, _d) = new_shared_storages(&[0, 1]);
    let (mut s1, mut s0) = (sts.pop().unwrap(), sts.pop().unwrap());

    // 两个分片写同名 stream，必须各自独立
    s0.apply(vec![append_entry(
        0,
        "same",
        ExpectedVersion::NoStream,
        vec![new_event("E", b"from0")],
    )])
    .await
    .expect("s0 apply");

    let r1 = s1
        .apply(vec![append_entry(
            0,
            "same",
            ExpectedVersion::NoStream,
            vec![new_event("E", b"from1")],
        )])
        .await
        .expect("s1 apply");
    assert!(
        matches!(r1[0], EsResponse::AppendOk { .. }),
        "分片 1 上该流应视为不存在"
    );

    assert_eq!(s0.read_stream_events("same", 0, 0).unwrap()[0].data, b"from0");
    assert_eq!(s1.read_stream_events("same", 0, 0).unwrap()[0].data, b"from1");
}

#[tokio::test]
async fn read_stream_range_and_limit() {
    let (mut st, _d) = new_storage(0);
    for i in 0..10u64 {
        st.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }

    // 从 version 3 起读 4 条
    let evs = st.read_stream_events("s1", 3, 4).expect("读流");
    let vs: Vec<u64> = evs.iter().map(|e| e.version).collect();
    assert_eq!(vs, vec![3, 4, 5, 6]);

    // limit 0 表示不限量
    assert_eq!(st.read_stream_events("s1", 0, 0).unwrap().len(), 10);
    // 起点越界返回空
    assert!(st.read_stream_events("s1", 100, 0).unwrap().is_empty());
}

#[tokio::test]
async fn prefix_stream_names_isolated() {
    let (mut st, _d) = new_storage(0);
    // "a" 与 "ab"：无长度前缀时扫 "a" 会把 "ab" 的事件带出来
    st.apply(vec![append_entry(
        0,
        "a",
        ExpectedVersion::Any,
        vec![new_event("E", b"ev-a")],
    )])
    .await
    .expect("写 a");
    st.apply(vec![append_entry(
        1,
        "ab",
        ExpectedVersion::Any,
        vec![new_event("E", b"ev-ab")],
    )])
    .await
    .expect("写 ab");

    let a = st.read_stream_events("a", 0, 0).expect("读 a");
    assert_eq!(a.len(), 1, "流 a 只应有自己的事件");
    assert_eq!(a[0].data, b"ev-a");

    let ab = st.read_stream_events("ab", 0, 0).expect("读 ab");
    assert_eq!(ab.len(), 1);
    assert_eq!(ab[0].data, b"ev-ab");
}

#[tokio::test]
async fn snapshot_roundtrip_consistent() {
    use openraft::RaftSnapshotBuilder;

    let (mut src, _d1) = new_storage(0);
    for i in 0..5u64 {
        src.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }

    let snap = src.build_snapshot().await.expect("建快照");
    assert_eq!(snap.meta.last_log_id, Some(log_id(1, 4)));

    // get_current_snapshot 必须能读回刚建的快照
    let cur = src.get_current_snapshot().await.expect("读当前快照");
    assert!(cur.is_some(), "建完快照后 get_current_snapshot 不应为 None");

    // 灌到一个空存储里
    let (mut dst, _d2) = new_storage(0);
    dst.install_snapshot(&snap.meta, snap.snapshot)
        .await
        .expect("装快照");

    let evs = dst.read_stream_events("s1", 0, 0).expect("读流");
    assert_eq!(evs.len(), 5, "快照恢复后事件数须一致");
    let vs: Vec<u64> = evs.iter().map(|e| e.version).collect();
    assert_eq!(vs, vec![0, 1, 2, 3, 4]);

    let (la, _) = dst.applied_state().await.expect("读已应用状态");
    assert_eq!(la, Some(log_id(1, 4)), "快照的 last_applied 须生效");
}

#[tokio::test]
async fn snapshot_overwrite_clears_old() {
    use openraft::RaftSnapshotBuilder;

    // 源只有 s1
    let (mut src, _d1) = new_storage(0);
    src.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::Any,
        vec![new_event("E", b"x")],
    )])
    .await
    .expect("apply");
    let snap = src.build_snapshot().await.expect("建快照");

    // 目标已有 s2，装快照后 s2 必须消失
    let (mut dst, _d2) = new_storage(0);
    dst.apply(vec![append_entry(
        0,
        "s2",
        ExpectedVersion::Any,
        vec![new_event("E", b"stale")],
    )])
    .await
    .expect("apply");
    assert_eq!(dst.read_stream_events("s2", 0, 0).unwrap().len(), 1);

    dst.install_snapshot(&snap.meta, snap.snapshot)
        .await
        .expect("装快照");

    assert_eq!(dst.read_stream_events("s1", 0, 0).unwrap().len(), 1);
    assert!(
        dst.read_stream_events("s2", 0, 0).unwrap().is_empty(),
        "快照里不存在的流必须被清掉，否则会残留陈旧数据"
    );
}

#[tokio::test]
async fn read_all_position_ordered() {
    use openraft::storage::RaftStateMachine;

    let (mut st, _d) = new_storage(0);

    // 写入 3 个流各 2 条
    for stream in &["s1", "s2", "s3"] {
        st.apply(vec![append_entry(
            0,
            stream,
            ExpectedVersion::NoStream,
            vec![new_event("E", b"a"), new_event("E", b"b")],
        )])
        .await
        .expect("apply");
    }

    // ReadAll 应返回 6 条，按 position 排序
    let events = st.read_all_events(0, 0).expect("read_all");
    assert_eq!(events.len(), 6, "应有 6 条事件");

    // position 递增
    for i in 1..events.len() {
        assert!(
            events[i].position > events[i - 1].position,
            "position 必须递增"
        );
    }

    // 来自不同流
    let streams: std::collections::HashSet<_> = events.iter().map(|e| e.stream_id.as_str()).collect();
    assert_eq!(streams.len(), 3, "应来自 3 个流");
}

#[tokio::test]
async fn read_all_from_position_with_limit() {
    use openraft::storage::RaftStateMachine;

    let (mut st, _d) = new_storage(0);
    for i in 0..10 {
        st.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }

    // 从 position 3 读 4 条
    let events = st.read_all_events(3, 4).expect("read_all");
    assert_eq!(events.len(), 4);
    assert_eq!(events[0].position, 3);
    assert_eq!(events[3].position, 6);
}
#[tokio::test]
async fn apply_broadcasts_to_subscribers() {
    let (mut st, _d) = new_storage(0);

    // 必须在 apply 前订阅：broadcast 只推送订阅之后产生的事件
    let mut rx = st.subscribe_events();

    st.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::NoStream,
        vec![new_event("E", b"first"), new_event("E", b"second")],
    )])
    .await
    .expect("apply");

    // 一批两条事件应各广播一次，且顺序与 position 一致
    let e1 = rx.recv().await.expect("收第一条");
    let e2 = rx.recv().await.expect("收第二条");
    assert_eq!(e1.data, b"first");
    assert_eq!(e2.data, b"second");
    assert_eq!(e1.version, 0);
    assert_eq!(e2.version, 1);
    assert!(e2.position > e1.position, "广播顺序须与 position 一致");
}

#[tokio::test]
async fn conflict_apply_no_broadcast() {
    let (mut st, _d) = new_storage(0);

    st.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::NoStream,
        vec![new_event("E", b"a")],
    )])
    .await
    .expect("首次 apply");

    let mut rx = st.subscribe_events();

    // 用 NoStream 再写同一个流，必然冲突
    let resp = st
        .apply(vec![append_entry(
            1,
            "s1",
            ExpectedVersion::NoStream,
            vec![new_event("E", b"b")],
        )])
        .await
        .expect("apply");
    assert!(matches!(resp[0], EsResponse::OptimisticConflict { .. }));

    // 冲突没有写入，也就不该有广播
    assert!(
        rx.try_recv().is_err(),
        "乐观并发冲突时不能广播事件，否则订阅者会收到不存在的数据"
    );
}

// ---- 快照文件化：压缩 / 保留 / 清理 / 损坏 ----

/// 三种压缩算法下 build→get_current→install 全链路往返一致
#[tokio::test]
async fn snapshot_roundtrip_all_compressions() {
    use openraft::RaftSnapshotBuilder;
    use crate::snapshot::Compression;

    for c in [Compression::Zstd, Compression::Lz4, Compression::None] {
        let dir = tempfile::tempdir().expect("临时目录");
        let (mut src, _) = new_storage_cfg(
            0,
            crate::snapshot::SnapshotConfig {
                dir: dir.path().join("snapshots"),
                compression: c,
                keep: 3,
            },
            dir,
        );
        for i in 0..5u64 {
            src.apply(vec![append_entry(
                i,
                "s1",
                ExpectedVersion::Any,
                vec![new_event("E", &[i as u8])],
            )])
            .await
            .expect("apply");
        }

        let snap = src.build_snapshot().await.expect("建快照");
        assert_eq!(snap.meta.last_log_id, Some(log_id(1, 4)));
        // 快照文件确实写在快照目录里（独立于业务数据 tree）
        let snap_dir = src.snapshot_store().dir().to_path_buf();
        assert!(snap_dir.join("..").join("snapshots").exists());

        let cur = src.get_current_snapshot().await.expect("读当前快照");
        let cur = cur.expect("建完快照后应有当前快照");
        assert_eq!(cur.meta.snapshot_id, snap.meta.snapshot_id);

        let (mut dst, _d2) = new_storage(0);
        dst.install_snapshot(&cur.meta, cur.snapshot)
            .await
            .expect("装快照");

        let evs = dst.read_stream_events("s1", 0, 0).expect("读流");
        assert_eq!(evs.len(), 5, "快照恢复后事件数须一致（压缩 {:?}）", c);
        let (la, _) = dst.applied_state().await.expect("读已应用状态");
        assert_eq!(la, Some(log_id(1, 4)));
    }
}

/// 多快照保留：build 超出 keep 时自动清理最旧快照文件
#[tokio::test]
async fn snapshot_retention_cleans_oldest() {
    use openraft::RaftSnapshotBuilder;

    let dir = tempfile::tempdir().expect("临时目录");
    let (mut src, _) = new_storage_cfg(
        0,
        crate::snapshot::SnapshotConfig {
            dir: dir.path().join("snapshots"),
            compression: crate::snapshot::Compression::None,
            keep: 2,
        },
        dir,
    );

    // 连续 apply + build 3 次，产生 (term=1, index=0/1/2) 三个快照
    for i in 0..3u64 {
        src.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
        src.build_snapshot().await.expect("建快照");
    }

    let files: Vec<_> = src.snapshot_store().list_entries().unwrap();
    assert_eq!(files.len(), 2, "keep=2 应只保留最近 2 个快照");
    // 保留的应是 index=1 与 index=2（最旧 index=0 被清理）
    for f in &files {
        let m = f.meta.as_ref().expect("meta");
        let idx = m.last_log_id.expect("last_log_id").index;
        assert!(idx >= 1, "最旧快照应被清理，残留 index={idx}");
    }
}

/// 安装快照后保留策略同样生效（接收的快照计入历史）
#[tokio::test]
async fn snapshot_install_respects_retention() {
    use openraft::RaftSnapshotBuilder;

    let dir = tempfile::tempdir().expect("临时目录");
    // 目标存储已有 2 个历史快照文件（index=0/1）。
    // 注意：必须持有 TempDir（解构丢弃会立即删目录）。
    let (mut dst, _dst_dir) = new_storage_cfg(
        0,
        crate::snapshot::SnapshotConfig {
            dir: dir.path().join("snapshots"),
            compression: crate::snapshot::Compression::None,
            keep: 2,
        },
        dir,
    );
    for i in 0..2u64 {
        dst.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
        dst.build_snapshot().await.expect("建快照");
    }

    // 源存储生成 index=2 的快照，安装到目标。
    // 用真实接收路径模拟：begin_receiving_snapshot 创建 temp 文件 → 写入
    // 快照内容（openraft Chunked 的行为）→ install（转正 + 保留清理）。
    let (mut src, _d2) = new_storage(0);
    for i in 0..3u64 {
        src.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }
    let snap = src.build_snapshot().await.expect("建快照");
    let src_bytes = std::fs::read(snap.snapshot.path()).expect("读快照内容");
    let mut recv = dst.begin_receiving_snapshot().await.expect("开始接收");
    {
        use tokio::io::AsyncWriteExt;
        recv.write_all(&src_bytes).await.expect("写入接收文件");
        // shutdown 刷出 tokio File 内部缓冲（openraft Chunked 在传输完成时
        // 同样调用 shutdown，见 snapshot_transport.rs done 分支）——缺了它
        // 数据停留在 tokio 用户态缓冲，文件系统里仍是空的
        recv.shutdown().await.expect("刷出缓冲");
        // 确认接收文件确实写入了完整内容
        let written = std::fs::metadata(recv.path())
            .expect("接收文件应存在")
            .len();
        assert_eq!(
            written as usize,
            src_bytes.len(),
            "接收文件应写入完整快照内容"
        );
    }
    dst.install_snapshot(&snap.meta, recv).await.expect("装快照");

    let files: Vec<_> = dst.snapshot_store().list_entries().unwrap();
    assert_eq!(files.len(), 2, "install 后仍应只保留 keep 个");
    for f in &files {
        let m = f.meta.as_ref().expect("meta");
        let idx = m.last_log_id.expect("last_log_id").index;
        assert!(idx >= 1, "install 后最旧快照应被清理，残留 index={idx}");
    }
    // 转正成功：接收的快照文件进入 dst 快照目录
    assert!(
        files.iter().any(|f| f.meta.as_ref().unwrap().last_log_id.unwrap().index == 2),
        "接收的快照应转正为正式文件"
    );
}

/// 启动清理：旧版 snapshot_current key 与 incoming 残留临时文件一并删除
#[tokio::test]
async fn snapshot_startup_cleanup_legacy() {
    let (st, _dir) = new_storage(0);
    let shard = st.shard_id();

    // 预置旧版快照 key（旧格式数据，可丢弃）与残留临时文件
    let old_key = crate::key::snapshot_current(shard);
    st.set(&old_key, b"legacy-format").await.expect("写旧 key");
    let incoming = st.snapshot_store().incoming_dir();
    std::fs::create_dir_all(&incoming).expect("建 incoming");
    std::fs::write(incoming.join("stale.tmp"), b"partial").expect("写残留");
    assert!(st.get(&old_key).unwrap().is_some(), "旧 key 应存在");

    // 启动恢复路径执行清理
    st.restore_applied_state().await.expect("恢复状态");

    assert!(st.get(&old_key).unwrap().is_none(), "旧版快照 key 应被删除");
    assert!(!incoming.join("stale.tmp").exists(), "残留临时文件应被清理");
}

/// 损坏的最新快照被跳过：get_current_snapshot 返回仍有效的快照
#[tokio::test]
async fn snapshot_corrupted_latest_skipped() {
    use openraft::RaftSnapshotBuilder;
    use crate::snapshot::Compression;

    let dir = tempfile::tempdir().expect("临时目录");
    let (mut src, _) = new_storage_cfg(
        0,
        crate::snapshot::SnapshotConfig {
            dir: dir.path().join("snapshots"),
            compression: Compression::None,
            keep: 3,
        },
        dir,
    );
    src.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::Any,
        vec![new_event("E", b"a")],
    )])
    .await
    .expect("apply");
    src.build_snapshot().await.expect("建快照");

    // 写一个损坏的"更新"快照文件（index=99，内容不是合法快照）
    let bad = src.snapshot_store().final_path(Some(log_id(1, 99)));
    std::fs::write(&bad, b"corrupted").expect("写坏文件");

    // latest 跳过损坏文件，返回 index=0 的有效快照
    let cur = src.get_current_snapshot().await.expect("读当前快照");
    let cur = cur.expect("应有有效快照");
    assert_eq!(
        cur.meta.last_log_id.expect("last_log_id").index,
        0,
        "损坏的最新快照应被跳过，返回有效快照"
    );
}

/// 空快照（无数据 build）可建可装
#[tokio::test]
async fn snapshot_empty_build_install() {
    use openraft::RaftSnapshotBuilder;

    let (mut src, _) = new_storage(0);
    let snap = src.build_snapshot().await.expect("建空快照");
    assert_eq!(snap.meta.last_log_id, None, "空快照 last_log_id 为 None");
    // 文件名用哨兵 term=0/index=0
    let fname = src
        .snapshot_store()
        .final_path(None)
        .file_name()
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert!(fname.ends_with("-00000000000000000000-00000000000000000000.esnap"));

    let (mut dst, _d2) = new_storage(0);
    dst.install_snapshot(&snap.meta, snap.snapshot)
        .await
        .expect("装空快照");
    let evs = dst.read_stream_events("s1", 0, 0).expect("读流");
    assert!(evs.is_empty(), "空快照装完无数据");
}

/// 离线 restore：恢复到快照点，清空日志与后续数据
#[tokio::test]
async fn snapshot_restore_to_point_in_time() {
    use openraft::RaftSnapshotBuilder;
    use openraft::storage::RaftLogStorage;
    use crate::snapshot::restore;

    // 源：apply 5 条（index 0..4）后建快照，再 apply 3 条（恢复点之后的数据）
    let (mut src, _d1) = new_storage(0);
    for i in 0..5u64 {
        src.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }
    let snap = src.build_snapshot().await.expect("建快照");
    for i in 5..8u64 {
        src.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }

    // 目标：先有"被清掉"的数据与日志
    let dir = tempfile::tempdir().expect("临时目录");
    let tree_path = dir.path().to_path_buf();
    let snap_dir = dir.path().join("snapshots");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(tree_path.clone())
            .build()
            .expect("打开 tree"),
    );
    {
        let dst_dir = tempfile::tempdir().expect("临时目录");
        let (mut dst, _dst_dir) = new_storage_cfg(
            0,
            crate::snapshot::SnapshotConfig {
                dir: snap_dir.clone(),
                ..Default::default()
            },
            dst_dir,
        );
        dst.apply(vec![append_entry(
            0,
            "stale",
            ExpectedVersion::Any,
            vec![new_event("E", b"stale")],
        )])
        .await
        .expect("apply");
        dst.set(&crate::key::raft_vote(0), b"{\"term\":9}").await.unwrap();
        dst.close().await.expect("关闭");
    }

    // 离线 restore（cluster 停机后操作）
    let report = restore(tree.clone(), 0, snap.snapshot.path(), &snap_dir)
        .await
        .expect("restore");
    assert_eq!(report.shard_id, 0);
    assert_eq!(report.events, 5, "恢复 5 条事件");
    assert_eq!(report.term, 1);
    assert_eq!(report.index, 4);

    // 收尾：关闭 tree 释放 LOCK，才能重开验证（esctl 同样先关再收尾）
    tree.flush_wal(true).expect("flush");
    tree.close().await.expect("关闭 tree");

    // 重启存储验证：数据回到快照点，日志清空，基线一致
    let tree2 = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(tree_path)
            .build()
            .expect("重开 tree"),
    );
    let mut st2 = crate::EsStorage::new(
        0,
        tree2,
        crate::snapshot::SnapshotConfig {
            dir: snap_dir,
            ..Default::default()
        },
    )
    .expect("建存储");

    // 旧流被清掉
    assert!(st2.read_stream_events("stale", 0, 0).unwrap().is_empty());
    // 快照点数据在
    let evs = st2.read_stream_events("s1", 0, 0).unwrap();
    assert_eq!(evs.len(), 5, "恢复到快照点，5 条事件");
    let vs: Vec<u64> = evs.iter().map(|e| e.version).collect();
    assert_eq!(vs, vec![0, 1, 2, 3, 4]);
    // 日志区清空 + 基线写回：get_log_state 回落 last_purged = 快照点
    st2.restore_applied_state().await.unwrap();
    let log_state = st2.get_log_state().await.expect("读日志状态");
    assert_eq!(
        log_state.last_log_id,
        Some(log_id(1, 4)),
        "日志基线应回到快照点"
    );
    assert_eq!(
        st2.read_committed().await.unwrap(),
        Some(log_id(1, 4)),
        "committed 应写回快照点"
    );
    let (la, _) = st2.applied_state().await.unwrap();
    assert_eq!(la, Some(log_id(1, 4)), "applied 应回到快照点");
}

/// restore 分片不匹配必须拒绝
#[tokio::test]
async fn snapshot_restore_shard_mismatch_rejected() {
    use openraft::RaftSnapshotBuilder;
    use crate::snapshot::restore;

    let (mut src, _d1) = new_storage(0);
    src.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::Any,
        vec![new_event("E", b"x")],
    )])
    .await
    .expect("apply");
    let snap = src.build_snapshot().await.expect("建快照");

    let dir = tempfile::tempdir().expect("临时目录");
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("打开 tree"),
    );
    let err = restore(tree.clone(), 1, snap.snapshot.path(), &dir.path().join("snapshots"))
        .await
        .expect_err("分片不匹配应报错");
    assert!(
        err.to_string().contains("分片"),
        "错误应说明分片不匹配: {err}"
    );
}

/// restore 后快照目录只剩恢复的快照（旧文件被清除）
#[tokio::test]
async fn snapshot_restore_replaces_snapshot_dir() {
    use openraft::RaftSnapshotBuilder;
    use crate::snapshot::restore;

    let (mut src, _d1) = new_storage(0);
    for i in 0..3u64 {
        src.apply(vec![append_entry(
            i,
            "s1",
            ExpectedVersion::Any,
            vec![new_event("E", &[i as u8])],
        )])
        .await
        .expect("apply");
    }
    let snap = src.build_snapshot().await.expect("建快照");

    let dir = tempfile::tempdir().expect("临时目录");
    // 目标快照目录里预置一个合法但陈旧的分片 0 快照文件（恢复后必须被清除）。
    // 用合法文件而非 junk：清理按文件头分片过滤，损坏文件无法判断分片会被保留
    let snap_dir = dir.path().join("snapshots");
    std::fs::create_dir_all(&snap_dir).unwrap();
    let stale = snap_dir.join("snap-00000000-00000000000000000099-00000000000000000099.esnap");
    std::fs::copy(snap.snapshot.path(), &stale).unwrap();

    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("打开 tree"),
    );
    let report = restore(tree, 0, snap.snapshot.path(), &snap_dir)
        .await
        .expect("restore");

    // 目录只剩恢复的快照文件（旧假文件被清）
    let entries: Vec<_> = std::fs::read_dir(&snap_dir).unwrap().collect();
    let names: Vec<String> = entries
        .into_iter()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".esnap"))
        .collect();
    assert_eq!(names.len(), 1, "恢复后只应有当前快照: {names:?}");
    assert_eq!(
        report.snapshot_file.file_name().unwrap().to_str().unwrap(),
        names[0]
    );
}

// ---- DeleteStream：在线迁移清尾语义 ----

/// 构造 DeleteStream entry
fn delete_entry(index: u64, stream: &str) -> openraft::Entry<crate::TypeConfig> {
    let mut e = entry_with(1, index, stream, ExpectedVersion::Any, vec![]);
    e.payload = EntryPayload::Normal(crate::EsRequest::DeleteStream {
        stream_id: stream.to_string(),
    });
    e
}

#[tokio::test]
async fn delete_removes_all_stream_data() {
    let (mut st, _d) = new_storage(0);

    // 写 2 条 → 删除
    st.apply(vec![append_entry(
        0,
        "s1",
        ExpectedVersion::NoStream,
        vec![new_event("E", b"a"), new_event("E", b"b")],
    )])
    .await
    .expect("append");
    st.apply(vec![delete_entry(1, "s1")]).await.expect("delete");

    // 读流空、meta 不存在、$all 空
    assert!(st.read_stream_events("s1", 0, 0).expect("读流").is_empty());
    assert!(st.read_stream_meta("s1").expect("读 meta").is_none());
    assert!(
        st.read_all_events(0, 0).expect("读 all").is_empty(),
        "$all 不应再看到被删流的事件（position 指针已清理）"
    );

    // 删除后 NoStream 可重新创建（版本从 0 重新开始）
    let resp = st
        .apply(vec![append_entry(
            2,
            "s1",
            ExpectedVersion::NoStream,
            vec![new_event("E", b"c")],
        )])
        .await
        .expect("重建");
    match &resp[0] {
        EsResponse::AppendOk { next_expected_version, .. } => {
            assert_eq!(*next_expected_version, 0, "重建后版本重新从 0 起");
        }
        other => panic!("应成功，实际: {other:?}"),
    }
    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].data, b"c");
}

#[tokio::test]
async fn delete_nonexistent_is_noop() {
    let (mut st, _d) = new_storage(0);
    let resp = st
        .apply(vec![delete_entry(0, "ghost")])
        .await
        .expect("delete 不存在流");
    assert!(matches!(resp[0], EsResponse::DeleteOk), "应返回 DeleteOk: {:?}", resp[0]);
    // 无副作用：$all 仍空、next_position 不推进
    assert!(st.read_all_events(0, 0).expect("读 all").is_empty());
}

#[tokio::test]
async fn delete_then_append_same_batch_recreates() {
    let (mut st, _d) = new_storage(0);
    // 同批：先删后写 → 流应存在（后操作覆盖）
    st.apply(vec![append_entry(0, "s1", ExpectedVersion::NoStream, vec![new_event("E", b"a")])])
        .await
        .expect("写");
    let resp = st
        .apply(vec![
            delete_entry(1, "s1"),
            append_entry(2, "s1", ExpectedVersion::NoStream, vec![new_event("E", b"b")]),
        ])
        .await
        .expect("同批先删后写");
    assert!(matches!(resp[0], EsResponse::DeleteOk));
    assert!(matches!(resp[1], EsResponse::AppendOk { .. }));
    let evs = st.read_stream_events("s1", 0, 0).expect("读流");
    assert_eq!(evs.len(), 1, "后写应保留");
    assert_eq!(evs[0].data, b"b");
}

#[tokio::test]
async fn append_then_delete_same_batch_removes() {
    let (mut st, _d) = new_storage(0);
    // 同批：先写后删 → 流不存在（后操作覆盖）
    let resp = st
        .apply(vec![
            append_entry(0, "s1", ExpectedVersion::NoStream, vec![new_event("E", b"a")]),
            delete_entry(1, "s1"),
        ])
        .await
        .expect("同批先写后删");
    assert!(matches!(resp[0], EsResponse::AppendOk { .. }));
    assert!(matches!(resp[1], EsResponse::DeleteOk));
    assert!(st.read_stream_meta("s1").expect("读 meta").is_none());
    assert!(st.read_all_events(0, 0).expect("读 all").is_empty());
}

/// 模糊测试：随机 Append/DeleteStream 序列后，不变量成立——
/// 流的版本连续无空洞、删除后流不可见、重建后版本归零。
///
/// 确定性伪随机（固定种子 LCG）：可复现、无 proptest 宏的 async 限制。
#[tokio::test]
async fn fuzz_random_append_delete_invariants() {
    // LCG：固定种子，可复现
    let mut seed: u64 = 0x5eed_cafe;
    let mut rng = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        seed
    };

    for round in 0..30 {
        let (mut st, _d) = new_storage(0);
        let n_ops = (rng() % 15) + 1;
        let n_names = (rng() % 3) + 1;
        let names: Vec<String> = (0..n_names).map(|i| format!("s{i}")).collect();
        for i in 0..n_ops {
            let name = &names[(rng() as usize) % names.len()];
            let mut entries = Vec::new();
            if rng() % 2 == 0 {
                let n = (rng() % 3) + 1;
                let events: Vec<_> = (0..n).map(|_| new_event("E", b"x")).collect();
                entries.push(append_entry(i as u64, name, ExpectedVersion::Any, events));
            } else {
                entries.push(delete_entry(i as u64, name));
            }
            let _ = st.apply(entries).await.expect("apply");
        }

        // 不变量：存在流的版本连续无空洞（从 0 到 current_version）
        for name in &names {
            if let Some(meta) = st.read_stream_meta(name).expect("读 meta") {
                let evs = st.read_stream_events(name, 0, 0).expect("读流");
                assert_eq!(
                    evs.len() as u64,
                    meta.current_version + 1,
                    "round {round} 流 {name} 版本应连续无空洞"
                );
                let versions: Vec<u64> = evs.iter().map(|e| e.version).collect();
                let expect: Vec<u64> = (0..=meta.current_version).collect();
                assert_eq!(versions, expect, "round {round} 版本序列应完整");
            }
        }
    }
}
