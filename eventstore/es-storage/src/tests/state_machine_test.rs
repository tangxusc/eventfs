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
async fn 首次写入版本从0起() {
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
async fn 连续追加版本递增不空洞() {
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
async fn no_stream对已存在流报冲突() {
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
async fn stream_exists对不存在流报冲突() {
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
async fn exact版本匹配与不匹配() {
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
async fn 相同event_id重放幂等() {
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
async fn 同批次内多条append版本串接() {
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
async fn hlc随事件落盘() {
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
async fn applied_state随apply推进并可重启恢复() {
    let dir = tempfile::tempdir().expect("临时目录");
    let path = dir.path().to_path_buf();

    {
        let tree = surrealkv::TreeBuilder::new()
            .with_path(path.clone())
            .build()
            .expect("开 tree");
        let mut st = crate::EsStorage::new(0, std::sync::Arc::new(tree)).expect("建存储");
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
    let mut st = crate::EsStorage::new(0, std::sync::Arc::new(tree)).expect("建存储");
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
async fn 分片间状态机互不干扰() {
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
async fn 读流区间与限量() {
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
async fn 前缀包含的流名不串数据() {
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
async fn 快照往返后数据一致() {
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
async fn 装快照会清掉目标原有数据() {
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
async fn read_all按position顺序读取() {
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
async fn read_all可指定起始与限量() {
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
async fn apply后广播新事件给订阅者() {
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
async fn 冲突的apply不广播事件() {
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
