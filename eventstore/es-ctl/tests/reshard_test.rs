//! esctl reshard 端到端：进程内 2 分片写数据 → 释放锁 → esctl reshard 子进程
//! 重分布到 4 分片 → 打开目标树验证数据完整（流数/事件数一致、version/event_id
//! 不变、分片内 position 连续）。

use std::collections::{HashMap, HashSet};
use std::process::{Command, Output};
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use surrealkv::LSMIterator;

/// 启动 2 分片旧布局数据目录并写入数据，返回 (数据目录, 事件总数)。
async fn start_and_write() -> (tempfile::TempDir, usize) {
    let dir = tempfile::tempdir().expect("临时目录");
    let total = write_old_layout(dir.path()).await;
    (dir, total)
}

/// 在指定目录构造旧布局数据（单共享 tree + 分片 key 前缀）：6 流 × 2 事件，
/// 每分片 3 个流。返回事件总数。
///
/// Phase 1 起服务器改为 per-shard tree（{data_dir}/shard-{id}/），但 esctl
/// reshard 命令本期仍按旧布局读取——es_storage::reshard 的枚举/路由建立在
/// 共享 tree + 分片前缀 key 上（Phase 3 才随命令替换）。测试直接构造命令
/// 能理解的旧布局（与 tests_e2e.rs 的 make_reshard_src 同款），语义不变。
async fn write_old_layout(dir: &std::path::Path) -> usize {
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.to_path_buf())
            .build()
            .expect("建 src tree"),
    );
    let mut sts = (0..2u64)
        .map(|id| {
            es_storage::EsStorage::new(
                id,
                tree.clone(),
                es_storage::snapshot::SnapshotConfig {
                    dir: dir.join("snapshots"),
                    ..Default::default()
                },
            )
            .expect("建存储")
        })
        .collect::<Vec<_>>();

    // 每分片 3 个流：按 es_core::route(stream, 2) 实际路由挑选，保证两个
    // 分片都有数据（infer_shard_count 按最大分片 ID 推断，缺一个分片会低估）
    let mut names_by_shard: [Vec<String>; 2] = [vec![], vec![]];
    for i in 0..100u64 {
        let name = format!("stream-{i}");
        let s = es_core::route(&name, 2) as usize;
        if names_by_shard[s].len() < 3 {
            names_by_shard[s].push(name);
        }
        if names_by_shard.iter().all(|v| v.len() == 3) {
            break;
        }
    }
    assert!(
        names_by_shard.iter().all(|v| v.len() == 3),
        "应能找到 3 个流路由到每个分片: {names_by_shard:?}"
    );

    let mut total = 0;
    for (shard_id, names) in names_by_shard.iter().enumerate() {
        let entries: Vec<Entry<es_storage::TypeConfig>> = names
            .iter()
            .enumerate()
            .map(|(i, name)| Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 0), (i + 1) as u64),
                payload: EntryPayload::Normal(es_storage::EsRequest::Append {
                    stream_id: name.clone(),
                    expected_version: es_core::ExpectedVersion::NoStream,
                    events: vec![
                        es_core::NewEvent {
                            event_id: uuid::Uuid::new_v4(),
                            event_type: "E".into(),
                            data: format!("{{\"v\":{shard_id}-{i}-0}}").into_bytes(),
                            metadata: vec![],
                        },
                        es_core::NewEvent {
                            event_id: uuid::Uuid::new_v4(),
                            event_type: "E".into(),
                            data: format!("{{\"v\":{shard_id}-{i}-1}}").into_bytes(),
                            metadata: vec![],
                        },
                    ],
                    hlc: es_core::Hlc { wall: 1, logical: 0 },
                }),
            })
            .collect();
        sts[shard_id].apply(entries).await.expect("写分片");
        total += names.len() * 2;
    }

    // 释放 LOCK：逐分片关存储（共享 tree，最后一个 close 释放锁文件）
    for st in &sts {
        st.close().await.expect("关闭存储");
    }

    total
}

/// 运行 esctl（不带 --endpoints，离线命令不需要）
fn esctl_offline(args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(args);
    cmd.output().expect("运行 esctl")
}

fn err(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string()
}

/// 打开目标树并统计 (流数, 事件数, stream -> (versions, event_ids))
async fn verify_dst(
    dir: &std::path::Path,
    dst_shards: u64,
) -> (usize, usize, HashMap<String, (Vec<u64>, Vec<String>)>) {
    // 目标目录直接是 dst tree（与 src 布局相同，EsStorage 同 key 编码）
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.to_path_buf())
            .build()
            .expect("打开目标树"),
    );

    let mut streams: HashMap<String, (Vec<u64>, Vec<String>)> = HashMap::new();
    for shard_id in 0..dst_shards {
        // 扫描状态机事件区：[TAG_SM=0x02][shard:BE8][SM_EVENT=0x01]
        let start = key_prefix(shard_id);
        let end = es_storage::key::successor(&start).expect("前缀末字节非 0xFF，后继存在");
        // surrealkv 迭代器不是 Send，须在同步块内完成扫描（不能跨 await 存活）
        let txn = tree.begin().expect("begin");
        let mut it = txn.range(start, end).expect("range");
        it.seek_first().expect("seek_first");
        while it.valid() {
            let value = it.value().expect("value");
            // 事件值 = es_core::Event（bincode 序列化，与 es-storage 存储格式一致）
            let event: es_core::Event =
                es_storage::encode::decode(&value).expect("事件值应为合法 bincode（es_core::Event）");
            let entry = streams
                .entry(event.stream_id)
                .or_insert_with(|| (vec![], vec![]));
            entry.0.push(event.version);
            entry.1.push(event.event_id.to_string());
            it.next().expect("next");
        }
        drop(it);
        drop(txn);
    }

    tree.flush_wal(true).expect("flush");
    tree.close().await.expect("close");

    let streams = streams;
    let events = streams.values().map(|(v, _)| v.len()).sum::<usize>();
    (streams.len(), events, streams)
}

/// 状态机事件区扫描前缀：[TAG_SM][shard:BE8][SM_EVENT]（对齐 es-storage/src/key.rs）
fn key_prefix(shard_id: u64) -> Vec<u8> {
    let mut prefix = Vec::new();
    prefix.push(0x02); // TAG_SM
    prefix.extend_from_slice(&shard_id.to_be_bytes());
    prefix.push(0x01); // SM_EVENT
    prefix
}

#[tokio::test(flavor = "multi_thread")]
async fn reshard_2_to_4_data_intact() {
    let (dir, total) = start_and_write().await;

    let dst_dir = tempfile::tempdir().expect("目标临时目录");
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "2",
        "--dst-dir",
        dst_dir.path().to_str().unwrap(),
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert!(out.status.success(), "reshard 失败: {}", err(&out));
    let text = stdout(&out);
    assert!(text.contains("重分布完成"), "{text}");
    assert!(text.contains("2 分片 → 4 分片"), "{text}");

    // 验证目标布局：流数、事件数一致
    let (dst_streams, dst_events, streams) = verify_dst(dst_dir.path(), 4).await;
    assert_eq!(dst_streams, 6, "目标流数应为 6");
    assert_eq!(dst_events, total, "目标事件数应等于写入数 {total}");

    // 逐流验证 version 连续且 event_id 不变
    for (stream, (versions, event_ids)) in &streams {
        let mut vs = versions.clone();
        vs.sort_unstable();
        assert_eq!(
            vs,
            vec![0, 1],
            "流 {stream} version 应为 [0,1]，实际 {vs:?}"
        );
        assert_eq!(event_ids.len(), 2, "流 {stream} 应有 2 个事件");
    }

    // 事件 ID 集合跨流唯一（无重复）
    let all_ids: HashSet<&String> = streams.values().flat_map(|(_, ids)| ids).collect();
    assert_eq!(all_ids.len(), total, "事件 ID 应全部唯一");
}

#[tokio::test(flavor = "multi_thread")]
async fn reshard_negative_cases_rejected() {
    let (dir, _) = start_and_write().await;

    // src == dst
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "2",
        "--dst-dir",
        dir.path().to_str().unwrap(),
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(err(&out).contains("必须不同"), "{}", err(&out));

    // src 不存在
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        "/nonexistent-xyz",
        "--src-shards",
        "2",
        "--dst-dir",
        "/tmp/nonexistent-dst-xyz",
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(err(&out).contains("不存在"), "{}", err(&out));

    // 分片数为 0
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "0",
        "--dst-dir",
        "/tmp/nonexistent-dst-xyz",
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert_eq!(out.status.code(), Some(1));
}

#[tokio::test(flavor = "multi_thread")]
async fn reshard_dst_nonempty_requires_confirm() {
    let (dir, _) = start_and_write().await;

    // 目标目录已存在且有文件
    let dst_dir = tempfile::tempdir().expect("目标临时目录");
    std::fs::write(dst_dir.path().join("LOCK"), b"x").expect("写文件");

    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "2",
        "--dst-dir",
        dst_dir.path().to_str().unwrap(),
        "--dst-shards",
        "4",
    ]);
    assert_eq!(out.status.code(), Some(1));
    assert!(err(&out).contains("非空"), "{}", err(&out));

    // 加 --yes 后通过
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "2",
        "--dst-dir",
        dst_dir.path().to_str().unwrap(),
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert!(out.status.success(), "reshard 失败: {}", err(&out));
}

#[tokio::test(flavor = "multi_thread")]
async fn reshard_lock_held_rejected() {
    let dir = tempfile::tempdir().expect("临时目录");
    // 旧布局（共享 tree）：分片 0 写入 1 条事件后保持打开 → 持有顶层 LOCK
    // （Phase 1 起服务器改为 per-shard tree，顶层的锁语义由本测试直接构造）
    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.path().to_path_buf())
            .build()
            .expect("建 src tree"),
    );
    let mut st = es_storage::EsStorage::new(
        0,
        tree,
        es_storage::snapshot::SnapshotConfig {
            dir: dir.path().join("snapshots"),
            ..Default::default()
        },
    )
    .expect("建存储");
    st.apply(vec![Entry::<es_storage::TypeConfig> {
        log_id: LogId::new(CommittedLeaderId::new(1, 0), 1),
        payload: EntryPayload::Normal(es_storage::EsRequest::Append {
            stream_id: "s0".into(),
            expected_version: es_core::ExpectedVersion::NoStream,
            events: vec![es_core::NewEvent {
                event_id: uuid::Uuid::new_v4(),
                event_type: "E".into(),
                data: b"x".to_vec(),
                metadata: vec![],
            }],
            hlc: es_core::Hlc { wall: 1, logical: 0 },
        }),
    }])
    .await
    .expect("写分片 0");

    // 存储未关闭 → 持有 LOCK，reshard 打开源目录必须失败
    let dst_dir = tempfile::tempdir().expect("目标临时目录");
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "1",
        "--dst-dir",
        dst_dir.path().to_str().unwrap(),
        "--dst-shards",
        "2",
        "--yes",
    ]);
    assert_eq!(
        out.status.code(),
        Some(1),
        "LOCK 占用时应失败: {}",
        err(&out)
    );
    assert!(
        err(&out).contains("LOCK") || err(&out).contains("占用"),
        "{}",
        err(&out)
    );

    // 释放后成功
    st.close().await.expect("关闭存储");
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "1",
        "--dst-dir",
        dst_dir.path().to_str().unwrap(),
        "--dst-shards",
        "2",
        "--yes",
    ]);
    assert!(out.status.success(), "释放 LOCK 后应成功: {}", err(&out));
}

#[tokio::test(flavor = "multi_thread")]
async fn reshard_json_output_format() {
    let (dir, _) = start_and_write().await;
    let dst_dir = tempfile::tempdir().expect("目标临时目录");

    let out = Command::new(env!("CARGO_BIN_EXE_esctl"))
        .args([
            "-w",
            "json",
            "reshard",
            "--src-dir",
            dir.path().to_str().unwrap(),
            "--src-shards",
            "2",
            "--dst-dir",
            dst_dir.path().to_str().unwrap(),
            "--dst-shards",
            "4",
            "--yes",
        ])
        .output()
        .expect("运行 esctl");
    assert!(out.status.success(), "reshard -w json 失败: {}", err(&out));
    let json: serde_json::Value = serde_json::from_str(&stdout(&out)).expect("合法 JSON");
    assert_eq!(json["src_shards"], 2);
    assert_eq!(json["dst_shards"], 4);
    assert_eq!(json["src_streams"], json["dst_streams"], "流数应一致");
}

/// 发现 4：目标目录是全新路径（不存在）时无需预先 mkdir，
/// TreeBuilder 会创建（旧缺陷：canonicalize 对不存在路径报错，
/// 文档示例流程直接失败）
#[tokio::test(flavor = "multi_thread")]
async fn reshard_fresh_dst_no_precreate() {
    let (dir, total) = start_and_write().await;

    // dst 路径不存在：validate 的 canonicalize 曾对它报 NotFound
    let dst_dir = dir.path().join("dst-new");
    assert!(!dst_dir.exists(), "前置：目标目录必须不存在");

    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "2",
        "--dst-dir",
        dst_dir.to_str().unwrap(),
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert!(
        out.status.success(),
        "全新目标目录应直接创建并成功: {}",
        err(&out)
    );

    // 数据完整
    let (dst_streams, dst_events, _) = verify_dst(&dst_dir, 4).await;
    assert_eq!(dst_streams, 6, "目标流数应为 6");
    assert_eq!(dst_events, total, "目标事件数应等于写入数 {total}");
}

/// 发现 2：--src-shards 与数据目录实际布局不一致必须拒绝
/// （旧缺陷：少报分片数时，哈希落在枚举范围之外的分片数据被静默跳过，
/// 且 src/dst 计数来自同一枚举子集，完整性校验拦不住）
#[tokio::test(flavor = "multi_thread")]
async fn reshard_src_shard_mismatch_rejected() {
    let (dir, _) = start_and_write().await;
    let dst_dir = tempfile::tempdir().expect("目标临时目录");

    // 数据是 2 分片布局（start_and_write 写满两个分片），少报为 1
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "1",
        "--dst-dir",
        dst_dir.path().to_str().unwrap(),
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert_eq!(out.status.code(), Some(1), "分片数不匹配应拒绝");
    assert!(err(&out).contains("不一致"), "{}", err(&out));

    // 目标目录不应被写入脏数据（拒绝发生在打开 dst 之前）
    assert_eq!(stdout(&out).len(), 0, "拒绝路径不应有 stdout");

    // 发现 13：失败路径必须 flush+close 已打开的 src tree——
    // 若 LOCK 未释放，紧接着的正确命令会因"LOCK 被占用"再次失败
    let out = esctl_offline(&[
        "reshard",
        "--src-dir",
        dir.path().to_str().unwrap(),
        "--src-shards",
        "2",
        "--dst-dir",
        dst_dir.path().to_str().unwrap(),
        "--dst-shards",
        "4",
        "--yes",
    ]);
    assert!(
        out.status.success(),
        "失败后 LOCK 应已释放、正确命令可直接执行: {}",
        err(&out)
    );
}
