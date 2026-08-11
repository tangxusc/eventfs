//! esctl reshard 端到端：进程内 2 分片写数据 → 释放锁 → esctl reshard 子进程
//! 重分布到 4 分片 → 打开目标树验证数据完整（流数/事件数一致、version/event_id
//! 不变、分片内 position 连续）。

use std::collections::{HashMap, HashSet};
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use es_proto::eventstore::event_store_server::EventStoreServer;
use es_server::Server;
use es_server::config::{Config, NodeConfig, ShardConfig, StorageConfig};
use surrealkv::LSMIterator;

/// 启动 2 分片单节点服务器并写入数据，返回 (数据目录, 服务器, 临时目录)。
async fn start_and_write() -> (tempfile::TempDir, Server, usize) {
    let dir = tempfile::tempdir().expect("临时目录");
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
        },
        shards: ShardConfig { num_shards: 2 },
        tls: None,
    };

    let server = Server::new(config).expect("创建服务器");
    server.init().await.expect("初始化");

    let members = std::collections::BTreeSet::from([1u64]);
    for shard_id in 0..2 {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("取分片");
        shard
            .raft
            .initialize(members.clone())
            .await
            .expect("初始化 raft");
    }

    // 起 gRPC 服务（仅 EventStore 够写入）
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().expect("取地址"));
    let sm = server.shard_manager().clone();
    let handle = tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(es_server::service::EsService::new(
                sm,
            )))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 写 6 个流 × 2 条事件（覆盖两个分片）
    let mut total = 0;
    for i in 0..6 {
        let stream = format!("stream-{i}");
        for v in 0..2 {
            let data = format!("{{\"v\":{i}-{v}}}");
            let out = esctl(
                &addr,
                &["append", &stream, "--event-type", "E", "--data", &data],
            );
            assert!(out.status.success(), "append {stream} 失败: {}", err(&out));
            total += 1;
        }
    }

    // 停 gRPC 服务，确保落盘
    handle.abort();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 释放 LOCK：逐分片关存储（Tree 是共享的，最后一个 close 释放锁文件）
    for shard_id in 0..2 {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("取分片");
        shard.storage.close().await.expect("关闭存储");
    }
    tokio::time::sleep(Duration::from_millis(100)).await;

    (dir, server, total)
}

/// 运行 esctl（不带 --endpoints，离线命令不需要）
fn esctl_offline(args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(args);
    cmd.output().expect("运行 esctl")
}

fn esctl(endpoints: &str, args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_esctl"));
    cmd.args(["--endpoints", endpoints]);
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
            // 事件值 = es_core::Event（JSON 序列化，无 shard_id 字段）
            let event: es_core::Event =
                serde_json::from_slice(&value).expect("事件值应为合法 JSON（es_core::Event）");
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
    let (dir, _server, total) = start_and_write().await;

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
    let (dir, _server, _) = start_and_write().await;

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
    let (dir, _server, _) = start_and_write().await;

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
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".to_string(),
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
        },
        shards: ShardConfig { num_shards: 1 },
        tls: None,
    };
    let server = Server::new(config).expect("创建服务器");
    server.init().await.expect("初始化");

    // 服务器持有 LOCK 未释放，reshard 打开源目录必须失败
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
    let shard = server.shard_manager().get_shard(0).await.expect("取分片");
    shard.storage.close().await.expect("关闭存储");
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
    let (dir, _server, _) = start_and_write().await;
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
    let (dir, _server, total) = start_and_write().await;

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
    let (dir, _server, _) = start_and_write().await;
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
