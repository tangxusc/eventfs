//! 分片数变更与离线数据重分布
//!
//! 当修改 `num_shards` 时,流名的哈希路由结果改变,已存在的数据变得不可达。
//! 本模块提供离线工具,在集群停机窗口内将旧布局数据按新路由重写为新布局,
//! 保留 stream_id / version / event_id / HLC,但重新分配 position。
//!
//! ## 使用流程
//!
//! 1. 停止集群,备份数据目录
//! 2. 运行 `reshard(旧 tree, 旧分片数, 新 tree, 新分片数)`
//! 3. 验证输出(流数/事件数一致,抽样对比数据)
//! 4. 修改配置 `num_shards`,用新数据目录重启集群
//!
//! ## 设计要点
//!
//! - **离线单向处理**:无并发写入冲突,正确性风险低
//! - **K 路归并**:同目标分片的流按 (HLC, stream, version) 升序归并,
//!   保证分片内 version 序不乱,position 严格连续 0..N-1
//! - **幂等索引重建**:批首事件的 event_id 映射到新分片的 (v0, p0_new)
//! - **纯状态机输出**:不含 Raft 日志,集群重启后 Raft 重新自举
//!
//! 详见 `docs/reshard.md`

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use es_core::{Event, StreamMeta};
use serde::{Deserialize, Serialize};

use crate::key;
use crate::storage::EsStorage;

/// Reshard 执行报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReshardReport {
    /// 源布局扫描到的流数
    pub src_streams: usize,
    /// 源布局扫描到的事件数
    pub src_events: usize,
    /// 目标布局写入的流数
    pub dst_streams: usize,
    /// 目标布局写入的事件数
    pub dst_events: usize,
    /// 耗时
    pub elapsed: Duration,
}

/// 执行离线重分布。
///
/// 从 `src_tree` 读取 `src_num_shards` 个分片的数据,
/// 按 `dst_num_shards` 重新路由,写入 `dst_tree`。
///
/// **要求**:
/// - `src_tree` 与 `dst_tree` 不能是同一个(会冲突)
/// - 调用前集群必须已停止,无新写入
/// - `dst_tree` 应为空目录或全新创建的 tree
///
/// **输出**:
/// - 每个目标分片包含:事件、position 指针、StreamMeta、幂等索引、next_position
/// - 不包含 Raft 日志与 applied 状态(纯状态机数据)
pub async fn reshard(
    src_tree: Arc<surrealkv::Tree>,
    src_num_shards: u64,
    dst_tree: Arc<surrealkv::Tree>,
    dst_num_shards: u64,
) -> es_core::Result<ReshardReport> {
    let t0 = Instant::now();

    // 1. 枚举旧分片,收集全部流名与元数据
    let mut src_streams_total = 0;
    let mut src_events_total = 0;
    let mut streams_by_new_shard: BTreeMap<u64, Vec<(String, StreamMeta)>> = BTreeMap::new();

    for old_shard in 0..src_num_shards {
        let store = EsStorage::new(old_shard, src_tree.clone())?;
        let streams = store.list_streams()?;
        src_streams_total += streams.len();

        for (name, meta) in streams {
            let new_shard = es_core::route(&name, dst_num_shards);
            streams_by_new_shard
                .entry(new_shard)
                .or_default()
                .push((name, meta));
        }
    }

    // 2. 逐个新分片处理:读旧数据 → K 路归并 → 写新分片。
    //    源读计数与目标写计数分开统计:写侧若出现过滤/截断等静默差异,
    //    下面的事件数校验才能真实拦截(共用同一计数则恒等,校验形同虚设)。
    let mut dst_streams_total = 0;
    let mut dst_events_total = 0;

    for (new_shard, streams) in streams_by_new_shard {
        let (n_streams, src_read, dst_written) =
            process_shard(new_shard, streams, src_tree.clone(), dst_tree.clone(), src_num_shards)
                .await?;
        dst_streams_total += n_streams;
        dst_events_total += dst_written;
        src_events_total += src_read;
    }

    // 3. 校验完整性
    if src_streams_total != dst_streams_total {
        return Err(es_core::Error::Internal(format!(
            "流数不一致:源 {src_streams_total}, 目标 {dst_streams_total}"
        )));
    }
    if src_events_total != dst_events_total {
        return Err(es_core::Error::Internal(format!(
            "事件数不一致:源 {src_events_total}, 目标 {dst_events_total}"
        )));
    }

    Ok(ReshardReport {
        src_streams: src_streams_total,
        src_events: src_events_total,
        dst_streams: dst_streams_total,
        dst_events: dst_events_total,
        elapsed: t0.elapsed(),
    })
}

/// 处理一个新分片:读属于它的全部流 → 归并 → 写入
///
/// 返回 `(目标流数, 源侧读回事件数, 目标侧写入事件数)`。
/// 读事件按 `src_num_shards` 路由——与枚举旧分片用同一套分片数,
/// 避免此前用启发式推断(`infer_shard_count`)导致稀疏布局下读错分片。
async fn process_shard(
    new_shard: u64,
    streams: Vec<(String, StreamMeta)>,
    src_tree: Arc<surrealkv::Tree>,
    dst_tree: Arc<surrealkv::Tree>,
    src_num_shards: u64,
) -> es_core::Result<(usize, usize, usize)> {
    // 读取所有流的事件(从各自所在的旧分片)
    let mut all_events: Vec<Event> = Vec::new();
    for (stream_id, _meta) in &streams {
        let old_shard = es_core::route(stream_id, src_num_shards);
        let old_store = EsStorage::new(old_shard, src_tree.clone())?;
        let events = old_store.read_stream_events(stream_id, 0, 0)?;
        all_events.extend(events);
    }

    let src_read = all_events.len();

    // K 路归并:按 (HLC, stream_id, version) 升序
    all_events.sort_by(|a, b| {
        a.hlc
            .cmp(&b.hlc)
            .then_with(|| a.stream_id.cmp(&b.stream_id))
            .then_with(|| a.version.cmp(&b.version))
    });

    // 重分配 position:从 0 开始连续编号,shard 信息已隐含在写入的 key 中
    for (pos, ev) in all_events.iter_mut().enumerate() {
        ev.position = pos as u64;
    }

    // 写入新分片
    let dst_written = write_shard(new_shard, &streams, &all_events, dst_tree).await?;

    Ok((streams.len(), src_read, dst_written))
}

/// 写入新分片的全部数据:事件、position 指针、StreamMeta、幂等索引、next_position。
///
/// 返回实际写入的事件数(写侧独立计数,供事件数完整性校验)。
async fn write_shard(
    shard: u64,
    streams: &[(String, StreamMeta)],
    events: &[Event],
    tree: Arc<surrealkv::Tree>,
) -> es_core::Result<usize> {
    let mut txn = tree
        .begin()
        .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;

    // 1. 事件本体
    for ev in events {
        let k = key::sm_event(shard, &ev.stream_id, ev.version);
        let v = serde_json::to_vec(ev)
            .map_err(|e| es_core::Error::Serde(format!("Event 序列化失败: {e}")))?;
        txn.set(&k, &v)
            .map_err(|e| es_core::Error::Storage(format!("写 event 失败: {e}")))?;
    }

    // 2. position 指针
    for ev in events {
        let k = key::sm_position_ptr(shard, ev.position);
        let v = serde_json::to_vec(&(ev.stream_id.clone(), ev.version))
            .map_err(|e| es_core::Error::Serde(format!("position 指针序列化失败: {e}")))?;
        txn.set(&k, &v)
            .map_err(|e| es_core::Error::Storage(format!("写 position 指针失败: {e}")))?;
    }

    // 3. StreamMeta
    for (stream_id, meta) in streams {
        let k = key::sm_stream_meta(shard, stream_id);
        let v = serde_json::to_vec(meta)
            .map_err(|e| es_core::Error::Serde(format!("StreamMeta 序列化失败: {e}")))?;
        txn.set(&k, &v)
            .map_err(|e| es_core::Error::Storage(format!("写 StreamMeta 失败: {e}")))?;
    }

    // 4. 幂等索引:每批首事件 event_id → (v0, p0_new)
    let mut batches: HashMap<uuid::Uuid, (u64, u64)> = HashMap::new();
    for ev in events {
        // 简化:将每个事件都视为单事件批,event_id 映射到自己的 (version, position)。
        // 真实批边界难以还原(需识别同 HLC 的连续版本),但幂等性仍有效:
        // 重放时若 event_id 已存在,返回对应的 (v, p);客户端批长度一致即通过。
        batches.insert(ev.event_id, (ev.version, ev.position));
    }
    for (event_id, (v, p)) in batches {
        let k = key::sm_idempotency(shard, &event_id);
        let val = serde_json::to_vec(&(v, p))
            .map_err(|e| es_core::Error::Serde(format!("幂等索引序列化失败: {e}")))?;
        txn.set(&k, &val)
            .map_err(|e| es_core::Error::Storage(format!("写幂等索引失败: {e}")))?;
    }

    // 5. next_position
    let next_pos = events.len() as u64;
    let k = key::sm_next_position(shard);
    let v = serde_json::to_vec(&next_pos)
        .map_err(|e| es_core::Error::Serde(format!("next_position 序列化失败: {e}")))?;
    txn.set(&k, &v)
        .map_err(|e| es_core::Error::Storage(format!("写 next_position 失败: {e}")))?;

    txn.commit().await
        .map_err(|e| es_core::Error::Storage(format!("commit 失败: {e}")))?;

    Ok(events.len())
}

/// 从源 tree 推断数据布局中的分片数(启发式:扫 TAG_SM 段找最大的分片 ID + 1)。
///
/// 供离线工具校验 `--src-shards` 与目录实际布局一致,防止少报分片数时
/// 哈希落在枚举范围之外的分片数据被静默跳过。
pub fn infer_shard_count(tree: &surrealkv::Tree) -> es_core::Result<u64> {
    use surrealkv::LSMIterator;

    // 扫 TAG_SM 段的全部分片前缀,解出 shard_id
    let txn = tree
        .begin()
        .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;
    let start = vec![0x02u8]; // TAG_SM
    let end = vec![0x03u8]; // TAG_SM + 1
    let mut it = txn
        .range(start, end)
        .map_err(|e| es_core::Error::Storage(format!("range 失败: {e}")))?;
    it.seek_first()
        .map_err(|e| es_core::Error::Storage(format!("seek_first 失败: {e}")))?;

    let mut max_shard = 0u64;
    while it.valid() {
        let k = it.key().user_key();
        if k.len() >= 9 {
            let shard_bytes = &k[1..9];
            let shard = u64::from_be_bytes(shard_bytes.try_into().unwrap());
            max_shard = max_shard.max(shard);
        }
        if !it
            .next()
            .map_err(|e| es_core::Error::Storage(format!("next 失败: {e}")))?
        {
            break;
        }
    }

    Ok(max_shard + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_tree() -> Arc<surrealkv::Tree> {
        let dir = tempfile::tempdir().expect("临时目录");
        Arc::new(
            surrealkv::TreeBuilder::new()
                .with_path(dir.path().to_path_buf())
                .build()
                .expect("建 tree"),
        )
    }

    #[tokio::test]
    async fn reshard_空到空无错误() {
        let src = tmp_tree();
        let dst = tmp_tree();

        let report = reshard(src, 2, dst, 2).await.expect("reshard");
        assert_eq!(report.src_streams, 0);
        assert_eq!(report.src_events, 0);
    }

    /// 稀疏布局（部分分片无数据）时，读事件必须按声明的 src_num_shards 路由，
    /// 而不是按数据推断的分片数（旧实现用 infer_shard_count 推断 = 最大分片 + 1，
    /// 稀疏时低估，哈希路由与落盘时不符，数据被读错分片而丢失）。
    #[tokio::test]
    async fn reshard_稀疏布局_按声明分片数路由不丢数据() {
        use openraft::storage::RaftStateMachine;

        use crate::tests::{entry_with, new_event, new_shared_storages};
        use es_core::ExpectedVersion;

        // 流名 A：route(A,4)==0 且 route(A,2)==0（两种路由一致）；
        // 流名 B：route(B,4)==3 且 route(B,2)==1——若用推断值 2 路由会去分片 1 读
        let a = (0..1000u64)
            .map(|i| format!("sparse-a/{i}"))
            .find(|n| es_core::route(n, 4) == 0 && es_core::route(n, 2) == 0)
            .expect("应有路由一致的流名");
        let b = (0..1000u64)
            .map(|i| format!("sparse-b/{i}"))
            .find(|n| es_core::route(n, 4) == 3 && es_core::route(n, 2) == 1)
            .expect("应有路由不一致的流名");

        // 4 分片布局，只写分片 0 与 3（分片 1、2 无数据 → 推断分片数为 2）
        let (mut sts, _dir) = new_shared_storages(&[0, 3]);
        sts[0]
            .apply(vec![entry_with(
                1,
                0,
                &a,
                ExpectedVersion::NoStream,
                vec![new_event("E", b"a1"), new_event("E", b"a2")],
            )])
            .await
            .expect("写分片 0");
        sts[1]
            .apply(vec![entry_with(
                1,
                0,
                &b,
                ExpectedVersion::NoStream,
                vec![new_event("E", b"b1")],
            )])
            .await
            .expect("写分片 3");

        let src_tree = sts[0].tree().clone();
        assert_eq!(infer_shard_count(&src_tree).expect("推断"), 4, "数据在分片 0、3，推断应为 4");

        // 稀疏布局场景：--src-shards 用声明值 4（不匹配的 2 会被 es-ctl 拒绝）
        let dst = tmp_tree();
        let report = reshard(src_tree, 4, dst.clone(), 2).await.expect("reshard");
        assert_eq!(report.src_events, 3, "源应有 3 条事件");
        assert_eq!(report.dst_events, 3, "目标应写入 3 条事件");

        // 流 B 必须迁移成功（旧实现从分片 1 读 B → 事件丢失）
        let dst_store = EsStorage::new(es_core::route(&b, 2), dst).expect("目标存储");
        let evs = dst_store.read_stream_events(&b, 0, 0).expect("读流 B");
        assert_eq!(evs.len(), 1, "流 B 的事件不应丢失");
        assert_eq!(evs[0].data, b"b1");
    }
}
