//! `esctl migrate`：在线迁移流到目标分片（流的数据处理不暂停）。
//!
//! 取代旧 `esctl reshard`（离线全量重分布）。状态机：
//!
//! ```text
//! Preparing → FullCopying → Tailing → Switching → Draining → Verifying → Finalizing
//! ```
//!
//! - FullCopying/Tailing/Draining 都从「目标当前版本」读源补差（Exact 版本链
//!   写目标，幂等索引防重放）——断点续传天然成立，重复执行无害。
//! - Switching 是原子切换点（SetStreamShard，路由表版本 +1 广播）。
//! - 切换后 Drain 阶段：客户端新写直达目标（路由已切），复制从目标当前
//!   版本续，天然兼容并发写入；收敛判据 = 目标版本 ≥ 源版本且源连续安静。
//! - Verifying 失败自动回切路由（源数据从未被动过，安全）。
//! - Finalizing 删除源分片数据（幂等 no-op）。
//!
//! 全部操作经 Migration 服务原语（显式 shard 寻址），读走源 leader、
//! 写走目标 leader（leader 由 GetRaftState 探测定位）。

use std::time::Duration;

use anyhow::{Result, anyhow, bail};

use es_proto::eventstore::migration_client::MigrationClient;
use es_proto::eventstore::*;
use es_proto::tls::TlsClientConfig;
use tonic::transport::Channel;

use crate::cli::{Format, MigrateArgs};
use crate::commands::Ctx;
use crate::output;

/// 全量复制批大小（每批最大事件数）
const COPY_BATCH: u64 = 500;
/// 排水轮询间隔
const DRAIN_POLL: Duration = Duration::from_millis(2000);
/// 写操作重试次数（leader 变更/选举中）
const WRITE_RETRIES: usize = 10;
/// 全量复制「追平」判定：连续 N 轮复制后源版本无增长即视为追平
const FULLCOPY_QUIET_ROUNDS: u32 = 3;
/// 全量复制轮数上限：持续生产（复制速率 < 生产速率）时强制切换，
/// 排水阶段路由已切、源写入停止，剩余差量可在排水期收敛
const FULLCOPY_MAX_ROUNDS: u32 = 100;
/// copy_range 读空批连续上限（源数据短暂不可见的竞态，超过即报错）
const EMPTY_READ_LIMIT: u32 = 30;
/// verify/finalize 分页大小（read_stream_from_shard 已按块流式发送）
const PAGE_SIZE: u64 = 500;

/// 迁移结果报告（逐流）
pub(crate) struct MigrateReport {
    stream: String,
    src_shard: u64,
    dst_shard: u64,
    copied: u64,
    dry_run: bool,
}

/// 单个流的迁移状态机。
///
/// 返回迁移的事件数。任何阶段失败返回 Err（已切换时数据无害，可重跑排水；
/// 未切换时重跑从 Preparing 开始）。重跑是自愈的：路由已指向目标但源有
/// 残留数据（上次切换后中断）时，自动进入排水收尾而非拒绝。
async fn migrate_stream(
    ctx: &Ctx,
    stream: &str,
    dst_shard: u64,
    dry_run: bool,
    drain_quiet_rounds: u32,
    drain_timeout: Duration,
) -> Result<MigrateReport> {
    // ---- Preparing ----
    let route = ctx.cluster.get_route_table().await?;
    let routed_shard = route.lookup(stream);
    // 源分片定位：路由表优先；无记录（孤儿流：跨节点隐式建流竞态残留）
    // → 枚举分片自动定位
    let src_shard = match routed_shard {
        Some(s) => s,
        None => match find_stream_shard(ctx, stream).await? {
            Some(s) => {
                eprintln!("孤儿流 {stream}：路由表无记录，已在分片 {s} 发现数据，按孤儿迁移处理");
                s
            }
            None => bail!("流 {stream} 不在路由表中且任何分片无其数据（未创建？）"),
        },
    };

    if src_shard == dst_shard {
        // 路由已指向目标（上次迁移切换后中断/崩溃）：扫描其它分片找残留源数据
        match find_stream_shard_excluding(ctx, stream, dst_shard).await? {
            Some(residual) => {
                eprintln!(
                    "路由已指向目标分片，但分片 {residual} 仍有该流数据（上次迁移中断），继续排水收尾"
                );
                if dry_run {
                    println!("dry-run: 残留源分片 {residual} 的数据待收尾到 {dst_shard}");
                    return Ok(MigrateReport {
                        stream: stream.into(),
                        src_shard: residual,
                        dst_shard,
                        copied: 0,
                        dry_run: true,
                    });
                }
                return complete_migration(ctx, stream, residual, dst_shard, drain_quiet_rounds, drain_timeout).await;
            }
            None => {
                eprintln!("流 {stream} 已在分片 {dst_shard}，无残留数据，无需迁移");
                return Ok(MigrateReport {
                    stream: stream.into(),
                    src_shard: dst_shard,
                    dst_shard,
                    copied: 0,
                    dry_run,
                });
            }
        }
    }

    // 目标分片存在性：集群探测（ListShards 并集）
    let exists = {
        let scope = ctx.shards().await?;
        scope.all_ids().contains(&dst_shard)
    };
    if !exists {
        bail!("目标分片 {dst_shard} 不存在（不在放置表中）");
    }

    // 源/目标元数据（版本差报告与断点续传起点）
    let src_meta = get_meta(ctx, src_shard, stream).await?;
    let dst_meta = get_meta(ctx, dst_shard, stream).await?;
    let src_v = match src_meta {
        Some(m) => m.current_version,
        None => {
            eprintln!("源分片 {src_shard} 无该流数据（可能此前已完成迁移），跳过");
            return Ok(MigrateReport {
                stream: stream.into(),
                src_shard,
                dst_shard,
                copied: 0,
                dry_run,
            });
        }
    };
    let dst_v = dst_meta.map(|m| m.current_version).unwrap_or(0);
    if dry_run {
        println!(
            "dry-run: {stream} src=shard{src_shard}(v{src_v}) dst=shard{dst_shard}(v{dst_v}) 待复制 {} 条",
            src_v.saturating_sub(dst_v)
        );
        return Ok(MigrateReport {
            stream: stream.into(),
            src_shard,
            dst_shard,
            copied: 0,
            dry_run: true,
        });
    }

    // ---- FullCopying + Tailing：追平源当前版本 ----
    // 从「目标当前版本 +1」读源补差（目标无流时 from=0，NoStream 创建）。
    // 追平判据：复制完成后源版本无增长（连续 QUIET 轮）→ 切换。
    // 持续生产（复制速率 < 生产速率）由轮数上限兜底——强制切换后排水
    // 阶段路由已切、源写入停止，剩余差量在排水期收敛。
    let mut quiet = 0u32;
    let mut round = 0u32;
    while round < FULLCOPY_MAX_ROUNDS {
        round += 1;
        let d_meta = get_meta(ctx, dst_shard, stream).await?;
        let from = d_meta.map(|m| m.current_version + 1).unwrap_or(0);
        let s = get_meta(ctx, src_shard, stream)
            .await?
            .map(|m| m.current_version)
            .unwrap_or(0);
        if from > s {
            quiet += 1;
            if quiet >= FULLCOPY_QUIET_ROUNDS {
                break; // 追平
            }
            continue;
        }
        copy_range(ctx, stream, src_shard, dst_shard, from, s, false).await?;
        quiet = 0;
    }
    if round >= FULLCOPY_MAX_ROUNDS {
        eprintln!(
            "全量复制达到轮数上限（源持续生产，复制速率低于生产速率），强制切换——排水阶段收敛"
        );
    }

    // ---- Switching：原子切换路由 ----
    set_stream_shard(ctx, stream, dst_shard).await?;
    eprintln!("已切换路由：{stream} → shard {dst_shard}");

    // ---- Draining / Verifying / Finalizing ----
    complete_migration(ctx, stream, src_shard, dst_shard, drain_quiet_rounds, drain_timeout).await
}

/// 迁移收尾（Drain → Verify → Finalize）。切换后与「重跑自愈」共用。
async fn complete_migration(
    ctx: &Ctx,
    stream: &str,
    src_shard: u64,
    dst_shard: u64,
    drain_quiet_rounds: u32,
    drain_timeout: Duration,
) -> Result<MigrateReport> {
    // ---- Draining：补切换窗口内源侧增量（客户端新写已直达目标） ----
    let deadline = std::time::Instant::now() + drain_timeout;
    let mut quiet = 0u32;
    loop {
        let d_opt = get_meta(ctx, dst_shard, stream)
            .await?
            .map(|m| m.current_version);
        let s = get_meta(ctx, src_shard, stream)
            .await?
            .map(|m| m.current_version)
            .unwrap_or(0);
        if d_opt.unwrap_or(0) >= s {
            quiet += 1;
            if quiet >= drain_quiet_rounds {
                break; // 收敛
            }
        } else {
            quiet = 0;
            let from = d_opt.map(|v| v + 1).unwrap_or(0);
            copy_range(ctx, stream, src_shard, dst_shard, from, s, true).await?;
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "排水超时（源仍在产生数据或广播未收敛）：可重跑本命令完成排水（数据无害）"
            );
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }

    // ---- Verifying：源 ⊆ 目标（event_id 匹配 + 载荷/时间戳保真） ----
    if let Err(e) = verify(ctx, stream, src_shard, dst_shard).await {
        // 校验失败：回切路由（源数据从未被动过）
        eprintln!("校验失败，回切路由到源分片...");
        let _ = set_stream_shard(ctx, stream, src_shard).await;
        return Err(e);
    }

    // ---- Finalizing：删除源分片数据 ----
    // 删除前最后检查：verify 之后可能又有陈旧路由的写入落到源（广播
    // 尽力而为），补差一轮再删，缩小「确认后丢失」窗口
    tokio::time::sleep(Duration::from_millis(500)).await;
    let s_after = get_meta(ctx, src_shard, stream)
        .await?
        .map(|m| m.current_version)
        .unwrap_or(0);
    let d_after = get_meta(ctx, dst_shard, stream)
        .await?
        .map(|m| m.current_version)
        .unwrap_or(0);
    if s_after > d_after {
        copy_range(ctx, stream, src_shard, dst_shard, d_after + 1, s_after, true).await?;
    }
    delete_from_shard(ctx, src_shard, stream).await?;

    let total = get_meta(ctx, dst_shard, stream)
        .await?
        .map(|m| m.current_version + 1)
        .unwrap_or(0);
    Ok(MigrateReport {
        stream: stream.into(),
        src_shard,
        dst_shard,
        copied: total,
        dry_run: false,
    })
}

/// 从源读 `[from, to]`（version 闭区间）并逐条写入目标（Exact 版本链；
/// 排水阶段 `allow_conflict_any=true` 时冲突改用 Any 兜底）。
///
/// `version` 是源版本游标（Any 兜底后目标 version 可能重排，游标不受影响）。
async fn copy_range(
    ctx: &Ctx,
    stream: &str,
    src_shard: u64,
    dst_shard: u64,
    from: u64,
    to: u64,
    allow_conflict_any: bool,
) -> Result<()> {
    let mut version = from;
    let mut empty_reads = 0u32;
    while version <= to {
        // 批量读源（本地存储读，打源 leader 最稳；服务端已按块流式发送）
        let batch = read_from_shard(ctx, src_shard, stream, version, COPY_BATCH).await?;
        if batch.is_empty() {
            // 读空 = 源侧还没到该版本（复制与生产的竞态），短暂重试；
            // 连续为空说明源数据不可见（流被并发删除等），报错退出防死循环
            empty_reads += 1;
            if empty_reads >= EMPTY_READ_LIMIT {
                bail!(
                    "读源分片 {src_shard} 流 {stream} 连续 {empty_reads} 次为空（源数据不可见？）"
                );
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        empty_reads = 0;
        for ev in batch {
            let expected = expected_version_of(version);
            append_migrated(ctx, dst_shard, stream, expected, &ev, allow_conflict_any).await?;
            version += 1;
        }
    }
    Ok(())
}

/// 期望版本：流不存在（version 0）用 NoStream，其余 Exact(version-1)
fn expected_version_of(version: u64) -> ExpectedVersion {
    if version == 0 {
        ExpectedVersion {
            kind: Some(expected_version::Kind::NoStream(Empty {})),
        }
    } else {
        ExpectedVersion {
            kind: Some(expected_version::Kind::Exact(version - 1)),
        }
    }
}

/// 读源 shard 的流区间（显式 shard，不走路由表）
async fn read_from_shard(
    ctx: &Ctx,
    shard: u64,
    stream: &str,
    from: u64,
    max: u64,
) -> Result<Vec<Event>> {
    let mut client = migration_client_to_leader(ctx, shard).await?;
    let resp = client
        .read_stream_from_shard(ReadStreamFromShardRequest {
            shard_id: shard,
            stream_id: stream.to_string(),
            from_version: from,
            max_count: max,
        })
        .await
        .map_err(|e| anyhow!("读源分片 {shard} 失败: {e}"))?;
    let mut events = Vec::new();
    let mut stream = resp.into_inner();
    while let Some(r) = stream.message().await? {
        events.extend(r.events);
    }
    Ok(events)
}

/// 读源/目标 shard 的流元数据
async fn get_meta(ctx: &Ctx, shard: u64, stream: &str) -> Result<Option<GetStreamMetaResponse>> {
    let mut client = migration_client_to_leader(ctx, shard).await?;
    let resp = client
        .get_stream_meta_from_shard(GetStreamMetaFromShardRequest {
            shard_id: shard,
            stream_id: stream.to_string(),
        })
        .await
        .map_err(|e| anyhow!("读分片 {shard} 元数据失败: {e}"))?;
    let r = resp.into_inner();
    Ok(if r.exists { Some(r) } else { None })
}

/// 写目标 shard（单事件，Exact 版本链，重试 leader 定位）。
///
/// 排水阶段 `allow_conflict_any=true`：路由已切后客户端并发写入直达目标，
/// 可能占用 Exact 期望的版本槽——冲突时改用 Any 重试（目标分配新版本，
/// 事件载荷/event_id/hlc 保真，version 允许重排；verify 按 event_id 比对）。
/// 全量复制阶段（切换前）无并发写入，冲突即真实错误，直接上抛。
async fn append_migrated(
    ctx: &Ctx,
    shard: u64,
    stream: &str,
    expected: ExpectedVersion,
    ev: &Event,
    allow_conflict_any: bool,
) -> Result<()> {
    let hlc = ev
        .hlc
        .clone()
        .ok_or_else(|| anyhow!("事件缺少 hlc（迁移保真要求源 HLC）"))?;
    let event = MigratedEvent {
        event_id: ev.event_id.clone(),
        event_type: ev.event_type.clone(),
        data: ev.data.clone(),
        metadata: ev.metadata.clone(),
        hlc: Some(hlc.clone()),
    };
    let mut last_err = None;
    for _ in 0..WRITE_RETRIES {
        let mut client = migration_client_to_leader(ctx, shard).await?;
        let req = AppendMigratedRequest {
            shard_id: shard,
            stream_id: stream.to_string(),
            expected_version: Some(expected.clone()),
            event: Some(event.clone()),
        };
        match client.append_migrated(req).await {
            Ok(_) => return Ok(()),
            Err(e) if e.code() == tonic::Code::FailedPrecondition && allow_conflict_any => {
                // 版本槽被客户端并发写入占用：Any 兜底重试（数据保真优先）
                let any_req = AppendMigratedRequest {
                    shard_id: shard,
                    stream_id: stream.to_string(),
                    expected_version: Some(ExpectedVersion {
                        kind: Some(expected_version::Kind::Any(Empty {})),
                    }),
                    event: Some(event.clone()),
                };
                match client.append_migrated(any_req).await {
                    Ok(_) => return Ok(()),
                    Err(e2) if e2.code() == tonic::Code::Unavailable => {
                        last_err = Some(e2);
                        tokio::time::sleep(Duration::from_millis(300)).await;
                    }
                    Err(e2) => {
                        return Err(anyhow!("迁移写入（Any 兜底）失败: {e2}"));
                    }
                }
            }
            Err(e) if e.code() == tonic::Code::Unavailable => {
                // leader 变更/选举中 → 重定位重试
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
            Err(e) => return Err(anyhow!("迁移写入失败: {e}")),
        }
    }
    Err(anyhow!(
        "迁移写入多次重试失败：{}",
        last_err.map(|e| e.to_string()).unwrap_or_default()
    ))
}

/// 删除源 shard 的流（幂等）
async fn delete_from_shard(ctx: &Ctx, shard: u64, stream: &str) -> Result<()> {
    let mut client = migration_client_to_leader(ctx, shard).await?;
    client
        .delete_stream_from_shard(DeleteStreamFromShardRequest {
            shard_id: shard,
            stream_id: stream.to_string(),
        })
        .await
        .map_err(|e| anyhow!("删除源分片数据失败: {e}"))?;
    Ok(())
}

/// 切换路由（任意节点执行，版本仲裁收敛）
async fn set_stream_shard(ctx: &Ctx, stream: &str, shard: u64) -> Result<()> {
    let mut client = ctx.cluster.migration_client(&ctx.cluster.pick_endpoint()).await?;
    client
        .set_stream_shard(SetStreamShardRequest {
            stream_id: stream.to_string(),
            shard_id: shard,
        })
        .await
        .map_err(|e| anyhow!("切换路由失败: {e}"))?;
    Ok(())
}

/// 源 ⊆ 目标校验：源的全部事件必须存在于目标，且载荷/时间戳保真。
///
/// - 按 event_id 匹配（复制幂等，重放安全）；不做数量相等断言——切换后
///   客户端新写直达目标，目标可能比源多（源是旧数据，Finalizing 才删）。
/// - **内容保真**：比对 hlc/event_type/data/metadata——复制中载荷截断或
///   篡改必须在校验阶段拦截（否则 Finalizing 删除源后损坏成为唯一副本）。
/// - version 允许不同：排水阶段客户端并发写入可能使目标版本重排
///   （Any 兜底路径），版本差异不代表数据缺失。
/// - 分页读取：整条流可能超过 8MB 单消息上限（服务端已分块流式发送）。
async fn verify(ctx: &Ctx, stream: &str, src_shard: u64, dst_shard: u64) -> Result<()> {
    let src = read_all_paged(ctx, src_shard, stream).await?;
    let dst = read_all_paged(ctx, dst_shard, stream).await?;
    let dst_by_id: std::collections::HashMap<String, &Event> =
        dst.iter().map(|e| (hex(&e.event_id), e)).collect();
    for a in &src {
        let key = hex(&a.event_id);
        match dst_by_id.get(&key) {
            Some(b) => {
                if a.hlc != b.hlc
                    || a.event_type != b.event_type
                    || a.data != b.data
                    || a.metadata != b.metadata
                {
                    bail!(
                        "事件不一致（event_id {key}，version {}）：源与目标载荷/时间戳不同——复制损坏",
                        a.version
                    );
                }
            }
            None => bail!(
                "源事件缺失于目标（event_id {key}，version {}）——复制遗漏",
                a.version
            ),
        }
    }
    Ok(())
}

/// 分页读完整条流（服务端 read_stream_from_shard 已按块流式发送，
/// 客户端逐块收集，避免 8MB 单消息上限）
async fn read_all_paged(ctx: &Ctx, shard: u64, stream: &str) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    let mut from = 0u64;
    loop {
        let batch = read_from_shard(ctx, shard, stream, from, PAGE_SIZE).await?;
        if batch.is_empty() {
            break; // 读尽
        }
        from += batch.len() as u64;
        events.extend(batch);
    }
    Ok(events)
}

/// 枚举分片查找流所在 shard（孤儿流定位 / 重跑自愈的残留源检测）
async fn find_stream_shard(ctx: &Ctx, stream: &str) -> Result<Option<u64>> {
    find_stream_shard_excluding(ctx, stream, u64::MAX).await
}

/// 枚举分片查找流所在 shard，排除指定 shard
async fn find_stream_shard_excluding(
    ctx: &Ctx,
    stream: &str,
    exclude: u64,
) -> Result<Option<u64>> {
    let scope = ctx.shards().await?;
    for shard in scope.all_ids() {
        if shard == exclude {
            continue;
        }
        let streams = list_streams_from_shard_local(ctx, shard).await?;
        if streams.iter().any(|s| s == stream) {
            return Ok(Some(shard));
        }
    }
    Ok(None)
}

/// 枚举 shard 上的全部流（打 shard leader；migrate 内部用，route check 用 route.rs 的版本）
async fn list_streams_from_shard_local(ctx: &Ctx, shard: u64) -> Result<Vec<String>> {
    let mut client = migration_client_to_leader(ctx, shard).await?;
    let resp = client
        .list_streams(ListStreamsRequest { shard_id: shard })
        .await
        .map_err(|e| anyhow!("枚举分片 {shard} 流失败: {e}"))?;
    Ok(resp.into_inner().stream_ids)
}

/// 定位 shard leader 并构建 Migration 客户端（写/读统一走 leader）
pub(crate) async fn migration_client_to_leader(
    ctx: &Ctx,
    shard: u64,
) -> Result<MigrationClient<Channel>> {
    let leader = ctx
        .cluster
        .find_shard_leader(shard)
        .await
        .ok_or_else(|| anyhow!("分片 {shard} 无 leader（集群选举中？）"))?;
    ctx.cluster.migration_client(&leader).await
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// 渲染迁移报告（纯函数）
pub(crate) fn render_report(r: &MigrateReport, format: Format) -> String {
    match format {
        Format::Simple => format!(
            "{}: shard {} → {}，{} 条事件{}",
            r.stream,
            r.src_shard,
            r.dst_shard,
            r.copied,
            if r.dry_run { "（dry-run）" } else { "" }
        ),
        Format::Table => {
            let rows = vec![vec![
                r.stream.clone(),
                r.src_shard.to_string(),
                r.dst_shard.to_string(),
                r.copied.to_string(),
                if r.dry_run { "dry-run".into() } else { "done".into() },
            ]];
            output::render_table(&["STREAM", "FROM", "TO", "EVENTS", "STATUS"], &rows)
        }
        Format::Json => serde_json::json!({
            "stream": r.stream,
            "src_shard": r.src_shard,
            "dst_shard": r.dst_shard,
            "events": r.copied,
            "dry_run": r.dry_run,
        })
        .to_string(),
    }
}

/// 批量迁移（--shard）：枚举源分片全部流，逐流独立状态机
async fn migrate_shard(
    ctx: &Ctx,
    src_shard: u64,
    dst_shard: u64,
    dry_run: bool,
    drain_quiet_rounds: u32,
    drain_timeout: Duration,
) -> Result<Vec<MigrateReport>> {
    let streams = list_streams_from_shard_local(ctx, src_shard).await?;
    if streams.is_empty() {
        println!("分片 {src_shard} 无流，无事可做");
        return Ok(Vec::new());
    }
    let mut reports = Vec::new();
    let mut failed = 0;
    for s in &streams {
        match migrate_stream(ctx, s, dst_shard, dry_run, drain_quiet_rounds, drain_timeout).await
        {
            Ok(r) => reports.push(r),
            Err(e) => {
                failed += 1;
                eprintln!("迁移 {s} 失败：{e:#}");
            }
        }
    }
    if failed > 0 {
        bail!("{failed} 个流迁移失败（其余成功；失败项可单独重跑）");
    }
    Ok(reports)
}

pub async fn run(ctx: &Ctx, args: &MigrateArgs) -> Result<()> {
    let drain_timeout = Duration::from_secs(args.drain_timeout_secs);

    let reports = match (&args.stream, args.shard) {
        (Some(stream), _) => {
            vec![migrate_stream(
                ctx,
                stream,
                args.to,
                args.dry_run,
                args.drain_quiet_rounds,
                drain_timeout,
            )
            .await?]
        }
        (None, Some(shard)) => {
            if shard == args.to {
                bail!("源分片与目标分片相同");
            }
            migrate_shard(
                ctx,
                shard,
                args.to,
                args.dry_run,
                args.drain_quiet_rounds,
                drain_timeout,
            )
            .await?
        }
        (None, None) => bail!("必须指定 --stream 或 --shard"),
    };

    for r in &reports {
        println!("{}", render_report(r, ctx.format));
    }
    if !args.dry_run && !reports.is_empty() {
        println!("\n迁移完成。建议执行 `esctl route recount` 校准流计数。");
    }
    Ok(())
}

// 以下类型只用于对齐 proto 导入（rustfmt 友好）
#[allow(unused)]
fn _type_alignment(_: TlsClientConfig) {}
