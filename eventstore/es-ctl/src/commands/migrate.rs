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
/// 未切换时重跑从 Preparing 开始）。
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
    let src_shard = route
        .lookup(stream)
        .ok_or_else(|| anyhow!("流 {stream} 不在路由表中（未创建？）"))?;
    if src_shard == dst_shard {
        bail!("流 {stream} 已在分片 {dst_shard}，无需迁移");
    }
    // 目标分片存在性：路由表计数键或集群探测（ListShards 并集）
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
    let src_v = src_meta.map(|m| m.current_version).unwrap_or(u64::MAX); // 源不存在：无事可做
    let dst_v = dst_meta.map(|m| m.current_version).unwrap_or(0);
    if src_v == u64::MAX {
        return Ok(MigrateReport {
            stream: stream.into(),
            src_shard,
            dst_shard,
            copied: 0,
            dry_run,
        });
    }
    if dry_run {
        println!(
            "dry-run: {stream} src=shard{src_shard}(v{src_v}) dst=shard{dst_shard}(v{dst_v}) 待复制 {} 条",
            src_v.saturating_sub(dst_v.saturating_sub(0)).saturating_sub(0)
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
    // 循环从「目标当前版本 +1」读源补差，直到源与目标版本一致。
    // 目标无流时 from=0（NoStream 创建），有流时 from=current+1（Exact 链）
    loop {
        let d_meta = get_meta(ctx, dst_shard, stream).await?;
        let from = d_meta.map(|m| m.current_version + 1).unwrap_or(0);
        let s = get_meta(ctx, src_shard, stream)
            .await?
            .map(|m| m.current_version)
            .unwrap_or(0);
        if from > s {
            break; // 追平
        }
        copy_range(ctx, stream, src_shard, dst_shard, from, s).await?;
    }

    // ---- Switching：原子切换路由 ----
    set_stream_shard(ctx, stream, dst_shard).await?;
    eprintln!("已切换路由：{stream} → shard {dst_shard}");

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
            copy_range(ctx, stream, src_shard, dst_shard, from, s).await?;
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "排水超时（源仍在产生数据或广播未收敛）：可重跑本命令完成排水（数据无害）"
            );
        }
        tokio::time::sleep(DRAIN_POLL).await;
    }

    // ---- Verifying：双向全量对比 ----
    if let Err(e) = verify(ctx, stream, src_shard, dst_shard).await {
        // 校验失败：回切路由（源数据从未被动过）
        eprintln!("校验失败，回切路由到源分片...");
        let _ = set_stream_shard(ctx, stream, src_shard).await;
        return Err(e);
    }

    // ---- Finalizing：删除源分片数据 ----
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

/// 从源读 `[from, to]`（version 闭区间）并逐条写入目标（Exact 版本链）。
async fn copy_range(
    ctx: &Ctx,
    stream: &str,
    src_shard: u64,
    dst_shard: u64,
    from: u64,
    to: u64,
) -> Result<()> {
    let mut version = from;
    while version <= to {
        // 批量读源（本地存储读，打源 leader 最稳）
        let batch = read_from_shard(ctx, src_shard, stream, version, COPY_BATCH).await?;
        if batch.is_empty() {
            // 读空 = 源侧还没到该版本（正常：FullCopy 与 Tailing 间竞态），
            // 稍后重试；调用方循环以版本收敛为准
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        for ev in batch {
            let expected = expected_version_of(version);
            append_migrated(ctx, dst_shard, stream, expected, &ev).await?;
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

/// 写目标 shard（单事件，Exact 版本链，重试 leader 定位）
async fn append_migrated(ctx: &Ctx, shard: u64, stream: &str, expected: ExpectedVersion, ev: &Event) -> Result<()> {
    let hlc = ev
        .hlc
        .clone()
        .ok_or_else(|| anyhow!("事件缺少 hlc（迁移保真要求源 HLC）"))?;
    let mut last_err = None;
    for _ in 0..WRITE_RETRIES {
        let mut client = migration_client_to_leader(ctx, shard).await?;
        let req = AppendMigratedRequest {
            shard_id: shard,
            stream_id: stream.to_string(),
            expected_version: Some(expected.clone()),
            event: Some(MigratedEvent {
                event_id: ev.event_id.clone(),
                event_type: ev.event_type.clone(),
                data: ev.data.clone(),
                metadata: ev.metadata.clone(),
                hlc: Some(hlc.clone()),
            }),
        };
        match client.append_migrated(req).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                // Unavailable（leader 变更/选举中）→ 重定位重试；其余上抛
                if e.code() != tonic::Code::Unavailable {
                    return Err(anyhow!("迁移写入失败: {e}"));
                }
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
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

/// 双向对比：源的全部事件必须都存在于目标（event_id/version/hlc 一致）。
///
/// 不做数量相等断言——切换后客户端新写直达目标，目标可能比源多
/// （源是旧数据，Finalizing 才删除）。校验失败 = 复制遗漏（数据丢失）。
async fn verify(ctx: &Ctx, stream: &str, src_shard: u64, dst_shard: u64) -> Result<()> {
    let src = read_from_shard(ctx, src_shard, stream, 0, 0).await?;
    let dst = read_from_shard(ctx, dst_shard, stream, 0, 0).await?;
    let dst_by_id: std::collections::HashMap<Vec<u8>, &Event> =
        dst.iter().map(|e| (e.event_id.clone(), e)).collect();
    for a in &src {
        match dst_by_id.get(&a.event_id) {
            Some(b) => {
                if a.version != b.version || a.hlc != b.hlc {
                    bail!(
                        "事件不一致（version {}）：源 hlc={:?}，目标 hlc={:?}",
                        a.version,
                        a.hlc,
                        b.hlc
                    );
                }
            }
            None => bail!(
                "源事件缺失于目标（version {}，event_id {}）——复制遗漏",
                a.version,
                hex(&a.event_id)
            ),
        }
    }
    Ok(())
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
    let mut client = migration_client_to_leader(ctx, src_shard).await?;
    let resp = client
        .list_streams(ListStreamsRequest { shard_id: src_shard })
        .await
        .map_err(|e| anyhow!("枚举分片 {src_shard} 流失败: {e}"))?;
    let streams = resp.into_inner().stream_ids;
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
