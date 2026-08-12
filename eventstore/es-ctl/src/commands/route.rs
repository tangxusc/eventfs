//! `esctl route`：查看/校准流路由表（stream → shard 归属）。

use anyhow::{Result, anyhow};

use es_core::route::RouteTable;

use crate::cli::{Format, RouteArgs};
use crate::commands::Ctx;
use crate::output;

/// 渲染路由表（纯函数，可单测）
pub fn render_route_table(table: &RouteTable, format: Format) -> String {
    match format {
        Format::Simple => {
            if table.streams.is_empty() {
                return format!("路由表为空（version={}）", table.version);
            }
            let mut lines = vec![format!("version={}", table.version)];
            for (stream, shard) in &table.streams {
                lines.push(format!("{stream} -> shard {shard}"));
            }
            lines.join("\n")
        }
        Format::Table => {
            let rows: Vec<Vec<String>> = table
                .streams
                .iter()
                .map(|(s, shard)| vec![s.clone(), shard.to_string()])
                .collect();
            let mut out = vec![output::render_table(&["STREAM", "SHARD"], &rows)];
            out.push(format!("version={}", table.version));
            out.join("\n")
        }
        Format::Json => serde_json::json!({
            "version": table.version,
            "streams": table.streams,
            "shard_stream_counts": table.shard_stream_counts,
        })
        .to_string(),
    }
}

pub async fn run(ctx: &Ctx, args: &RouteArgs) -> Result<()> {
    if args.check {
        return run_check(ctx).await;
    }
    // recount 优先（也返回校准后的表）；默认只展示
    let table = if args.recount {
        eprintln!("正在校准 per-shard 流计数...");
        ctx.cluster.recount_streams().await?
    } else {
        ctx.cluster.get_route_table().await?
    };
    println!("{}", render_route_table(&table, ctx.format));
    Ok(())
}

/// 孤儿流检测：枚举各分片实际存储的流，与路由表对比。
///
/// - **孤儿**：存储中有但路由表无记录——隐式建流跨节点竞态等场景的残留，
///   可用 `esctl migrate --stream <s> --to <shard>` 合并修复。
/// - **虚挂**：路由表指向的分片与存储实际所在不一致——迁移切换后未收敛
///   或路由表手工编辑出错，指向的写入会 NotFound。
async fn run_check(ctx: &Ctx) -> Result<()> {
    let table = ctx.cluster.get_route_table().await?;
    let scope = ctx.shards().await?;

    let mut orphans: Vec<(String, u64)> = Vec::new(); // (stream, 实际 shard)
    let mut phantom: Vec<(String, u64, u64)> = Vec::new(); // (stream, 实际, 路由表指向)
    for shard in scope.all_ids() {
        let streams = list_streams_from_shard(ctx, shard).await?;
        for s in streams {
            match table.lookup(&s) {
                Some(owner) if owner != shard => phantom.push((s, shard, owner)),
                None => orphans.push((s, shard)),
                _ => {} // 一致
            }
        }
    }

    match ctx.format {
        Format::Simple => {
            if orphans.is_empty() && phantom.is_empty() {
                println!("路由表与各分片存储一致（{} 个流）", table.streams.len());
            } else {
                for (s, shard) in &orphans {
                    println!("孤儿：{s} 存在于 shard {shard}，但路由表无记录");
                }
                for (s, actual, owner) in &phantom {
                    println!("虚挂：{s} 实际在 shard {actual}，路由表指向 shard {owner}");
                }
                eprintln!(
                    "发现 {} 个孤儿、{} 个虚挂；可用 migrate 合并孤儿流",
                    orphans.len(),
                    phantom.len()
                );
            }
        }
        Format::Table => {
            let rows: Vec<Vec<String>> = orphans
                .iter()
                .map(|(s, shard)| vec![s.clone(), shard.to_string(), "孤儿".into()])
                .chain(
                    phantom
                        .iter()
                        .map(|(s, actual, owner)| {
                            vec![s.clone(), actual.to_string(), format!("虚挂(指向 {owner})")]
                        }),
                )
                .collect();
            let out = if rows.is_empty() {
                format!("路由表与各分片存储一致（{} 个流）", table.streams.len())
            } else {
                output::render_table(&["STREAM", "SHARD", "问题"], &rows)
            };
            println!("{out}");
        }
        Format::Json => {
            let json = serde_json::json!({
                "orphans": orphans.iter().map(|(s, shard)| serde_json::json!({"stream": s, "shard": shard})).collect::<Vec<_>>(),
                "phantom": phantom.iter().map(|(s, a, o)| serde_json::json!({"stream": s, "actual_shard": a, "route_shard": o})).collect::<Vec<_>>(),
            });
            println!("{json}");
        }
    }
    Ok(())
}

/// 枚举 shard 上的全部流（打 shard leader）
async fn list_streams_from_shard(ctx: &Ctx, shard: u64) -> Result<Vec<String>> {
    let mut client = crate::commands::migrate::migration_client_to_leader(ctx, shard).await?;
    let resp = client
        .list_streams(es_proto::eventstore::ListStreamsRequest { shard_id: shard })
        .await
        .map_err(|e| anyhow!("枚举分片 {shard} 流失败: {e}"))?;
    Ok(resp.into_inner().stream_ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> RouteTable {
        let mut t = RouteTable::new();
        t.insert("a", 1);
        t.insert("b", 1);
        t.insert("c", 2);
        t
    }

    #[test]
    fn render_simple_lists_mapping() {
        let out = render_route_table(&table(), Format::Simple);
        assert!(out.contains("version=3"), "应含版本: {out}");
        assert!(out.contains("a -> shard 1"), "应含映射: {out}");
        assert!(out.contains("c -> shard 2"), "应含映射: {out}");
    }

    #[test]
    fn render_simple_empty() {
        let out = render_route_table(&RouteTable::new(), Format::Simple);
        assert!(out.contains("路由表为空"), "空表提示: {out}");
    }

    #[test]
    fn render_json_full() {
        let out = render_route_table(&table(), Format::Json);
        // serde_json 序列化无空格：键值紧邻
        assert!(out.contains("\"version\":3"), "应含版本: {out}");
        assert!(out.contains("\"a\":1"), "应含映射: {out}");
    }
}
