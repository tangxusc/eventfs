//! `esctl route`：查看/校准流路由表（stream → shard 归属）。

use anyhow::Result;

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
