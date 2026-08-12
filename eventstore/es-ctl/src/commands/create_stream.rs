//! `esctl create-stream`：显式创建流，服务端分配 shard（大致最少流）。

use anyhow::Result;

use crate::cli::{CreateStreamArgs, Format};
use crate::commands::Ctx;
use crate::output;

/// 渲染创建结果（纯函数，可单测）
pub fn render_create(
    stream: &str,
    resp: &es_proto::eventstore::CreateStreamResponse,
    format: Format,
) -> String {
    match format {
        Format::Simple => {
            let verb = if resp.exists { "已存在" } else { "创建成功" };
            let leader = if resp.leader_addr.is_empty() {
                String::new()
            } else {
                format!("\nleader_addr: {}", resp.leader_addr)
            };
            format!("{verb}\nstream: {stream}\nshard: {}{leader}", resp.shard_id)
        }
        Format::Table => {
            let rows = vec![vec![
                stream.to_string(),
                resp.shard_id.to_string(),
                if resp.exists { "exists" } else { "created" }.into(),
                resp.leader_addr.clone(),
            ]];
            output::render_table(&["STREAM", "SHARD", "STATUS", "LEADER_ADDR"], &rows)
        }
        Format::Json => serde_json::json!({
            "stream_id": stream,
            "shard_id": resp.shard_id,
            "leader_addr": resp.leader_addr,
            "exists": resp.exists,
        })
        .to_string(),
    }
}

pub async fn run(ctx: &Ctx, args: &CreateStreamArgs) -> Result<()> {
    let resp = ctx.cluster.create_stream(&args.stream).await?;
    println!("{}", render_create(&args.stream, &resp, ctx.format));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(shard: u64, leader: &str, exists: bool) -> es_proto::eventstore::CreateStreamResponse {
        es_proto::eventstore::CreateStreamResponse {
            shard_id: shard,
            leader_addr: leader.into(),
            exists,
        }
    }

    #[test]
    fn render_simple_new_stream() {
        let out = render_create("order-1", &resp(3, "http://127.0.0.1:50052", false), Format::Simple);
        assert!(out.contains("创建成功"), "新流应标创建成功: {out}");
        assert!(out.contains("stream: order-1"), "应含流名: {out}");
        assert!(out.contains("shard: 3"), "应含 shard: {out}");
        assert!(out.contains("leader_addr: http://127.0.0.1:50052"), "应含地址: {out}");
    }

    #[test]
    fn render_simple_existing_no_leader() {
        let out = render_create("s", &resp(1, "", true), Format::Simple);
        assert!(out.contains("已存在"), "已有流应标已存在: {out}");
        assert!(!out.contains("leader_addr"), "无地址不应输出该行: {out}");
    }

    #[test]
    fn render_table_shape() {
        let out = render_create("s", &resp(2, "", true), Format::Table);
        assert!(out.contains("STREAM"), "应有表头");
        assert!(out.contains("exists"), "已有流标 exists");
    }

    #[test]
    fn render_json_fields() {
        let out = render_create("s", &resp(5, "http://x", true), Format::Json);
        // serde_json 序列化无空格：键值紧邻
        assert!(out.contains("\"stream_id\":\"s\""), "应含 stream_id: {out}");
        assert!(out.contains("\"shard_id\":5"), "应含 shard_id: {out}");
        assert!(out.contains("\"exists\":true"), "应含 exists: {out}");
    }
}
