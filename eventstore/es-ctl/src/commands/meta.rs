//! `esctl meta`：查询流元数据（当前版本、所在分片）。

use anyhow::{Context, Result};
use es_proto::eventstore::*;

use crate::cli::{Format, MetaArgs};
use crate::commands::Ctx;
use crate::output;

/// 元数据响应渲染为输出文本（纯函数，可单测）
pub fn render_meta(format: Format, resp: &GetStreamMetaResponse) -> String {
    match format {
        Format::Simple => {
            if resp.exists {
                format!(
                    "exists: true\ncurrent_version: {}\nshard_id: {}",
                    resp.current_version, resp.shard_id
                )
            } else {
                "exists: false".into()
            }
        }
        Format::Table => {
            let rows: Vec<Vec<String>> = if resp.exists {
                vec![
                    vec!["exists".into(), "true".into()],
                    vec!["current_version".into(), resp.current_version.to_string()],
                    vec!["shard_id".into(), resp.shard_id.to_string()],
                ]
            } else {
                vec![vec!["exists".into(), "false".into()]]
            };
            output::render_table(&["FIELD", "VALUE"], &rows)
                .trim_end()
                .to_string()
        }
        Format::Json => serde_json::json!({
            "exists": resp.exists,
            "current_version": resp.current_version,
            "shard_id": resp.shard_id,
        })
        .to_string(),
    }
}

pub async fn run(ctx: &Ctx, args: &MetaArgs) -> Result<()> {
    let resp = ctx
        .cluster
        .with_any_endpoint(|mut client| {
            let req = GetStreamMetaRequest {
                stream_id: args.stream.clone(),
            };
            async move { client.get_stream_meta(req).await.map(|r| r.into_inner()) }
        })
        .await
        .context("查询流元数据失败")?;

    println!("{}", render_meta(ctx.format, &resp));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resp(exists: bool, version: u64, shard: u64) -> GetStreamMetaResponse {
        GetStreamMetaResponse {
            exists,
            current_version: version,
            shard_id: shard,
        }
    }

    #[test]
    fn simple_exists_and_missing() {
        assert_eq!(
            render_meta(Format::Simple, &resp(true, 4, 3)),
            "exists: true\ncurrent_version: 4\nshard_id: 3"
        );
        assert_eq!(
            render_meta(Format::Simple, &resp(false, 0, 0)),
            "exists: false"
        );
    }

    #[test]
    fn table_two_states() {
        let t = render_meta(Format::Table, &resp(true, 4, 3));
        assert!(t.contains("FIELD"), "{t}");
        assert!(t.contains("current_version"), "{t}");
        let t = render_meta(Format::Table, &resp(false, 0, 0));
        assert!(t.contains("false"), "{t}");
    }

    #[test]
    fn json_structure() {
        let json: serde_json::Value =
            serde_json::from_str(&render_meta(Format::Json, &resp(true, 4, 3))).expect("JSON");
        assert_eq!(json["exists"], true);
        assert_eq!(json["current_version"], 4);
        assert_eq!(json["shard_id"], 3);
        let json: serde_json::Value =
            serde_json::from_str(&render_meta(Format::Json, &resp(false, 0, 0))).expect("JSON");
        assert_eq!(json["exists"], false);
    }
}
