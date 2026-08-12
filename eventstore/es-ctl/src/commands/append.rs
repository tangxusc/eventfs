//! `esctl append`：追加事件到流（乐观并发）。

use anyhow::{Result, anyhow, bail};
use es_proto::eventstore::*;
use uuid::Uuid;

use crate::cli::{AppendArgs, ExpectedVersionArg, Format};
use crate::commands::Ctx;
use crate::output;

/// 期望版本参数转 proto 请求字段
pub fn to_expected_version(ev: &ExpectedVersionArg) -> ExpectedVersion {
    let kind = match ev {
        ExpectedVersionArg::Any => expected_version::Kind::Any(Empty {}),
        ExpectedVersionArg::NoStream => expected_version::Kind::NoStream(Empty {}),
        ExpectedVersionArg::StreamExists => expected_version::Kind::StreamExists(Empty {}),
        ExpectedVersionArg::Exact(v) => expected_version::Kind::Exact(*v),
    };
    ExpectedVersion { kind: Some(kind) }
}

/// 读取数据载荷：--data 字符串优先，--data-file 读文件字节。
fn load_payload(inline: &Option<String>, file: &Option<std::path::PathBuf>) -> Result<Vec<u8>> {
    match (inline, file) {
        (Some(s), None) => Ok(s.as_bytes().to_vec()),
        (None, Some(path)) => {
            std::fs::read(path).map_err(|e| anyhow!("读取数据文件 {} 失败: {e}", path.display()))
        }
        _ => unreachable!("clap 参数组已保证二选一"),
    }
}

/// 把 leader 写失败翻译为中文提示（乐观冲突时给出实际版本）
fn translate_append_error(e: anyhow::Error) -> anyhow::Error {
    let msg = e.to_string();
    if let Some(actual) = msg.strip_prefix("optimistic conflict: actual_version=") {
        anyhow!("乐观并发冲突：流实际版本为 {actual}，与期望版本不符")
    } else {
        e
    }
}

pub async fn run(ctx: &Ctx, args: &AppendArgs) -> Result<()> {
    let data = load_payload(&args.data, &args.data_file)?;
    let metadata = match (&args.metadata, &args.metadata_file) {
        (Some(s), None) => Some(s.as_bytes().to_vec()),
        (None, Some(path)) => Some(
            std::fs::read(path)
                .map_err(|e| anyhow!("读取元数据文件 {} 失败: {e}", path.display()))?,
        ),
        (None, None) => None,
        _ => unreachable!("clap 参数组已保证互斥"),
    };

    let event_id = match &args.event_id {
        Some(s) => Uuid::parse_str(s).map_err(|e| anyhow!("非法事件 ID {s:?}: {e}"))?,
        None => Uuid::new_v4(),
    };

    // 预显示路由分片（仅供提示，实际以服务端落盘分片为准）。
    // count=0 时 route 取模除零会 panic：clap 已拒绝显式 --shards 0，
    // 但探测路径（集群未初始化、探测到 0 分片）也可能返回 0，这里兜底。
    let scope = ctx.shards().await?;
    if scope.is_empty() {
        bail!("分片数为 0（集群未初始化或探测失败），无法路由：请用 --shards 指定分片数");
    }
    let route_shard = es_core::routing::route(&args.stream, scope.count());

    let new_event = NewEvent {
        event_id: event_id.as_bytes().to_vec(),
        event_type: args.event_type.clone(),
        data,
        metadata: metadata.unwrap_or_default(),
    };

    let resp = ctx
        .cluster
        .with_leader(route_shard, |mut client| {
            let req = AppendRequest {
                stream_id: args.stream.clone(),
                expected_version: Some(to_expected_version(&args.expected_version)),
                events: vec![new_event.clone()],
            };
            async move { client.append(req).await.map(|r| r.into_inner()) }
        })
        .await
        .map_err(translate_append_error)?;

    let out = match ctx.format {
        Format::Simple => format!(
            "OK 写入成功\nstream: {}\nshard: {}\nnext_expected_version: {}\nfirst_position: {}\nlast_position: {}",
            args.stream,
            resp.shard_id,
            resp.next_expected_version,
            resp.first_position,
            resp.last_position,
        ),
        Format::Table => output::render_table(
            &["STREAM", "SHARD", "NEXT_VERSION", "FIRST_POS", "LAST_POS"],
            &[vec![
                args.stream.clone(),
                resp.shard_id.to_string(),
                resp.next_expected_version.to_string(),
                resp.first_position.to_string(),
                resp.last_position.to_string(),
            ]],
        )
        .trim_end()
        .to_string(),
        Format::Json => serde_json::json!({
            "stream_id": args.stream,
            "shard_id": resp.shard_id,
            "next_expected_version": resp.next_expected_version,
            "first_position": resp.first_position,
            "last_position": resp.last_position,
        })
        .to_string(),
    };
    println!("{out}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_version_maps_to_proto_four() {
        let cases = [
            (
                ExpectedVersionArg::Any,
                expected_version::Kind::Any(Empty {}),
            ),
            (
                ExpectedVersionArg::NoStream,
                expected_version::Kind::NoStream(Empty {}),
            ),
            (
                ExpectedVersionArg::StreamExists,
                expected_version::Kind::StreamExists(Empty {}),
            ),
            (
                ExpectedVersionArg::Exact(7),
                expected_version::Kind::Exact(7),
            ),
        ];
        for (arg, expect) in cases {
            let ev = to_expected_version(&arg);
            match (ev.kind, expect) {
                (Some(expected_version::Kind::Any(_)), expected_version::Kind::Any(_))
                | (
                    Some(expected_version::Kind::NoStream(_)),
                    expected_version::Kind::NoStream(_),
                )
                | (
                    Some(expected_version::Kind::StreamExists(_)),
                    expected_version::Kind::StreamExists(_),
                ) => {}
                (Some(expected_version::Kind::Exact(v)), expected_version::Kind::Exact(e)) => {
                    assert_eq!(v, e)
                }
                _ => panic!("kind 不匹配"),
            }
        }
    }

    #[test]
    fn optimistic_conflict_error_translated() {
        let e = anyhow!("optimistic conflict: actual_version=5");
        let t = translate_append_error(e);
        assert!(t.to_string().contains("乐观并发冲突"), "{}", t);
        assert!(t.to_string().contains('5'));
    }

    #[test]
    fn non_conflict_error_preserved() {
        let e = anyhow!("unavailable: connection refused");
        assert_eq!(
            translate_append_error(e).to_string(),
            "unavailable: connection refused"
        );
    }

    #[test]
    fn payload_inline_string_preferred() {
        assert_eq!(
            load_payload(&Some("abc".into()), &None).expect("字符串"),
            b"abc"
        );
    }
}
