//! `esctl read` / `esctl readall`：读取流事件与跨分片 $all 流。

use anyhow::{Context, Result};
use es_proto::eventstore::*;

use crate::cli::{Format, ReadAllArgs, ReadArgs};
use crate::commands::{self, Ctx};

/// 方向参数转 proto 枚举值
fn direction(backward: bool) -> i32 {
    if backward {
        Direction::Backward as i32
    } else {
        Direction::Forward as i32
    }
}

pub async fn run(ctx: &Ctx, args: &ReadArgs) -> Result<()> {
    // 反向读：from_version 未显式指定（默认 0）时传 u64::MAX 表示「从最新往回」
    // （服务端约定，state_machine::read_stream_events_backward）。
    let from_version = if args.backward && args.from_version == 0 {
        u64::MAX
    } else {
        args.from_version
    };

    let events = ctx
        .cluster
        .with_any_endpoint(|mut client| {
            let req = ReadStreamRequest {
                stream_id: args.stream.clone(),
                from_version,
                max_count: args.max_count,
                direction: direction(args.backward),
            };
            async move {
                let stream = client.read_stream(req).await?.into_inner();
                commands::collect_events(stream)
                    .await
                    .map_err(|e| tonic::Status::internal(e.to_string()))
            }
        })
        .await
        .context("读取流失败")?;

    println!("{}", commands::render_events(ctx.format, &events));
    Ok(())
}

pub async fn run_all(ctx: &Ctx, args: &ReadAllArgs) -> Result<()> {
    let scope = ctx.shards().await?;

    // from_positions 非空时覆盖 shard_ids 与 from_position（proto 语义）
    let (shard_ids, from_positions): (Vec<u64>, Vec<ShardPosition>) =
        match (&args.from_positions, &args.shard_ids) {
            (Some(positions), _) => {
                let ids = positions.0.iter().map(|(s, _)| *s).collect();
                let sps = positions
                    .0
                    .iter()
                    .map(|(s, p)| ShardPosition {
                        shard_id: *s,
                        from_position: *p,
                    })
                    .collect();
                (ids, sps)
            }
            (None, Some(ids)) => (ids.0.clone(), vec![]),
            (None, None) => (scope.all_ids(), vec![]),
        };

    let events = ctx
        .cluster
        .with_any_endpoint(|mut client| {
            let req = ReadAllRequest {
                shard_ids: shard_ids.clone(),
                from_position: args.from_position,
                max_count: args.max_count,
                direction: direction(args.backward),
                from_positions: from_positions.clone(),
            };
            async move {
                let stream = client.read_all(req).await?.into_inner();
                commands::collect_events(stream)
                    .await
                    .map_err(|e| tonic::Status::internal(e.to_string()))
            }
        })
        .await
        .context("读取 $all 失败")?;

    // 翻页：max_count 生效且本页取满时给出续读游标
    let count = events.len();
    if args.max_count > 0 && count as u64 >= args.max_count {
        let next = commands::next_from_positions(&events, args.backward);
        match ctx.format {
            Format::Json => {
                // json 只输出一行：取满时把续读游标并入同一对象
                let mut value: serde_json::Value =
                    serde_json::from_str(&commands::render_events(ctx.format, &events))
                        .context("解析 JSON")?;
                let next_text: Vec<String> = next.iter().map(|(s, p)| format!("{s}:{p}")).collect();
                if let serde_json::Value::Object(ref mut obj) = value {
                    obj.insert("next_from_positions".into(), serde_json::json!(next_text));
                }
                println!("{value}");
            }
            Format::Simple | Format::Table => {
                println!("{}", commands::render_events(ctx.format, &events));
                eprintln!(
                    "# 下一页：esctl readall --from-positions \"{}\"",
                    commands::from_positions_text(&next)
                );
            }
        }
    } else {
        println!("{}", commands::render_events(ctx.format, &events));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 方向参数映射() {
        assert_eq!(direction(false), Direction::Forward as i32);
        assert_eq!(direction(true), Direction::Backward as i32);
    }
}
