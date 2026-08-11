//! `esctl status`：各端点健康与分片归属视图。

use anyhow::{Result, anyhow};

use crate::cli::{Format, StatusArgs};
use crate::commands::Ctx;
use crate::output;

pub async fn run(ctx: &Ctx, _args: &StatusArgs) -> Result<()> {
    let scope = ctx.shards().await?;
    let shard_ids = scope.all_ids();

    // 每端点 × 分片探测 GetRaftState，聚合可达性、leader 归属、term
    let mut rows: Vec<(String, bool, Vec<u64>, Vec<u64>, u64)> = Vec::new();
    let mut any_reachable = false;

    for ep in ctx.cluster.endpoints() {
        let mut reachable = false;
        let mut leader_of = Vec::new();
        let mut following_of = Vec::new();
        let mut max_term = 0u64;

        for shard_id in &shard_ids {
            match ctx.cluster.get_raft_state_via(ep, *shard_id).await {
                Ok(state) => {
                    reachable = true;
                    max_term = max_term.max(state.current_term);
                    if state.is_leader {
                        leader_of.push(*shard_id);
                    } else if state.has_leader {
                        following_of.push(*shard_id);
                    }
                }
                Err(_) => {} // 该分片不可达/未初始化：不计数
            }
        }

        if reachable {
            any_reachable = true;
        }
        rows.push((ep.clone(), reachable, leader_of, following_of, max_term));
    }

    if !any_reachable {
        return Err(anyhow!(
            "全部端点不可达：{}",
            ctx.cluster.endpoints().join(", ")
        ));
    }

    let leader_text = |ids: &[u64]| {
        if ids.is_empty() {
            "[]".to_string()
        } else {
            format!(
                "[{}]",
                ids.iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
    };

    match ctx.format {
        Format::Simple => {
            for (ep, reachable, leader_of, following_of, term) in &rows {
                println!(
                    "{ep}: reachable={reachable}, leader_of={}, following_of={}, term={term}",
                    leader_text(leader_of),
                    leader_text(following_of)
                );
            }
        }
        Format::Table => {
            let table_rows: Vec<Vec<String>> = rows
                .iter()
                .map(|(ep, reachable, leader_of, following_of, term)| {
                    vec![
                        ep.clone(),
                        if *reachable {
                            "yes".into()
                        } else {
                            "no".into()
                        },
                        leader_text(leader_of),
                        leader_text(following_of),
                        term.to_string(),
                    ]
                })
                .collect();
            println!(
                "{}",
                output::render_table(
                    &["ENDPOINT", "REACHABLE", "LEADER_OF", "FOLLOWING_OF", "TERM"],
                    &table_rows
                )
                .trim_end()
            );
        }
        Format::Json => {
            let eps: Vec<serde_json::Value> = rows
                .iter()
                .map(|(ep, reachable, leader_of, following_of, term)| {
                    serde_json::json!({
                        "endpoint": ep,
                        "reachable": reachable,
                        "leader_of": leader_of,
                        "following_of": following_of,
                        "term": term,
                    })
                })
                .collect();
            println!("{}", serde_json::json!({ "endpoints": eps }));
        }
    }
    Ok(())
}
