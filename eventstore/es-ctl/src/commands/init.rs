//! `esctl init`：初始化分片集群（把给定成员写入首条 membership 日志）。

use anyhow::{Result, anyhow};
use es_proto::endpoint::normalize_endpoint;
use es_proto::eventstore::*;

use crate::cli::{Format, InitArgs};
use crate::commands::Ctx;
use crate::output;

/// 目标分片列表：--all-shards 用全部分片，否则单分片（默认 0）
fn target_shards(
    ctx: &Ctx,
    args: &InitArgs,
) -> impl std::future::Future<Output = Result<Vec<u64>>> {
    async move {
        if args.all_shards {
            Ok(ctx.shards().await?.all_ids())
        } else {
            Ok(vec![args.shard.unwrap_or(0)])
        }
    }
}

pub async fn run(ctx: &Ctx, args: &InitArgs) -> Result<()> {
    let shards = target_shards(ctx, args).await?;
    let members: Vec<RaftMember> = args
        .member
        .iter()
        .map(|m| RaftMember {
            node_id: m.node_id,
            addr: normalize_endpoint(&m.addr),
        })
        .collect();

    // --all-shards 部分初始化场景：已初始化的分片逐片告警（init_shard 内）并
    // 继续补完其余分片，全部尝试完后若有失败再整体报错——不能用 `?` 在第一个
    // 已初始化分片处中断，否则其余分片永远不会被初始化。
    let mut failures: Vec<String> = Vec::new();
    for shard_id in shards {
        if let Err(e) = init_shard(ctx, shard_id, &members).await {
            failures.push(format!("分片 {shard_id}: {e:#}"));
        }
    }
    if !failures.is_empty() {
        return Err(anyhow!("部分分片初始化失败：{}", failures.join("；")));
    }
    Ok(())
}

/// 初始化单个分片：依序尝试各端点，第一个成功即止。
///
/// initialize 不需要 leader（把成员写入本节点首条日志），但要求节点日志为空；
/// 已初始化的分片返回 failed_precondition（openraft NotAllowed），
/// 视为"已初始化"，打印告警并以退出码 1 结束——避免静默重放。
async fn init_shard(ctx: &Ctx, shard_id: u64, members: &[RaftMember]) -> Result<()> {
    let mut last_err: Option<String> = None;

    for ep in ctx.cluster.rotated_endpoints() {
        let mut client = ctx.cluster.admin_client(&ep).await?;
        let req = InitializeRequest {
            shard_id,
            members: members.to_vec(),
        };
        match client.initialize(req).await {
            Ok(_) => {
                let member_text = members
                    .iter()
                    .map(|m| format!("{}@{}", m.node_id, m.addr))
                    .collect::<Vec<_>>()
                    .join(",");
                match ctx.format {
                    Format::Simple => {
                        println!("OK 分片 {shard_id} 已初始化：members=[{member_text}]")
                    }
                    Format::Table => println!(
                        "{}",
                        output::render_table(
                            &["SHARD", "STATUS", "MEMBERS"],
                            &[vec![
                                shard_id.to_string(),
                                "initialized".into(),
                                member_text.clone()
                            ]]
                        )
                        .trim_end()
                    ),
                    Format::Json => println!(
                        "{}",
                        serde_json::json!({
                            "shard_id": shard_id,
                            "status": "initialized",
                            "members": members.iter().map(|m| format!("{}@{}", m.node_id, m.addr)).collect::<Vec<_>>(),
                        })
                    ),
                }
                return Ok(());
            }
            Err(status)
                if status.code() == tonic::Code::FailedPrecondition
                    || status.code() == tonic::Code::AlreadyExists =>
            {
                eprintln!(
                    "警告：分片 {shard_id} 可能已初始化（端点 {ep}：{}）",
                    status.message()
                );
                return Err(anyhow!("分片 {shard_id} 初始化失败：{}", status.message()));
            }
            Err(status) => {
                last_err = Some(format!("{ep}: {}", status.message()));
            }
        }
    }

    Err(anyhow!(
        "分片 {shard_id} 初始化失败：所有端点不可用或已初始化（{}）",
        last_err.unwrap_or_else(|| "无错误".into())
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn member_addr_normalized() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:50052"),
            "http://127.0.0.1:50052"
        );
        assert_eq!(normalize_endpoint("https://x:1"), "https://x:1");
    }
}
