//! `esctl member`：集群成员管理（add / remove / list）。

use std::collections::BTreeSet;

use anyhow::{Result, anyhow, bail};
use es_proto::endpoint::normalize_endpoint;
use es_proto::eventstore::*;

use crate::cli::{Format, MemberAddArgs, MemberArgs, MemberListArgs, MemberRemoveArgs};
use crate::commands::Ctx;
use crate::output;

pub async fn run(ctx: &Ctx, args: &MemberArgs) -> Result<()> {
    match &args.action {
        crate::cli::MemberAction::Add(a) => run_add(ctx, a).await,
        crate::cli::MemberAction::Remove(a) => run_remove(ctx, a).await,
        crate::cli::MemberAction::List(a) => run_list(ctx, a).await,
    }
}

/// 目标分片列表：--all-shards 用全部分片，否则单分片（默认 0）
async fn target_shards(ctx: &Ctx, all_shards: bool, shard: Option<u64>) -> Result<Vec<u64>> {
    if all_shards {
        Ok(ctx.shards().await?.all_ids())
    } else {
        Ok(vec![shard.unwrap_or(0)])
    }
}

/// 输出成员变更结果
fn print_change(shard_id: u64, node_id: u64, format: Format, detail: &str, ok: bool) {
    match format {
        Format::Simple => println!("OK 节点 {node_id} {detail}（分片 {shard_id}）"),
        Format::Table => println!(
            "{}",
            output::render_table(
                &["SHARD", "NODE", "ACTION", "STATUS"],
                &[vec![
                    shard_id.to_string(),
                    node_id.to_string(),
                    detail.into(),
                    if ok { "ok".into() } else { "failed".into() }
                ]]
            )
            .trim_end()
        ),
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "shard_id": shard_id,
                "node_id": node_id,
                "action": detail,
                "status": if ok { "ok" } else { "failed" },
            })
        ),
    }
}

pub async fn run_add(ctx: &Ctx, args: &MemberAddArgs) -> Result<()> {
    let shards = target_shards(ctx, args.all_shards, args.shard).await?;
    let member = RaftMember {
        node_id: args.member.node_id,
        addr: normalize_endpoint(&args.member.addr),
    };

    for shard_id in shards {
        // 第一步：加为 learner（错误不带 leader 提示，必须先 find_leader）
        ctx.cluster
            .with_admin_leader(shard_id, |mut client| {
                let req = AddLearnerRequest {
                    shard_id,
                    member: Some(member.clone()),
                    blocking: !args.no_blocking,
                };
                async move { client.add_learner(req).await.map(|r| r.into_inner()) }
            })
            .await
            .map_err(|e| anyhow!("分片 {shard_id} 添加 learner 失败：{e:#}"))?;

        if args.learner_only {
            print_change(
                shard_id,
                args.member.node_id,
                ctx.format,
                "已加入分片为 learner",
                true,
            );
            continue;
        }

        // 第二步：提升为投票成员（CAS 读-改-写）。
        // 读 voters → 提交「期望 = 刚读到的快照」整体放在闭包内：
        // CAS 冲突（FailedPrecondition）时 with_admin_leader 重试闭包并重读 voters，
        // 并发变更不会被后到者静默覆盖。
        let voters_text = ctx
            .cluster
            .with_admin_leader(shard_id, |mut client| {
                let node_id = args.member.node_id;
                async move {
                    // admin client 已指向 leader 端点，直接读当前 voters 作 CAS 期望
                    let state = client
                        .get_raft_state(GetRaftStateRequest { shard_id })
                        .await?
                        .into_inner();
                    let mut voters: BTreeSet<u64> = state.voter_ids.iter().copied().collect();
                    voters.insert(node_id);
                    let voters_list = voters.iter().copied().collect::<Vec<_>>();
                    let req = ChangeMembershipRequest {
                        shard_id,
                        voter_ids: voters_list.clone(),
                        expected_voters: state.voter_ids,
                        retain: false,
                    };
                    client
                        .change_membership(req)
                        .await
                        .map(|r| (r.into_inner(), voters_list))
                }
            })
            .await
            .map_err(|e| anyhow!("分片 {shard_id} 提升成员失败：{e:#}"))?
            .1
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");

        print_change(
            shard_id,
            args.member.node_id,
            ctx.format,
            &format!("已提升为投票成员（voters=[{voters_text}]）"),
            true,
        );
    }
    Ok(())
}

pub async fn run_remove(ctx: &Ctx, args: &MemberRemoveArgs) -> Result<()> {
    let shards = target_shards(ctx, args.all_shards, args.shard).await?;

    for shard_id in shards {
        let (leader_ep, _) = ctx.cluster.find_leader(shard_id).await?;
        let state = ctx
            .cluster
            .get_raft_state_via(&leader_ep, shard_id)
            .await
            .map_err(|e| anyhow!("查询分片 {shard_id} 状态失败：{}", e.message()))?;

        let voters: BTreeSet<u64> = state.voter_ids.iter().copied().collect();
        if !voters.contains(&args.node_id) {
            bail!(
                "分片 {shard_id} 的投票成员为 [{}]，节点 {} 不在其中（learner 无法移除，无对应 RPC）",
                voters
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                args.node_id
            );
        }

        // 变更带 CAS：闭包内重读当前 voters 作期望快照，防止外部校验读到
        // 的集合与提交之间被并发变更（与 run_add 第二步同一模式）
        let remain: Vec<u64> = voters
            .iter()
            .copied()
            .filter(|v| *v != args.node_id)
            .collect();
        ctx.cluster
            .with_admin_leader(shard_id, |mut client| {
                let node_id = args.node_id;
                let retain = args.retain;
                async move {
                    let state = client
                        .get_raft_state(GetRaftStateRequest { shard_id })
                        .await?
                        .into_inner();
                    let current: BTreeSet<u64> = state.voter_ids.iter().copied().collect();
                    let remain: Vec<u64> =
                        current.iter().copied().filter(|v| *v != node_id).collect();
                    let req = ChangeMembershipRequest {
                        shard_id,
                        voter_ids: remain,
                        expected_voters: state.voter_ids,
                        retain,
                    };
                    client.change_membership(req).await.map(|r| r.into_inner())
                }
            })
            .await
            .map_err(|e| anyhow!("分片 {shard_id} 移除成员失败：{e:#}"))?;

        let remain_text = remain
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let detail = if args.retain {
            format!("已降级为 learner（voters=[{remain_text}]）")
        } else {
            format!("已从投票成员中移除（voters=[{remain_text}]）")
        };
        print_change(shard_id, args.node_id, ctx.format, &detail, true);
    }
    Ok(())
}

pub async fn run_list(ctx: &Ctx, _args: &MemberListArgs) -> Result<()> {
    let scope = ctx.shards().await?;
    let shard_ids = scope.all_ids();

    // 遍历 0..N × 端点聚合 GetRaftState；voter_ids 取首个可达端点的响应。
    // NotFound = 该端点无此分片（未初始化），合法跳过；其它错误收集。
    // 全部端点均不可达时整体报错——把网络故障伪装成"未初始化"会误导运维
    // （仿 status 命令的兜底护栏）。
    let mut per_shard: Vec<(u64, Vec<GetRaftStateResponse>)> = Vec::new();
    let mut lookup_errors: Vec<String> = Vec::new();
    let mut any_reachable = false;
    for shard_id in shard_ids {
        let mut nodes = Vec::new();
        for ep in ctx.cluster.endpoints() {
            match ctx.cluster.get_raft_state_via(ep, shard_id).await {
                Ok(state) => {
                    any_reachable = true;
                    nodes.push(state);
                }
                Err(status) if status.code() == tonic::Code::NotFound => {
                    // 该端点未注册此分片：未初始化，跳过
                }
                Err(status) => lookup_errors.push(format!("{ep}: {}", status.message())),
            }
        }
        per_shard.push((shard_id, nodes));
    }

    if !any_reachable {
        return Err(anyhow!(
            "所有端点均不可达，无法查询成员状态（{}）",
            if lookup_errors.is_empty() {
                "无错误".into()
            } else {
                lookup_errors.join("；")
            }
        ));
    }

    match ctx.format {
        Format::Simple => {
            for (shard_id, nodes) in &per_shard {
                if nodes.is_empty() {
                    println!("shard {shard_id}: 未初始化");
                    continue;
                }
                let leader = nodes
                    .iter()
                    .find(|n| n.is_leader)
                    .map(|n| n.current_leader.to_string())
                    .unwrap_or_else(|| "-".into());
                let voters = nodes[0]
                    .voter_ids
                    .iter()
                    .map(|v| v.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let desc = nodes
                    .iter()
                    .map(|n| format!("{}({})", n.node_id, n.server_state))
                    .collect::<Vec<_>>()
                    .join(" ");
                println!("shard {shard_id}: leader={leader} voters=[{voters}] nodes: {desc}");
            }
        }
        Format::Table => {
            let mut rows: Vec<Vec<String>> = Vec::new();
            for (shard_id, nodes) in &per_shard {
                if nodes.is_empty() {
                    rows.push(vec![
                        shard_id.to_string(),
                        "-".into(),
                        "未初始化".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                    ]);
                    continue;
                }
                for n in nodes {
                    rows.push(vec![
                        shard_id.to_string(),
                        n.node_id.to_string(),
                        n.server_state.clone(),
                        n.current_term.to_string(),
                        if n.has_leader {
                            n.current_leader.to_string()
                        } else {
                            "-".into()
                        },
                        if n.has_last_applied {
                            n.last_applied.to_string()
                        } else {
                            "-".into()
                        },
                        if n.voter_ids.contains(&n.node_id) {
                            "yes".into()
                        } else {
                            "no".into()
                        },
                    ]);
                }
            }
            println!(
                "{}",
                output::render_table(
                    &[
                        "SHARD",
                        "NODE",
                        "STATE",
                        "TERM",
                        "LEADER",
                        "LAST_APPLIED",
                        "VOTER"
                    ],
                    &rows
                )
                .trim_end()
            );
        }
        Format::Json => {
            let groups: Vec<serde_json::Value> = per_shard
                .iter()
                .map(|(shard_id, nodes)| {
                    serde_json::json!({
                        "shard_id": shard_id,
                        "nodes": nodes.iter().map(|n| serde_json::json!({
                            "node_id": n.node_id,
                            "server_state": n.server_state,
                            "is_leader": n.is_leader,
                            "has_leader": n.has_leader,
                            "current_leader": if n.has_leader { n.current_leader } else { 0 },
                            "current_term": n.current_term,
                            "last_applied": if n.has_last_applied { n.last_applied } else { 0 },
                            "voter_ids": n.voter_ids,
                        })).collect::<Vec<_>>(),
                    })
                })
                .collect();
            println!("{}", serde_json::json!({ "shards": groups }));
        }
    }
    Ok(())
}
