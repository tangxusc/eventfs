//! `esctl reshard`：离线重分布（变更分片数）。
//!
//! 直接操作本地数据目录，不走网络。前置条件：集群完全停机、已备份数据。
//! 核心逻辑复用 `es_storage::reshard::reshard`（K 路归并 + position 重分配）。

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};

use crate::cli::{Format, ReshardArgs};
use crate::output;

/// fail-fast 参数校验（纯函数，可单测）
fn validate(args: &ReshardArgs) -> Result<()> {
    if !args.src_dir.exists() {
        bail!("源数据目录不存在：{}", args.src_dir.display());
    }
    if args.src_shards == 0 || args.dst_shards == 0 {
        bail!("源/目标分片数必须 ≥ 1");
    }
    // 库要求 src/dst 必须是不同 tree。
    let src = std::fs::canonicalize(&args.src_dir).context("解析源目录")?;
    // dst 可能尚不存在（TreeBuilder 会创建，docs/reshard.md 示例正是新路径）：
    // canonicalize 对不存在路径报 NotFound，不能让它破坏流程。
    // src 存在而 dst 不存在时两者必然不同；父目录可解析时再做词法比较兜底。
    let dst = match std::fs::canonicalize(&args.dst_dir) {
        Ok(p) => p,
        Err(_) if !args.dst_dir.exists() => {
            let parent = args
                .dst_dir
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new("/"));
            match std::fs::canonicalize(parent) {
                Ok(p) => p.join(args.dst_dir.file_name().unwrap_or_default()),
                // 父目录也不存在：路径必然与 src 不同（src 存在）
                Err(_) => return Ok(()),
            }
        }
        Err(e) => return Err(e).context("解析目标目录"),
    };
    if src == dst {
        bail!("源目录与目标目录必须不同");
    }
    Ok(())
}

/// 目标目录非空检查：已存在且含任何条目则视为"已使用"，防覆盖
fn dst_dir_nonempty(args: &ReshardArgs) -> bool {
    match std::fs::read_dir(&args.dst_dir) {
        Ok(mut it) => it.next().is_some(),
        Err(_) => false, // 目录不存在（将被创建）或不可读
    }
}

/// 确认交互：--yes 直接通过；否则打印停机警告并要求 y/N。
/// 非交互 stdin（EOF）视为拒绝。
fn confirm(args: &ReshardArgs) -> Result<()> {
    if args.yes {
        return Ok(());
    }
    eprintln!("警告：此操作需要集群完全停机，且应已备份数据目录！");
    eprintln!(
        "将把 {}（{} 分片）重分布到 {}（{} 分片）。",
        args.src_dir.display(),
        args.src_shards,
        args.dst_dir.display(),
        args.dst_shards
    );
    eprint!("继续? [y/N] ");
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("读取确认输入")?;
    match line.trim().to_lowercase().as_str() {
        "y" | "yes" => Ok(()),
        _ => bail!("已取消（数据未改动）"),
    }
}

/// 打开数据目录的 surrealkv tree。
///
/// 集群未停时 LOCK 文件被服务端持有，此处 `build()` 报
/// "already locked by another process"——自然成为停机约束的安全网。
fn open_tree(dir: &Path) -> Result<Arc<surrealkv::Tree>> {
    Ok(Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(dir.to_path_buf())
            .build()
            .map_err(|e| {
                anyhow!(
                    "打开数据目录 {} 失败（集群可能仍在运行，LOCK 被占用）: {e}",
                    dir.display()
                )
            })?,
    ))
}

/// 关闭 tree：先 flush_wal 落盘再 close（与 EsStorage::close 语义一致）
async fn close_tree(dir: &Path, tree: Arc<surrealkv::Tree>) -> Result<()> {
    tree.flush_wal(true)
        .map_err(|e| anyhow!("flush {} 失败: {e}", dir.display()))?;
    tree.close()
        .await
        .map_err(|e| anyhow!("close {} 失败: {e}", dir.display()))
}

pub async fn run(format: Format, args: &ReshardArgs) -> Result<()> {
    // 1. 参数校验（fail-fast）
    validate(args)?;
    // 2. 停机/备份确认；目标目录非空且未 --yes 时同样拒绝
    if dst_dir_nonempty(args) && !args.yes {
        bail!(
            "目标目录 {} 已存在且非空；如确认覆盖请加 --yes",
            args.dst_dir.display()
        );
    }
    confirm(args)?;

    // 3. 打开 src tree。从打开第一个 tree 起，任何后续失败都必须把已打开的
    //    tree 全部 flush+close（否则 LOCK 不释放、部分写入不落盘）。
    let src_tree = open_tree(&args.src_dir)?;

    // 校验 --src-shards 与目录实际布局一致：少报分片数时，哈希落在枚举范围
    // （0..src_shards）之外的分片数据会被静默跳过，且 src/dst 计数来自同一
    // 枚举子集（自洽），完整性校验拦不住——必须在执行前拒绝。
    let actual_shards = es_storage::reshard::infer_shard_count(&src_tree)
        .context("推断源目录分片数")?;
    if actual_shards != args.src_shards {
        close_tree(&args.src_dir, src_tree).await.ok();
        bail!(
            "--src-shards {} 与数据目录实际分片数 {} 不一致（按数据中出现的最大分片 ID 推断；\
             部分分片无数据的稀疏布局会低估，请以集群配置为准）。\
             分片数不匹配会漏读或读错分片，请更正后重试",
            args.src_shards, actual_shards
        );
    }

    // 4. 打开 dst tree（不存在时由 TreeBuilder 创建）
    let dst_tree = match open_tree(&args.dst_dir) {
        Ok(t) => t,
        Err(e) => {
            close_tree(&args.src_dir, src_tree).await.ok();
            return Err(e);
        }
    };

    // 5. 执行重分布（无论成败都收尾关闭）
    let started = std::time::Instant::now();
    let result = es_storage::reshard::reshard(
        src_tree.clone(),
        args.src_shards,
        dst_tree.clone(),
        args.dst_shards,
    )
    .await
    .context("重分布失败");

    // 6. 收尾：失败也要关（先 flush_wal 落盘再 close，与 EsStorage::close 语义一致），
    //    错误合并上报——残留未关闭的 tree 会占用 LOCK 且留下未落盘脏数据。
    let mut close_errs = Vec::new();
    if let Err(e) = close_tree(&args.src_dir, src_tree).await {
        close_errs.push(e);
    }
    if let Err(e) = close_tree(&args.dst_dir, dst_tree).await {
        close_errs.push(e);
    }
    let report = result?;
    if let Some(e) = close_errs.pop() {
        for extra in close_errs {
            eprintln!("警告：{extra:#}");
        }
        return Err(e);
    }
    let elapsed = started.elapsed();

    // 6. 输出
    let elapsed_ms = elapsed.as_millis() as u64;
    match format {
        Format::Simple => {
            println!(
                "重分布完成：{} 分片 → {} 分片",
                args.src_shards, args.dst_shards
            );
            println!(
                "  源布局：   {} 流，{} 事件",
                report.src_streams, report.src_events
            );
            println!(
                "  目标布局： {} 流，{} 事件",
                report.dst_streams, report.dst_events
            );
            println!("  耗时：     {:.1}s", elapsed.as_secs_f64());
            println!(
                "警告：请修改配置 num_shards={} 后，用新数据目录重启集群；确认后删除旧目录。",
                args.dst_shards
            );
        }
        Format::Table => {
            let rows = vec![
                vec!["src_dir".into(), args.src_dir.display().to_string()],
                vec!["src_shards".into(), args.src_shards.to_string()],
                vec!["dst_dir".into(), args.dst_dir.display().to_string()],
                vec!["dst_shards".into(), args.dst_shards.to_string()],
                vec!["src_streams".into(), report.src_streams.to_string()],
                vec!["src_events".into(), report.src_events.to_string()],
                vec!["dst_streams".into(), report.dst_streams.to_string()],
                vec!["dst_events".into(), report.dst_events.to_string()],
                vec!["elapsed_ms".into(), elapsed_ms.to_string()],
            ];
            println!(
                "{}",
                output::render_table(&["FIELD", "VALUE"], &rows).trim_end()
            );
        }
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "src_dir": args.src_dir.display().to_string(),
                "src_shards": args.src_shards,
                "dst_dir": args.dst_dir.display().to_string(),
                "dst_shards": args.dst_shards,
                "src_streams": report.src_streams,
                "src_events": report.src_events,
                "dst_streams": report.dst_streams,
                "dst_events": report.dst_events,
                "elapsed_ms": elapsed_ms,
            })
        ),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn args(src: &str, src_shards: u64, dst: &str, dst_shards: u64, yes: bool) -> ReshardArgs {
        ReshardArgs {
            src_dir: PathBuf::from(src),
            src_shards,
            dst_dir: PathBuf::from(dst),
            dst_shards,
            yes,
        }
    }

    #[test]
    fn validate_src_dir_missing() {
        let a = args("/nonexistent-xyz", 2, "/tmp/esctl-dst-xyz", 4, true);
        let err = validate(&a);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("不存在"));
    }

    #[test]
    fn validate_zero_shards_rejected() {
        let a = args(".", 0, "/tmp/esctl-dst-xyz", 4, true);
        assert!(validate(&a).is_err());
        let a = args(".", 2, "/tmp/esctl-dst-xyz", 0, true);
        assert!(validate(&a).is_err());
    }

    #[test]
    fn validate_src_dst_same_rejected() {
        let a = args(".", 2, ".", 4, true);
        assert!(validate(&a).is_err());
    }

    #[test]
    fn dst_dir_nonempty_detected() {
        let tmp = tempfile::tempdir().expect("临时目录");
        assert!(!dst_dir_nonempty(&args(
            tmp.path().to_str().unwrap(),
            2,
            "/nonexistent",
            4,
            true
        )));
        std::fs::write(tmp.path().join("LOCK"), b"x").expect("写文件");
        assert!(dst_dir_nonempty(&args(
            tmp.path().to_str().unwrap(),
            2,
            tmp.path().to_str().unwrap(),
            4,
            true
        )));
    }
}
