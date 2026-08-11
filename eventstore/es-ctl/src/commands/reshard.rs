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
    // 库要求 src/dst 必须是不同 tree
    let src = std::fs::canonicalize(&args.src_dir).context("解析源目录")?;
    let dst = std::fs::canonicalize(&args.dst_dir).context("解析目标目录")?;
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

    // 3. 打开 src/dst tree（dst 不存在时由 TreeBuilder 创建）
    let src_tree = open_tree(&args.src_dir)?;
    let dst_tree = open_tree(&args.dst_dir)?;

    // 4. 执行重分布
    let started = std::time::Instant::now();
    let report = es_storage::reshard::reshard(
        src_tree.clone(),
        args.src_shards,
        dst_tree.clone(),
        args.dst_shards,
    )
    .await
    .context("重分布失败")?;
    let elapsed = started.elapsed();

    // 5. 关闭（失败也要关；错误合并上报）
    let mut close_errs = Vec::new();
    if let Err(e) = close_tree(&args.src_dir, src_tree).await {
        close_errs.push(e);
    }
    if let Err(e) = close_tree(&args.dst_dir, dst_tree).await {
        close_errs.push(e);
    }
    if let Some(e) = close_errs.pop() {
        for extra in close_errs {
            eprintln!("警告：{extra:#}");
        }
        return Err(e);
    }

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
    fn 校验_源目录不存在() {
        let a = args("/nonexistent-xyz", 2, "/tmp/esctl-dst-xyz", 4, true);
        let err = validate(&a);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("不存在"));
    }

    #[test]
    fn 校验_分片数为零() {
        let a = args(".", 0, "/tmp/esctl-dst-xyz", 4, true);
        assert!(validate(&a).is_err());
        let a = args(".", 2, "/tmp/esctl-dst-xyz", 0, true);
        assert!(validate(&a).is_err());
    }

    #[test]
    fn 校验_源目标相同() {
        let a = args(".", 2, ".", 4, true);
        assert!(validate(&a).is_err());
    }

    #[test]
    fn 目标目录非空检测() {
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
