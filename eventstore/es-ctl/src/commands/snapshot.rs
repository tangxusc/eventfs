//! `esctl snapshot`：离线快照管理（list / restore）。
//!
//! 直接操作本地数据目录，不走网络（同 reshard 模式）。
//! - list：扫描 {data_dir}/snapshots 下的 *.esnap，解析文件头（不碰 payload）
//! - restore：把快照恢复到指定分片（需集群停机，LOCK 安全网兜底）

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};

use crate::cli::{Format, SnapshotRestoreArgs};
use crate::output;

/// 快照目录：显式 --snapshot-dir 优先，否则 {data_dir}/snapshots。
///
/// 服务端可配置 [snapshot].dir 自定义目录，此时必须显式传入，
/// 否则 CLI 与服务器的快照视图不一致。
fn snapshot_dir(data_dir: &Path, explicit: Option<&Path>) -> std::path::PathBuf {
    explicit
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| data_dir.join("snapshots"))
}

/// 打开数据目录的 surrealkv tree（LOCK 安全网，同 reshard）
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

/// `esctl snapshot list`
pub async fn list(format: Format, args: &crate::cli::SnapshotListArgs) -> Result<()> {
    let dir = snapshot_dir(&args.data_dir, args.snapshot_dir.as_deref());
    if !dir.is_dir() {
        bail!(
            "快照目录不存在：{}（数据目录可能未初始化，或未配置自定义快照目录）",
            dir.display()
        );
    }
    // 用 SnapshotStore 的枚举逻辑（损坏文件标记 valid=false，不中断）
    let store = es_storage::snapshot::SnapshotStore::new(
        es_storage::snapshot::SnapshotConfig {
            dir: dir.clone(),
            ..Default::default()
        },
        0,
    )?;
    let entries = store.list_entries().context("扫描快照目录")?;

    if entries.is_empty() {
        println!("（无快照）");
        return Ok(());
    }

    match format {
        Format::Simple => {
            for e in &entries {
                match (&e.header, &e.meta) {
                    (Some(_h), Some(m)) => {
                        let (term, index) = match m.last_log_id {
                            Some(l) => (l.leader_id.term, l.index),
                            None => (0, 0),
                        };
                        let algo = e
                            .header
                            .as_ref()
                            .map(|h| h.compression.display_name())
                            .unwrap_or("-");
                        println!(
                            "{}  shard={}  term={} index={}  {}  {}B→{}B  {}",
                            e.path.file_name().unwrap().to_string_lossy(),
                            e.header.as_ref().unwrap().shard_id,
                            term,
                            index,
                            algo,
                            e.header.as_ref().unwrap().payload_len,
                            e.size,
                            fmt_time(e.mtime),
                        );
                    }
                    _ => {
                        println!(
                            "{}  损坏（头部解析失败，文件不可用）",
                            e.path.file_name().unwrap().to_string_lossy()
                        );
                    }
                }
            }
        }
        Format::Table => {
            let rows: Vec<Vec<String>> = entries
                .iter()
                .map(|e| match (&e.header, &e.meta) {
                    (Some(h), Some(m)) => {
                        let (term, index) = match m.last_log_id {
                            Some(l) => (l.leader_id.term, l.index),
                            None => (0, 0),
                        };
                        vec![
                            e.path.file_name().unwrap().to_string_lossy().to_string(),
                            h.shard_id.to_string(),
                            term.to_string(),
                            index.to_string(),
                            m.snapshot_id.clone(),
                            h.compression.display_name().to_string(),
                            h.payload_len.to_string(),
                            e.size.to_string(),
                            fmt_time(e.mtime),
                            "ok".into(),
                        ]
                    }
                    _ => vec![
                        e.path.file_name().unwrap().to_string_lossy().to_string(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        "-".into(),
                        e.size.to_string(),
                        "-".into(),
                        "损坏".into(),
                    ],
                })
                .collect();
            println!(
                "{}",
                output::render_table(
                    &[
                        "FILE", "SHARD", "TERM", "INDEX", "SNAPSHOT_ID", "COMPRESS",
                        "PAYLOAD_B", "SIZE_B", "MTIME", "STATUS"
                    ],
                    &rows
                )
                .trim_end()
            );
        }
        Format::Json => {
            let snapshots: Vec<serde_json::Value> = entries
                .iter()
                .map(|e| match (&e.header, &e.meta) {
                    (Some(h), Some(m)) => {
                        let (term, index) = match m.last_log_id {
                            Some(l) => (l.leader_id.term, l.index),
                            None => (0, 0),
                        };
                        serde_json::json!({
                            "file": e.path.file_name().unwrap().to_string_lossy(),
                            "shard_id": h.shard_id,
                            "term": term,
                            "index": index,
                            "snapshot_id": m.snapshot_id,
                            "compression": h.compression.display_name(),
                            "payload_len": h.payload_len,
                            "size": e.size,
                            "mtime": e.mtime.duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs()).unwrap_or(0),
                            "status": "ok",
                        })
                    }
                    _ => serde_json::json!({
                        "file": e.path.file_name().unwrap().to_string_lossy(),
                        "size": e.size,
                        "status": "损坏",
                    }),
                })
                .collect();
            println!("{}", serde_json::json!({ "snapshots": snapshots }));
        }
    }
    Ok(())
}

/// 确认交互：--yes 直接通过；否则打印停机警告并要求 y/N（同 reshard）
fn confirm(args: &SnapshotRestoreArgs) -> Result<()> {
    if args.yes {
        return Ok(());
    }
    eprintln!("警告：此操作需要集群完全停机，且应已备份数据目录！");
    eprintln!(
        "将把快照 {} 恢复到数据目录 {}（该分片的现有数据与日志将被覆盖）。",
        args.snapshot_file.display(),
        args.data_dir.display()
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

/// fail-fast 参数校验（纯函数，可单测）。
/// 只读文件头（32 + meta_len 字节），不把整个快照载入内存。
fn validate(args: &SnapshotRestoreArgs) -> Result<()> {
    if !args.data_dir.exists() {
        bail!("数据目录不存在：{}", args.data_dir.display());
    }
    if !args.snapshot_file.exists() {
        bail!("快照文件不存在：{}", args.snapshot_file.display());
    }
    let f = std::fs::File::open(&args.snapshot_file).context("打开快照文件")?;
    es_storage::snapshot::read_header(&mut std::io::BufReader::new(f))
        .map_err(|e| anyhow!("快照文件无效: {e}"))?;
    Ok(())
}

/// `esctl snapshot restore`
pub async fn run_restore(format: Format, args: &SnapshotRestoreArgs) -> Result<()> {
    // 1. fail-fast 校验（含快照文件头解析）
    validate(args)?;
    // 头部已在 validate 校验过，这里重开读取分片号（不整文件读入内存）
    let f = std::fs::File::open(&args.snapshot_file).context("打开快照文件")?;
    let (header, _) = es_storage::snapshot::read_header(&mut std::io::BufReader::new(f))
        .map_err(|e| anyhow!("快照文件无效: {e}"))?;
    let shard_id = header.shard_id;
    // 2. 停机确认
    confirm(args)?;

    // 3. 打开 tree（LOCK 安全网：集群未停时 build() 报 "already locked"）
    let tree = open_tree(&args.data_dir)?;

    // 4. 执行恢复（无论成败都收尾关闭）
    let started = std::time::Instant::now();
    let result = es_storage::snapshot::restore(
        tree.clone(),
        shard_id,
        &args.snapshot_file,
        &snapshot_dir(&args.data_dir, args.snapshot_dir.as_deref()),
    )
    .await
    .context("恢复失败");

    // 5. 收尾：失败也要关（flush+close），错误合并上报
    let close_res = close_tree(&args.data_dir, tree).await;
    let report = result?;
    if let Err(e) = close_res {
        eprintln!("警告：关闭数据目录失败: {e:#}");
    }
    let elapsed = started.elapsed();

    match format {
        Format::Simple => {
            println!("恢复完成：分片 {} 回到快照点 term={} index={}（{} 条事件）",
                report.shard_id, report.term, report.index, report.events);
            println!("  快照文件： {}", report.snapshot_file.display());
            println!("  耗时：     {:.1}s", elapsed.as_secs_f64());
            println!("提示：选举状态（vote）已保留，重启后节点以快照点直接恢复；");
            println!("      多节点集群启动后由 leader 复制快照点之后的日志或新快照。");
        }
        Format::Table => {
            let rows = vec![
                vec!["shard_id".into(), report.shard_id.to_string()],
                vec!["term".into(), report.term.to_string()],
                vec!["index".into(), report.index.to_string()],
                vec!["events".into(), report.events.to_string()],
                vec!["snapshot_file".into(), report.snapshot_file.display().to_string()],
                vec!["elapsed_ms".into(), elapsed.as_millis().to_string()],
            ];
            println!(
                "{}",
                output::render_table(&["FIELD", "VALUE"], &rows).trim_end()
            );
        }
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "shard_id": report.shard_id,
                "term": report.term,
                "index": report.index,
                "events": report.events,
                "snapshot_file": report.snapshot_file.display().to_string(),
                "elapsed_ms": elapsed.as_millis(),
            })
        ),
    }
    Ok(())
}

/// mtime 渲染为 RFC3339；转换失败退回原始值
fn fmt_time(t: std::time::SystemTime) -> String {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => match chrono::DateTime::from_timestamp(d.as_secs() as i64, 0) {
            Some(dt) => dt.to_rfc3339(),
            None => "?".into(),
        },
        Err(_) => "?".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn args(data_dir: &str, snap: &str, yes: bool) -> SnapshotRestoreArgs {
        SnapshotRestoreArgs {
            data_dir: PathBuf::from(data_dir),
            snapshot_file: PathBuf::from(snap),
            snapshot_dir: None,
            yes,
        }
    }

    #[test]
    fn validate_missing_data_dir() {
        let a = args("/nonexistent-xyz", "/nonexistent.snap", true);
        let err = validate(&a);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("数据目录"));
    }

    #[test]
    fn validate_missing_snapshot_file() {
        let dir = tempfile::tempdir().expect("临时目录");
        let a = args(dir.path().to_str().unwrap(), "/nonexistent.snap", true);
        let err = validate(&a);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("快照文件不存在"));
    }

    #[test]
    fn validate_invalid_snapshot_rejected() {
        let dir = tempfile::tempdir().expect("临时目录");
        let bad = dir.path().join("bad.snap");
        std::fs::write(&bad, b"not a snapshot").expect("写坏文件");
        let a = args(dir.path().to_str().unwrap(), bad.to_str().unwrap(), true);
        let err = validate(&a);
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("快照文件无效"));
    }
}
