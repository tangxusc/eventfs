//! EventStore 服务端入口。

use anyhow::Result;
use clap::Parser;
use es_server::{Config, Server};
use std::sync::Arc;

/// EventStore 服务器命令行参数
#[derive(Parser, Debug)]
#[command(name = "eventstored")]
#[command(about = "EventStore 分布式事件存储服务器", long_about = None)]
struct Args {
    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    /// 节点 ID（覆盖配置文件）
    #[arg(long)]
    node_id: Option<u64>,

    /// 监听地址（覆盖配置文件）
    #[arg(long)]
    listen: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // 加载配置。解析失败必须 fail-fast：静默回退默认配置会让用户以为
    // 配置已生效（尤其是旧格式的 [shards] num_shards 配置），实际却在
    // 用最小默认布局跑——数据目录与放置表都可能不对。
    let mut config = match load_config(&args.config) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(
                "配置加载失败：{e:#}\n\
                 若使用了旧格式（[shards] num_shards），请迁移为 [placement] 放置表\n\
                 （config.example.toml 有示例）"
            );
            std::process::exit(1);
        }
    };

    // 命令行参数覆盖
    config = apply_overrides(config, args.node_id, args.listen);

    tracing::info!("Starting EventStore server (node_id={})", config.node.id);
    tracing::info!("Listen address: {}", config.node.listen_addr);
    tracing::info!(
        "TLS: {}",
        if config.tls.is_some() {
            "https"
        } else {
            "disabled"
        }
    );
    tracing::info!("Data directory: {:?}", config.storage.data_dir);
    tracing::info!(
        "Local shards: {:?} (cluster total {})",
        config.local_shards(),
        config.shard_count()
    );

    // 创建服务器
    let server = Arc::new(Server::new(config)?);

    // 初始化
    server.init().await?;

    // 配置与路由表热更新 watcher（动态 shard 创建 / routes.json 热生效）。
    // notify 只能 watch 已存在的文件：先确保路由表文件存在（缺失时落盘空表）
    if let Err(e) = server.route_table().ensure_file().await {
        tracing::error!("路由表文件初始化失败（动态扩容不可用）：{e}");
    }
    let watcher = match es_server::watcher::spawn(
        std::path::PathBuf::from(&args.config),
        es_server::route_table::routes_path(&server.config().storage.data_dir),
        server.route_table().clone(),
        server.ownership().clone(),
        server.shard_manager().clone(),
        server.config().node.id, // --node-id 覆盖后的实际节点
    ) {
        Ok(w) => Some(w),
        Err(e) => {
            tracing::error!("watcher 启动失败（动态扩容不可用，其余功能正常）：{e}");
            None
        }
    };

    // 启动服务（监听阻塞）
    let serve_server = server.clone();
    let serve = tokio::spawn(async move { serve_server.serve().await });

    // 优雅关闭：Ctrl-C / SIGTERM → 停 watcher → 逐 shard 停 Raft 并关闭存储
    // （flush WAL + 释放 surrealkv LOCK，否则重启报 "already locked"）
    tokio::select! {
        res = serve => return res?,
        _ = shutdown_signal() => {
            tracing::info!("收到退出信号，优雅关闭...");
        }
    }
    if let Some(w) = watcher {
        w.stop().await;
    }
    server.shutdown().await;

    Ok(())
}

/// 等待 Ctrl-C 或 SIGTERM（unix）退出信号。
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let sigterm = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("注册 SIGTERM 处理器")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let sigterm = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = sigterm => {},
    }
}

fn load_config(path: &str) -> Result<Config> {
    let content = std::fs::read_to_string(path)?;

    // 根据扩展名判断格式
    if path.ends_with(".json") {
        let config: Config = serde_json::from_str(&content)?;
        Ok(config)
    } else {
        // 默认 toml
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }
}

/// 命令行参数覆盖配置（抽出来便于单测）。
fn apply_overrides(mut config: Config, node_id: Option<u64>, listen: Option<String>) -> Config {
    if let Some(node_id) = node_id {
        config.node.id = node_id;
    }
    if let Some(listen) = listen {
        config.node.listen_addr = listen;
    }
    config
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// 完整 toml 配置（Config 全字段必填，缺一不可反序列化）
    fn full_toml() -> String {
        r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"
peers = []

[storage]
data_dir = "/tmp/es-data"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]
"#
        .to_string()
    }

    fn full_json() -> String {
        r#"{"node":{"id":2,"listen_addr":"127.0.0.1:50052","peers":[]},"storage":{"data_dir":"/tmp/es-data"},"placement":{"replication_factor":1,"nodes":[{"id":2,"primary":[0],"replica":[]}]},"tls":null}"#
            .to_string()
    }

    #[test]
    fn load_config_toml_branch() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, full_toml()).expect("写配置");
        let cfg = load_config(path.to_str().unwrap()).expect("解析 toml");
        assert_eq!(cfg.node.id, 1);
        assert_eq!(cfg.local_shards(), vec![0]);
    }

    #[test]
    fn load_config_json_branch() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("config.json");
        std::fs::write(&path, full_json()).expect("写配置");
        let cfg = load_config(path.to_str().unwrap()).expect("解析 json");
        assert_eq!(cfg.node.id, 2);
        assert_eq!(cfg.local_shards(), vec![0]);
    }

    #[test]
    fn load_config_old_shards_format_rejected() {
        // 旧格式（[shards] num_shards）解析层已不再报错：serde 对未知段
        // 默认忽略、placement 走 default（rf=2 空表）。但 fail-fast 语义
        // 保留——空放置表在 validate() 处被拒，启动必然失败并提示迁移
        // [placement]，不会静默用默认布局运行。
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"
peers = []

[storage]
data_dir = "/tmp/es-data"

[shards]
num_shards = 8
"#,
        )
        .expect("写配置");
        let cfg = load_config(path.to_str().unwrap()).expect("旧格式可解析（未知段被忽略）");
        let err = cfg.validate().expect_err("空放置表应被拒绝（fail-fast）");
        assert!(
            err.contains("nodes"),
            "错误应说明 placement nodes 为空: {err}"
        );
    }

    #[test]
    fn load_config_missing_file_errors() {
        assert!(load_config("/nonexistent/config.toml").is_err());
    }

    #[test]
    fn load_config_invalid_content_errors() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("bad.toml");
        std::fs::write(&path, "not [valid toml").expect("写配置");
        assert!(load_config(path.to_str().unwrap()).is_err());
    }

    #[test]
    fn args_clap_parse_defaults() {
        let args = Args::try_parse_from(["eventstored"]).expect("无参数应可用默认值");
        assert_eq!(args.config, "config.toml");
        assert_eq!(args.node_id, None);
        assert_eq!(args.listen, None);
    }

    #[test]
    fn args_clap_parse_overrides() {
        let args = Args::try_parse_from([
            "eventstored",
            "--config",
            "c.toml",
            "--node-id",
            "9",
            "--listen",
            "127.0.0.1:9999",
        ])
        .expect("解析覆盖参数");
        assert_eq!(args.config, "c.toml");
        assert_eq!(args.node_id, Some(9));
        assert_eq!(args.listen, Some("127.0.0.1:9999".to_string()));
    }

    #[test]
    fn apply_overrides_some_and_none() {
        let base = Config::default();
        // 都提供 → 覆盖
        let over = apply_overrides(base.clone(), Some(9), Some("127.0.0.1:8888".into()));
        assert_eq!(over.node.id, 9);
        assert_eq!(over.node.listen_addr, "127.0.0.1:8888");
        // 都不提供 → 原样保留
        let kept = apply_overrides(base.clone(), None, None);
        assert_eq!(kept.node.id, base.node.id);
        assert_eq!(kept.node.listen_addr, base.node.listen_addr);
        // 只覆盖其一 → 另一个不动
        let partial = apply_overrides(base.clone(), Some(7), None);
        assert_eq!(partial.node.id, 7);
        assert_eq!(partial.node.listen_addr, base.node.listen_addr);
    }
}
