//! EventStore 服务端入口。

use anyhow::Result;
use clap::Parser;
use es_server::{Config, Server};

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

    // 加载配置
    let mut config = load_config(&args.config).unwrap_or_else(|e| {
        tracing::warn!("Failed to load config from {}: {}", args.config, e);
        tracing::info!("Using default configuration");
        Config::default()
    });

    // 命令行参数覆盖
    config = apply_overrides(config, args.node_id, args.listen);

    tracing::info!("Starting EventStore server (node_id={})", config.node.id);
    tracing::info!("Listen address: {}", config.node.listen_addr);
    tracing::info!(
        "TLS: {}",
        if config.tls.is_some() { "https" } else { "disabled" }
    );
    tracing::info!("Data directory: {:?}", config.storage.data_dir);
    tracing::info!("Number of shards: {}", config.shards.num_shards);

    // 创建服务器
    let server = Server::new(config)?;

    // 初始化
    server.init().await?;

    // 启动服务
    server.serve().await?;

    Ok(())
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
    fn full_toml(num_shards: u64) -> String {
        format!(
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"
peers = []

[storage]
data_dir = "/tmp/es-data"

[shards]
num_shards = {num_shards}
"#
        )
    }

    fn full_json(num_shards: u64) -> String {
        format!(
            r#"{{"node":{{"id":2,"listen_addr":"127.0.0.1:50052","peers":[]}},"storage":{{"data_dir":"/tmp/es-data"}},"shards":{{"num_shards":{num_shards}}},"tls":null}}"#
        )
    }

    #[test]
    fn load_config_toml_branch() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("config.toml");
        std::fs::write(&path, full_toml(4)).expect("写配置");
        let cfg = load_config(path.to_str().unwrap()).expect("解析 toml");
        assert_eq!(cfg.node.id, 1);
        assert_eq!(cfg.shards.num_shards, 4);
    }

    #[test]
    fn load_config_json_branch() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = dir.path().join("config.json");
        std::fs::write(&path, full_json(8)).expect("写配置");
        let cfg = load_config(path.to_str().unwrap()).expect("解析 json");
        assert_eq!(cfg.node.id, 2);
        assert_eq!(cfg.shards.num_shards, 8);
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
