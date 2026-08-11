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
    if let Some(node_id) = args.node_id {
        config.node.id = node_id;
    }
    if let Some(listen) = args.listen {
        config.node.listen_addr = listen;
    }

    tracing::info!("Starting EventStore server (node_id={})", config.node.id);
    tracing::info!("Listen address: {}", config.node.listen_addr);
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
