//! esctl 入口：解析参数、装配上下文、分发命令。
//!
//! 退出码约定（仿 etcdctl）：
//! - 0：成功
//! - 1：运行时失败（连接失败、无 leader、乐观并发冲突等）
//! - 2：参数错误（clap 默认）

use std::process::ExitCode;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;

use es_proto::tls::TlsClientConfig;

use crate::cli::{Cli, Command};
use crate::commands::Ctx;

mod cli;
mod client;
mod commands;
mod output;
mod shards;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let tls = load_tls(&cli.global)?;
    let cluster = client::ClusterClient::new(
        &cli.global.endpoints,
        tls,
        Duration::from_secs(cli.global.dial_timeout),
        Duration::from_secs(cli.global.timeout),
    )?;
    let ctx = Ctx::new(cluster, cli.global);

    match cli.command {
        Command::Init(a) => commands::init::run(&ctx, &a).await,
        Command::Member(a) => commands::member::run(&ctx, &a).await,
        Command::Status(a) => commands::status::run(&ctx, &a).await,
        Command::Snapshot(a) => match a.action {
            crate::cli::SnapshotAction::List(args) => {
                commands::snapshot::list(ctx.format, &args).await
            }
            crate::cli::SnapshotAction::Restore(args) => {
                commands::snapshot::run_restore(ctx.format, &args).await
            }
        },
        Command::Aggregate(a) => commands::aggregate::run(&ctx, &a.action).await,
    }
}

/// 全局 TLS 策略：--cacert → 严格校验；否则默认跳过校验（自签友好，与 es-client 语义一致）。
/// 仅 https 端点生效；http 端点由 apply_endpoint_tls 原样放行。
fn load_tls(global: &cli::GlobalArgs) -> Result<Option<TlsClientConfig>> {
    if let Some(path) = &global.cacert {
        let pem = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("读取 CA 文件 {} 失败: {e}", path.display()))?;
        Ok(Some(TlsClientConfig::Ca(pem)))
    } else {
        Ok(Some(TlsClientConfig::SkipVerify))
    }
}
