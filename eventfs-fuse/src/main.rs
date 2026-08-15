#[cfg(target_os = "linux")]
use std::path::PathBuf;
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[cfg(target_os = "linux")]
use anyhow::{Context, Result};
#[cfg(target_os = "linux")]
use clap::{Args, Parser, Subcommand};
#[cfg(target_os = "linux")]
use es_client::TlsClientConfig;
#[cfg(target_os = "linux")]
use eventfs_fuse::backend::{EventFsBackend, GrpcBackend};
#[cfg(target_os = "linux")]
use eventfs_fuse::config::MountConfig;
#[cfg(target_os = "linux")]
use eventfs_fuse::fuse::{EventFs, MountIdentity};

#[cfg(target_os = "linux")]
#[derive(Debug, Parser)]
#[command(name = "eventfs-fuse", version, about = "EventFS Linux FUSE3 daemon")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Subcommand)]
enum Command {
    /// 挂载 EventFS，缺省前台运行。
    Mount(MountArgs),
}

#[cfg(target_os = "linux")]
#[derive(Debug, Args)]
struct MountArgs {
    /// TOML 配置文件。
    #[arg(long)]
    config: PathBuf,
    /// 已存在的空挂载目录。
    mountpoint: PathBuf,
    /// 允许挂载用户之外的本机用户访问。
    #[arg(long)]
    allow_other: bool,
}

#[cfg(target_os = "linux")]
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    match Cli::parse().command {
        Command::Mount(args) => mount(args).await,
    }
}

#[cfg(target_os = "linux")]
async fn mount(args: MountArgs) -> Result<()> {
    let config = MountConfig::load(&args.config)?;
    anyhow::ensure!(args.mountpoint.is_dir(), "挂载点必须是已存在目录");
    let tls = tls_config(&config)?;
    let backend = Arc::new(GrpcBackend::connect(config.endpoints.clone(), tls).await?);
    let mut capabilities = backend.capabilities().await?;
    capabilities.max_event_bytes = capabilities.max_event_bytes.min(config.max_event_bytes);
    capabilities.max_state_bytes = capabilities.max_state_bytes.min(config.max_state_bytes);
    let filesystem = EventFs::new(
        backend,
        tokio::runtime::Handle::current(),
        capabilities,
        MountIdentity {
            // SAFETY: libc 进程身份查询没有前置条件。
            uid: unsafe { libc::geteuid() },
            // SAFETY: libc 进程身份查询没有前置条件。
            gid: unsafe { libc::getegid() },
        },
    );
    let fuse_config = fuse_config(args.allow_other);
    tracing::info!(mountpoint = %args.mountpoint.display(), "挂载 eventfs-fuse");
    fuser::mount(filesystem, &args.mountpoint, &fuse_config).context("FUSE 挂载失败")
}

#[cfg(target_os = "linux")]
fn tls_config(config: &MountConfig) -> Result<Option<TlsClientConfig>> {
    let tls = if let Some(path) = &config.ca_file {
        Some(TlsClientConfig::Ca(std::fs::read(path).with_context(
            || format!("读取 CA {} 失败", path.display()),
        )?))
    } else if config.insecure_skip_tls_verify {
        Some(TlsClientConfig::SkipVerify)
    } else {
        None
    };
    Ok(tls)
}

#[cfg(target_os = "linux")]
fn fuse_config(allow_other: bool) -> fuser::Config {
    let mut fuse_config = fuser::Config::default();
    fuse_config.mount_options.extend([
        fuser::MountOption::FSName("eventfs".into()),
        fuser::MountOption::Subtype("eventfs".into()),
        fuser::MountOption::DefaultPermissions,
        fuser::MountOption::RW,
        fuser::MountOption::NoDev,
        fuser::MountOption::NoSuid,
    ]);
    if allow_other {
        fuse_config.acl = fuser::SessionACL::All;
    }
    fuse_config
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("eventfs-fuse 仅支持 Linux FUSE3");
    std::process::exit(1);
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    fn config() -> MountConfig {
        MountConfig {
            endpoints: vec!["http://127.0.0.1:50051".into()],
            ca_file: None,
            insecure_skip_tls_verify: false,
            max_event_bytes: 1024,
            max_state_bytes: 1024,
        }
    }

    #[test]
    fn tls_modes_and_allow_other_map_to_runtime_configuration() {
        assert!(tls_config(&config()).unwrap().is_none());

        let mut value = config();
        value.insecure_skip_tls_verify = true;
        assert!(matches!(
            tls_config(&value).unwrap(),
            Some(TlsClientConfig::SkipVerify)
        ));

        let directory = tempfile::tempdir().unwrap();
        let ca_file = directory.path().join("ca.pem");
        std::fs::write(&ca_file, b"test-ca").unwrap();
        value.insecure_skip_tls_verify = false;
        value.ca_file = Some(ca_file);
        match tls_config(&value).unwrap().unwrap() {
            TlsClientConfig::Ca(bytes) => assert_eq!(bytes, b"test-ca"),
            other => panic!("预期 CA 配置，实际为 {other:?}"),
        }

        let normal = fuse_config(false);
        let shared = fuse_config(true);
        assert_eq!(shared.mount_options, normal.mount_options);
        assert_eq!(normal.acl, fuser::SessionACL::Owner);
        assert_eq!(shared.acl, fuser::SessionACL::All);
    }
}
