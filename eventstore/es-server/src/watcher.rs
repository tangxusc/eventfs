//! 配置热更新与运行期 Shard 创建。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use es_raft::ShardManager;
use notify::{RecursiveMode, Watcher};
use tokio::sync::watch;

use crate::config::Config;
use crate::factory;
use crate::rpc_support::RuntimeTopology;

const DEBOUNCE: Duration = Duration::from_millis(200);
const RECONCILE_INTERVAL: Duration = Duration::from_millis(500);

fn file_fingerprint(path: &std::path::Path) -> Option<u64> {
    let bytes = std::fs::read(path).ok()?;
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    Some(hasher.finish())
}

/// 配置 watcher 生命周期句柄。
pub struct WatcherHandle {
    _watcher: notify::RecommendedWatcher,
    task: tokio::task::JoinHandle<()>,
    shutdown: watch::Sender<bool>,
}

impl WatcherHandle {
    /// 停止后台调和任务并等待其退出。
    pub async fn stop(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

/// 监听配置文件并动态创建新增的本地 Shard。
///
/// `config_path` 必须位于已存在目录；`shard_manager` 接收新建 Shard，
/// `self_node_id` 保留命令行覆盖后的实际节点身份。notify 初始化失败时返回错误；
/// 后续配置读取、校验或 Shard 创建失败只记录错误并保留当前运行状态。
pub(crate) fn spawn(
    config_path: PathBuf,
    shard_manager: Arc<ShardManager>,
    topology: RuntimeTopology,
    self_node_id: u64,
) -> Result<WatcherHandle, notify::Error> {
    let config_name = config_path.file_name().unwrap_or_default().to_os_string();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
        if result.as_ref().is_ok_and(|event| {
            event
                .paths
                .iter()
                .any(|path| path.file_name() == Some(config_name.as_os_str()))
        }) {
            let _ = tx.blocking_send(result);
        }
    })?;
    watcher.watch(
        config_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new(".")),
        RecursiveMode::NonRecursive,
    )?;

    let mut fingerprint = file_fingerprint(&config_path);
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut reconcile = tokio::time::interval(RECONCILE_INTERVAL);
        reconcile.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !*shutdown_rx.borrow() {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                _ = reconcile.tick() => {
                    let current = file_fingerprint(&config_path);
                    if current != fingerprint {
                        handle_config_change(
                            &config_path,
                            &shard_manager,
                            &topology,
                            self_node_id,
                        ).await;
                        fingerprint = file_fingerprint(&config_path);
                    }
                }
                event = rx.recv() => {
                    let Some(event) = event else { break };
                    if event.is_err() { continue; }
                    tokio::time::sleep(DEBOUNCE).await;
                    if *shutdown_rx.borrow() { break; }
                    handle_config_change(
                        &config_path,
                        &shard_manager,
                        &topology,
                        self_node_id,
                    ).await;
                    fingerprint = file_fingerprint(&config_path);
                }
            }
        }
        tracing::info!("配置 watcher 已停止");
    });
    Ok(WatcherHandle {
        _watcher: watcher,
        task,
        shutdown,
    })
}

async fn handle_config_change(
    config_path: &PathBuf,
    shard_manager: &Arc<ShardManager>,
    topology: &RuntimeTopology,
    self_node_id: u64,
) {
    let content = match std::fs::read_to_string(config_path) {
        Ok(content) => content,
        Err(error) => {
            tracing::error!(?config_path, "配置热更新读取失败：{error}");
            return;
        }
    };
    let mut config: Config = match toml::from_str(&content) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!("配置热更新解析失败，保留旧配置：{error}");
            return;
        }
    };
    config.node.id = self_node_id;
    if let Err(error) = config.validate() {
        tracing::error!("配置热更新校验失败，保留旧配置：{error}");
        return;
    }

    let desired: std::collections::BTreeSet<_> = config.local_shards().into_iter().collect();
    let current: std::collections::BTreeSet<_> =
        shard_manager.shard_ids().await.into_iter().collect();
    let removed: Vec<_> = current.difference(&desired).copied().collect();
    if !removed.is_empty() {
        tracing::warn!(
            ?removed,
            "配置已移除本地 Shard；运行中实例和数据目录保留至重启"
        );
    }
    let mut created_all = true;
    for shard_id in desired.difference(&current).copied() {
        if let Err(error) = create_shard_blocking(&config, shard_manager, shard_id).await {
            tracing::error!(shard_id, "动态创建 Shard 失败：{error}");
            created_all = false;
        }
    }
    if created_all && let Err(error) = topology.reload(&config).await {
        tracing::error!("运行期拓扑更新失败，保留旧拓扑：{error}");
    }
}

async fn create_shard_blocking(
    config: &Config,
    shard_manager: &Arc<ShardManager>,
    shard_id: u64,
) -> Result<(), anyhow::Error> {
    if shard_manager.shard_ids().await.contains(&shard_id) {
        return Ok(());
    }
    let shard = factory::create_shard(config, shard_id).await?;
    shard_manager.register_shard(shard).await?;
    crate::bootstrap::bootstrap_new_shard(config, shard_manager.clone(), shard_id).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_added_and_removed() {
        let desired: std::collections::BTreeSet<u64> = [0, 1, 2, 4].into_iter().collect();
        let current: std::collections::BTreeSet<u64> = [0, 1, 3].into_iter().collect();
        assert_eq!(
            desired.difference(&current).copied().collect::<Vec<_>>(),
            [2, 4]
        );
        assert_eq!(
            current.difference(&desired).copied().collect::<Vec<_>>(),
            [3]
        );
    }

    #[test]
    fn invalid_hot_config_is_recoverable() {
        assert!(toml::from_str::<Config>("not [valid").is_err());
    }
}
