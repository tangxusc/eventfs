//! 配置文件与路由表热更新 watcher + 运行期动态 shard 创建。
//!
//! 节点只承载放置表分配的部分分片；扩容 = 更新所有节点配置（新增节点/
//! 新增 shards 行）→ 各节点 watch 到变更后运行期创建新增的 shards，无需重启。
//!
//! 两条 watch：
//! - config.toml：重载 + validate（失败仅告警，服务不受影响）→ diff 新增
//!   shards → 逐个创建（factory::create_shard → register_shard → bootstrap）
//! - routes.json：热更新路由表（运维手工修改文件同样生效）
//!
//! 配置中移除 shard：仅告警，数据目录保留；重新加入时幂等打开恢复。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tokio::sync::watch;

use es_raft::ShardManager;

use crate::config::Config;
use crate::factory;
use crate::route_table::RouteTableManager;

/// 事件合并窗口：配置文件常见 temp+rename 写入，会触发多事件；
/// 收到事件后等待窗口结束再重读文件（读到的即最终状态）。
const DEBOUNCE: Duration = Duration::from_millis(200);

/// watcher 生命周期句柄：持有 notify watcher（防 drop）与后台任务。
pub struct WatcherHandle {
    _watcher: notify::RecommendedWatcher,
    task: tokio::task::JoinHandle<()>,
    /// 置 true 停止后台循环
    shutdown: watch::Sender<bool>,
}

impl WatcherHandle {
    /// 停止 watcher 后台任务（shutdown 流程调用）。
    pub async fn stop(self) {
        let _ = self.shutdown.send(true);
        let _ = self.task.await;
    }
}

/// 启动配置与路由表 watcher。
///
/// watch 目标：**文件所在目录**而非文件本身——notify 在 macOS（FSEvents）
/// 按 inode 跟踪被 watch 的文件，`sed -i`/temp+rename 等原子替换会让
/// watcher 指向旧 inode 而永久丢失后续事件；watch 目录 + 文件名过滤
/// 则对 rename 替换天然可靠。
///
/// 注意：FSEvents 目录 watch 是**递归**的（shard 子目录的 surrealkv 写活动
/// 也产生事件）。回调里前置过滤——只放行目标文件（config/routes）的事件，
/// 否则事件风暴会溢出事件通道，把真正的配置变更事件挤掉。
pub fn spawn(
    config_path: PathBuf,
    routes_path: PathBuf,
    route_table: Arc<RouteTableManager>,
    shard_manager: Arc<ShardManager>,
    self_node_id: u64,
) -> Result<WatcherHandle, notify::Error> {
    // 事件是否命中目标文件：路径末尾文件名匹配（rename 替换后路径仍是新文件名）
    // 文件名提前提取为 owned 值——闭包要 move 进 notify 回调（'static）
    let config_name = config_path.file_name().unwrap_or_default().to_os_string();
    let routes_name = routes_path.file_name().unwrap_or_default().to_os_string();
    let matches = move |paths: &[std::path::PathBuf]| -> bool {
        paths.iter().any(|p| {
            let fname = p.file_name().unwrap_or_default();
            fname == config_name.as_os_str() || fname == routes_name.as_os_str()
        })
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel(1024);
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        // 前置过滤：只转发目标文件事件（防 shard 目录事件风暴挤掉配置变更）
        if res.as_ref().is_ok_and(|e| matches(&e.paths)) {
            // 事件发送失败（接收端已关）→ watcher 停摆，静默
            let _ = tx.blocking_send(res);
        }
    })?;
    // 目录去重后 watch（config 与 routes 可能同目录）
    let mut watched: Vec<std::path::PathBuf> = Vec::new();
    for p in [&config_path, &routes_path] {
        let dir = p.parent().unwrap_or_else(|| std::path::Path::new("."));
        if !watched.iter().any(|w| w == dir) {
            watched.push(dir.to_path_buf());
            watcher.watch(dir, RecursiveMode::NonRecursive)?;
        }
    }

    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

    let task = tokio::spawn(async move {
        // 事件到达 → debounce → 按路径分发
        while !*shutdown_rx.borrow() {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                maybe = rx.recv() => {
                    let Some(ev) = maybe else { break };
                    if ev.is_err() {
                        continue;
                    }
                    // debounce：等待窗口内的事件合并
                    tokio::time::sleep(DEBOUNCE).await;
                    if *shutdown_rx.borrow() {
                        break;
                    }
                    if ev.as_ref().is_ok_and(|e| {
                        e.paths.iter().any(|p| p.file_name() == config_path.file_name())
                    }) {
                        handle_config_change(&config_path, &route_table, &shard_manager, self_node_id)
                            .await;
                    } else if ev.as_ref().is_ok_and(|e| {
                        e.paths.iter().any(|p| p.file_name() == routes_path.file_name())
                    }) {
                        route_table.reload().await;
                    }
                }
            }
        }
        tracing::info!("watcher 已停止");
    });

    Ok(WatcherHandle {
        _watcher: watcher,
        task,
        shutdown: shutdown_tx,
    })
}

/// 配置变更处理：重载 → 校验 → 创建新增 shards → 更新分配范围。
async fn handle_config_change(
    config_path: &PathBuf,
    route_table: &Arc<RouteTableManager>,
    shard_manager: &Arc<ShardManager>,
    self_node_id: u64,
) {
    // 重载配置（fail-soft：损坏/非法保留旧配置，服务不受影响）
    let content = match std::fs::read_to_string(config_path) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("配置热更新：读取失败（{config_path:?}）：{e}");
            return;
        }
    };
    let mut cfg: Config = match toml::from_str(&content) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("配置热更新：解析失败，保留旧配置：{e}");
            return;
        }
    };
    // --node-id 命令行覆盖启动的节点：热更新必须按**实际节点**计算
    // local_shards/self_id，不能用文件里的 node.id（否则会把别的节点的
    // 分片创建/自举到本节点，形成幽灵 raft group）
    cfg.node.id = self_node_id;
    if let Err(e) = cfg.validate() {
        tracing::error!("配置热更新：校验失败，保留旧配置：{e}");
        return;
    }

    // diff：新增的本地 shards → 串行创建（全部就绪后才更新分配池，
    // 避免扩容窗口内新流被分配到尚未创建的 shard）
    let new_local: std::collections::BTreeSet<u64> = cfg.local_shards().into_iter().collect();
    let current: std::collections::BTreeSet<u64> =
        shard_manager.shard_ids().await.into_iter().collect();
    let added: Vec<u64> = new_local.difference(&current).copied().collect();
    let removed: Vec<u64> = current.difference(&new_local).copied().collect();

    if !removed.is_empty() {
        tracing::warn!(
            "配置热更新：以下 shards 不再由本节点承载（数据目录保留，可重新加入）：{removed:?}。\
             注意：路由表中指向这些 shards 的流在分片不存在后将不可写，\
             请用 `esctl route check` 检测并用 `esctl migrate` 迁移"
        );
    }

    let has_added = !added.is_empty();
    let mut created_ok = true;
    for shard_id in added {
        tracing::info!("配置热更新：新增 shard {shard_id}，动态创建...");
        match create_shard_blocking(&cfg, shard_manager, shard_id).await {
            Ok(true) => {} // 已注册（重复事件）视为成功
            Ok(false) => {}
            Err(e) => {
                created_ok = false;
                tracing::error!(shard_id, "动态创建分片失败：{e}");
            }
        }
    }

    // 分配范围随放置表更新（新 shards 加入分配池；创建失败的部分不加入——
    // 由 cfg 的 placement 决定整体，失败 shard 无法从池中单独剔除，
    // 因此仅在全部创建成功时才更新，避免把未就绪 shard 暴露给分配）
    if created_ok || !has_added {
        let shard_set: std::collections::BTreeSet<u64> = cfg
            .placement
            .nodes
            .iter()
            .flat_map(|n| n.primary.iter().chain(n.replica.iter()))
            .copied()
            .collect();
        route_table.set_shard_set(shard_set).await;
    }
}

/// 创建并注册单个 shard（含自举），返回是否已注册（幂等）。
async fn create_shard_blocking(
    cfg: &Config,
    shard_manager: &Arc<ShardManager>,
    shard_id: u64,
) -> Result<bool, anyhow::Error> {
    match factory::create_shard(cfg, shard_id).await {
        Ok(shard) => match shard_manager.register_shard(shard).await {
            Ok(()) => {
                crate::bootstrap::bootstrap_new_shard(cfg, shard_manager.clone(), shard_id).await;
                Ok(true)
            }
            // FSEvents 对 rename 替换可能延迟投递多波事件：第二次事件
            // 到达时 shard 可能已被（前一波）注册——视为幂等成功
            Err(e) if e.to_string().contains("already registered") => {
                tracing::info!(shard_id, "shard 已注册（重复事件），跳过");
                Ok(true)
            }
            Err(e) => Err(anyhow::anyhow!("动态注册分片失败：{e}")),
        },
        // 重复创建时 surrealkv 同目录二次打开报 already locked——
        // 同样视为幂等（首次创建已持有该目录）
        Err(e) if e.to_string().contains("already locked") => {
            tracing::info!(shard_id, "shard 已创建（重复事件），跳过");
            Ok(true)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 配置差异计算（纯逻辑，供 handle 内部用）——这里直接验证 diff 语义
    #[test]
    fn diff_added_and_removed() {
        let new_local: std::collections::BTreeSet<u64> = vec![0, 1, 2, 4].into_iter().collect();
        let current: std::collections::BTreeSet<u64> = vec![0, 1, 3].into_iter().collect();
        let added: Vec<u64> = new_local.difference(&current).copied().collect();
        let removed: Vec<u64> = current.difference(&new_local).copied().collect();
        assert_eq!(added, vec![2, 4]);
        assert_eq!(removed, vec![3]);
    }

    /// 配置热更新：解析+校验失败不影响当前状态（fail-soft 语义由
    /// handle_config_change 内部保证，此处验证纯解析路径）。
    #[test]
    fn hot_config_parse_failure_is_recoverable() {
        // 非法 toml → 解析失败；合法但校验失败 → validate 拒绝
        assert!(toml::from_str::<Config>("not [valid").is_err());
        let cfg: Config = toml::from_str(
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:50051"
peers = []

[storage]
data_dir = "./data"
"#,
        )
        .expect("缺 placement 段可解析（serde default 空表）");
        assert!(
            cfg.validate().is_err(),
            "空放置表校验失败（fail-soft 前置）"
        );
    }

    fn watcher_config(data_dir: &std::path::Path) -> Config {
        Config {
            node: crate::config::NodeConfig {
                id: 1,
                listen_addr: "127.0.0.1:0".into(),
                internal_listen_addr: None,
                peers: Vec::new(),
            },
            storage: crate::config::StorageConfig {
                data_dir: data_dir.to_path_buf(),
                memtable_arena_bytes: 4 * 1024 * 1024,
            },
            placement: crate::config::PlacementConfig {
                replication_factor: 1,
                nodes: vec![crate::config::PlacementNode {
                    id: 1,
                    primary: vec![0],
                    replica: Vec::new(),
                }],
            },
            snapshot: Default::default(),
            tls: None,
            limits: Default::default(),
        }
    }

    #[tokio::test]
    async fn invalid_hot_config_inputs_leave_route_table_unchanged() {
        let dir = tempfile::tempdir().expect("临时目录");
        let config = watcher_config(dir.path());
        let table = Arc::new(
            RouteTableManager::new(&config, dir.path().join("routes.json"))
                .expect("创建路由表管理器"),
        );
        let manager = Arc::new(ShardManager::new(1, 1));
        let path = dir.path().join("config.toml");

        // 文件缺失、语法错误和语义非法都必须保留运行期状态。
        handle_config_change(&path, &table, &manager, 1).await;
        std::fs::write(&path, "not [valid toml").expect("写入非法语法");
        handle_config_change(&path, &table, &manager, 1).await;
        std::fs::write(
            &path,
            r#"
[node]
id = 1
listen_addr = "127.0.0.1:0"

[storage]
data_dir = "./data"
"#,
        )
        .expect("写入语义非法配置");
        handle_config_change(&path, &table, &manager, 1).await;

        assert!(manager.shard_ids().await.is_empty());
        assert!(table.snapshot().await.streams.is_empty());
    }

    /// 配置与路由分属不同目录时，两个目录都必须被监听并可正常停止。
    #[tokio::test]
    async fn watcher_accepts_config_and_routes_in_distinct_directories() {
        let dir = tempfile::tempdir().expect("临时目录");
        let config = watcher_config(dir.path());
        let table = Arc::new(
            RouteTableManager::new(&config, dir.path().join("table/routes.json"))
                .expect("创建路由表管理器"),
        );
        let manager = Arc::new(ShardManager::new(1, 1));
        let config_path = dir.path().join("config/config.toml");
        let routes_path = dir.path().join("routes/routes.json");
        std::fs::create_dir_all(config_path.parent().expect("配置目录")).expect("创建配置目录");
        std::fs::create_dir_all(routes_path.parent().expect("路由目录")).expect("创建路由目录");

        spawn(config_path, routes_path, table, manager, 1)
            .expect("不同目录均可启动 watcher")
            .stop()
            .await;
    }

    /// 存储根路径是普通文件时，动态创建必须失败且不能注册半成品 shard。
    #[tokio::test]
    async fn create_shard_rejects_non_directory_data_root() {
        let dir = tempfile::tempdir().expect("临时目录");
        let data_file = dir.path().join("not-a-directory");
        std::fs::write(&data_file, b"block shard directory").expect("写入占位文件");
        let config = watcher_config(&data_file);
        let manager = Arc::new(ShardManager::new(1, 1));

        let err = create_shard_blocking(&config, &manager, 0)
            .await
            .expect_err("普通文件不能作为 shard 数据目录");
        assert!(!err.to_string().is_empty(), "失败必须携带诊断信息");
        assert!(manager.shard_ids().await.is_empty(), "失败不得注册 shard");
    }

    /// 热更新新增 shard 失败时，不能把未就绪 shard 暴露给流分配。
    #[tokio::test]
    async fn hot_config_creation_failure_keeps_previous_route_pool() {
        let dir = tempfile::tempdir().expect("临时目录");
        let data_file = dir.path().join("not-a-directory");
        std::fs::write(&data_file, b"block shard directory").expect("写入占位文件");

        let initial = watcher_config(&data_file);
        let table = Arc::new(
            RouteTableManager::new(&initial, dir.path().join("routes.json"))
                .expect("创建路由表管理器"),
        );
        let manager = Arc::new(ShardManager::new(1, 1));
        let mut reloaded = initial;
        reloaded.placement.nodes[0].primary.push(1);
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            toml::to_string(&reloaded).expect("序列化热更新配置"),
        )
        .expect("写入热更新配置");

        handle_config_change(&config_path, &table, &manager, 1).await;

        assert!(manager.shard_ids().await.is_empty(), "失败不得注册任何新增 shard");
        let (first, _) = table.allocate("route-pool-before").await.expect("分配首个流");
        let (second, _) = table.allocate("route-pool-after").await.expect("分配第二个流");
        assert_eq!(first, 0, "旧分配池只包含 shard 0");
        assert_eq!(second, 0, "创建失败后不得分配到未就绪的 shard 1");
    }
}
