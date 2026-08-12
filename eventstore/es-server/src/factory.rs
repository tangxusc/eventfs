//! 分片工厂：创建单个分片的存储与 Raft 实例。
//!
//! 启动加载与运行期动态创建（配置热更新发现新 shard）共用此单一代码路径，
//! 保证两种入口创建的实例结构完全一致。
//!
//! 每个 shard 一个独立的 surrealkv LSM tree（`{data_dir}/shard-{id}/`），
//! 与旧「全分片共享单 tree + key 前缀隔离」布局不同——memtable arena 按
//! 配置调小（surrealkv 默认 100MB 打开即预分配，N 个 shard 就是 N×100MB）。

use std::sync::Arc;

use anyhow::Result;

use es_raft::{GrpcNetwork, Shard, TlsClientConfig};
use es_storage::EsStorage;

use crate::config::Config;

/// 创建并注册前的单个分片实例（存储 + Raft）。
///
/// 调用方负责注册进 ShardManager 与触发 bootstrap。动态创建时本函数
/// 不依赖任何已注册状态，可安全并发调用（不同 shard 互不干扰）。
pub async fn create_shard(cfg: &Config, shard_id: u64) -> Result<Arc<Shard>> {
    // 每分片独立数据目录（独立 LSM tree + 独立 LOCK）
    let shard_dir = cfg.storage.data_dir.join(format!("shard-{shard_id}"));
    std::fs::create_dir_all(&shard_dir)?;

    let tree = Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(shard_dir.clone())
            .with_max_memtable_size(cfg.storage.memtable_arena_bytes)
            .build()?,
    );
    tracing::debug!(shard_id, "opened surrealkv tree at {:?}", shard_dir);

    // 快照目录共享（SnapshotStore 按 shard_id 区分文件名，见 snapshot.rs）
    let snap_dir = cfg
        .snapshot
        .dir
        .clone()
        .unwrap_or_else(|| cfg.storage.data_dir.join("snapshots"));
    std::fs::create_dir_all(&snap_dir)?;
    let snap_cfg = es_storage::snapshot::SnapshotConfig {
        dir: snap_dir,
        compression: cfg.snapshot.compression,
        keep: cfg.snapshot.keep,
    };

    let storage = EsStorage::new(shard_id, tree, snap_cfg)?;

    // 恢复已应用状态（openraft 启动前必须调用，否则会从错误位置重放）
    storage.restore_applied_state().await?;

    // Raft 配置：单节点集群先不启用心跳，避免选举风暴
    let raft_config = Arc::new(
        openraft::Config {
            cluster_name: format!("eventstore-shard-{shard_id}"),
            heartbeat_interval: 300,
            election_timeout_min: 600,
            election_timeout_max: 900,
            // 快照策略：每 5000 条日志建一次快照，之后只保留 1000 条。
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
            max_in_snapshot_log_to_keep: 1000,
            // 分块大小来自配置：默认 3MiB，上限 6MiB（config validate 保证）
            snapshot_max_chunk_size: cfg.snapshot.max_chunk_size,
            ..Default::default()
        }
        .validate()?,
    );

    // 节点间 Raft RPC 的客户端信任策略（https 对端生效；明文集群为 None）
    let client_tls: Option<TlsClientConfig> = match &cfg.tls {
        Some(t) => Some(t.client_trust().map_err(anyhow::Error::msg)?),
        None => None,
    };

    // 每个分片一个独立的 network：RaftNetworkFactory::new_client 只传
    // target 节点不传分片，分片信息必须由工厂自身携带
    let network = GrpcNetwork::new(shard_id, client_tls);

    let raft = openraft::Raft::new(
        cfg.node.id,
        raft_config,
        network,
        storage.clone(), // RaftLogStorage
        storage.clone(), // RaftStateMachine
    )
    .await?;

    let shard = Arc::new(Shard::new(shard_id, raft, Arc::new(storage)));
    tracing::info!("Created shard {} on node {}", shard_id, cfg.node.id);
    Ok(shard)
}
