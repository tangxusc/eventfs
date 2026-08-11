//! 服务器主结构。

use std::sync::Arc;
use anyhow::Result;

use es_raft::{ShardManager, GrpcNetwork, Shard};
use es_storage::EsStorage;
use crate::config::Config;
use crate::service::EsService;

/// EventStore 服务器
pub struct Server {
    config: Config,
    shard_manager: Arc<ShardManager>,
}

impl Server {
    /// 创建服务器实例
    pub fn new(config: Config) -> Result<Self> {
        let shard_manager = Arc::new(ShardManager::new(
            config.node.id,
            config.shards.num_shards,
        ));

        Ok(Self {
            config,
            shard_manager,
        })
    }

    /// 获取分片管理器（测试用）
    pub fn shard_manager(&self) -> &Arc<ShardManager> {
        &self.shard_manager
    }

    /// 初始化存储与 Raft 节点
    pub async fn init(&self) -> Result<()> {
        tracing::info!("Initializing storage and Raft nodes...");

        // 创建数据目录
        std::fs::create_dir_all(&self.config.storage.data_dir)?;

        // 打开共享 tree（所有分片共享，通过 key 前缀隔离）
        let tree_path = self.config.storage.data_dir.clone();
        let tree = Arc::new(
            surrealkv::TreeBuilder::new()
                .with_path(tree_path)
                .build()?,
        );

        tracing::info!("Opened shared surrealkv tree at {:?}", self.config.storage.data_dir);

        // 为每个分片创建存储 + Raft 节点
        for shard_id in 0..self.config.shards.num_shards {
            let storage = EsStorage::new(shard_id, tree.clone())?;

            // 恢复已应用状态（openraft 启动前必须调用，否则会从错误位置重放）
            storage.restore_applied_state().await?;

            // 创建 Raft 配置：单节点集群先不启用心跳，避免选举风暴
            let raft_config = Arc::new(
                openraft::Config {
                    cluster_name: format!("eventstore-shard-{}", shard_id),
                    heartbeat_interval: 300,
                    election_timeout_min: 600,
                    election_timeout_max: 900,
                    // 快照策略：每 5000 条日志建一次快照，之后只保留 1000 条。
                    // 不设的话日志会无限增长——磁盘被吃满，且新节点加入时
                    // 需重放全部历史日志，恢复时间随运行时长线性增长。
                    snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
                    max_in_snapshot_log_to_keep: 1000,
                    ..Default::default()
                }
                .validate()?,
            );

            // 每个分片一个独立的 network：RaftNetworkFactory::new_client 只传
            // target 节点不传分片，分片信息必须由工厂自身携带
            let network = GrpcNetwork::new(shard_id);

            let raft = openraft::Raft::new(
                self.config.node.id,
                raft_config,
                network,
                storage.clone(), // RaftLogStorage
                storage.clone(), // RaftStateMachine
            )
            .await?;

            let shard = Arc::new(Shard::new(shard_id, raft, Arc::new(storage)));
            self.shard_manager.register_shard(shard).await?;

            tracing::info!("Initialized shard {} on node {}", shard_id, self.config.node.id);
        }

        tracing::info!("Initialization complete: {} shards", self.config.shards.num_shards);

        // 配置了 node.peers 时后台自动组建集群（etcd 静态引导语义）。
        // 不阻塞 serve：组建失败仅告警，可经 RaftAdmin 手动接管。
        self.spawn_bootstrap();

        Ok(())
    }

    /// 后台自动组建集群任务。
    ///
    /// 在 serve 绑定端口之前 spawn：任务第一步会 TCP 轮询等所有 peers（含自己）
    /// 端口就绪，serve 绑定在毫秒级内完成，时序自洽。
    pub fn spawn_bootstrap(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let sm = self.shard_manager.clone();
        tokio::spawn(async move { crate::bootstrap::run(&config, sm).await })
    }

    /// 启动 gRPC 服务器。
    ///
    /// 三个服务共用一个端口：客户端 API、Raft 节点间 RPC、集群管理 API。
    pub async fn serve(&self) -> Result<()> {
        let addr = self.config.node.listen_addr.parse()?;
        let es_service = EsService::new(self.shard_manager.clone());
        let raft_service = es_raft::RaftRpcService::new(self.shard_manager.clone());
        let admin_service = es_raft::RaftAdminService::new(self.shard_manager.clone());

        tracing::info!("gRPC 服务监听 {}", addr);

        tonic::transport::Server::builder()
            .add_service(es_proto::eventstore::event_store_server::EventStoreServer::new(es_service))
            .add_service(es_proto::eventstore::raft_rpc_server::RaftRpcServer::new(raft_service))
            .add_service(es_proto::eventstore::raft_admin_server::RaftAdminServer::new(
                admin_service,
            ))
            .serve(addr)
            .await?;

        Ok(())
    }
}
