//! Aggregate-only 服务器装配。

use std::sync::Arc;

use anyhow::Result;
use es_raft::{Shard, ShardManager};

use crate::aggregate_service::AggregateStoreService;
use crate::config::Config;
use crate::factory;
use crate::rpc_support::RuntimeTopology;

/// EventFS AggregateStore 服务器。
pub struct Server {
    config: Config,
    shard_manager: Arc<ShardManager>,
    topology: RuntimeTopology,
}

impl Server {
    /// 创建服务器。
    ///
    /// `config` 定义节点、放置、存储与 TLS；配置不满足放置或 TLS 不变量时返回错误。
    /// 此函数不访问数据目录，也不启动网络服务。
    pub fn new(config: Config) -> Result<Self> {
        config.validate().map_err(anyhow::Error::msg)?;
        let topology = RuntimeTopology::new(&config).map_err(anyhow::Error::msg)?;
        let shard_manager = Arc::new(ShardManager::new(config.node.id, config.shard_count()));
        Ok(Self {
            config,
            shard_manager,
            topology,
        })
    }

    /// 返回分片管理器，供管理服务、watcher 与测试复用。
    pub fn shard_manager(&self) -> &Arc<ShardManager> {
        &self.shard_manager
    }

    /// 返回启动时验证通过的配置。
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// 创建与本服务器共享运行期拓扑的 AggregateStore module。
    ///
    /// 返回值可注册到公共和内部 gRPC listener；配置 watcher 成功应用新拓扑后，
    /// 已创建的 module 会立即使用新增 Shard 和 peer。此函数不执行网络或存储 I/O。
    pub fn aggregate_store_service(&self) -> AggregateStoreService {
        AggregateStoreService::with_topology(
            self.shard_manager.clone(),
            self.topology.clone(),
            self.config.limits.max_event_bytes,
        )
    }

    /// 启动配置 watcher 并把成功的拓扑热更新发布给 AggregateStore。
    ///
    /// `config_path` 的父目录必须存在。notify 初始化失败时返回错误；后续配置解析、
    /// Shard 创建或拓扑校验失败会记录错误并保留旧运行状态。
    pub fn spawn_config_watcher(
        &self,
        config_path: std::path::PathBuf,
    ) -> Result<crate::watcher::WatcherHandle, notify::Error> {
        crate::watcher::spawn(
            config_path,
            self.shard_manager.clone(),
            self.topology.clone(),
            self.config.node.id,
        )
    }

    /// 初始化本节点承载的存储与 Raft 节点。
    ///
    /// 创建数据/快照目录并逐一注册本地 Shard；任一创建或注册失败立即返回错误。
    /// 成功后后台启动静态 peer 自举。
    pub async fn init(&self) -> Result<()> {
        tracing::info!("Initializing AggregateStore storage and Raft nodes...");
        std::fs::create_dir_all(&self.config.storage.data_dir)?;
        let snapshot_dir = self
            .config
            .snapshot
            .dir
            .clone()
            .unwrap_or_else(|| self.config.storage.data_dir.join("snapshots"));
        std::fs::create_dir_all(snapshot_dir)?;

        for shard_id in self.config.local_shards() {
            let shard = factory::create_shard(&self.config, shard_id).await?;
            self.shard_manager.register_shard(shard).await?;
        }
        tracing::info!(
            node_id = self.config.node.id,
            shard_count = self.config.local_shards().len(),
            "AggregateStore initialization complete"
        );
        self.spawn_bootstrap();
        Ok(())
    }

    /// 依次停止 Raft 并关闭存储，确保 WAL flush 且释放数据目录锁。
    pub async fn shutdown(&self) {
        let shard_ids = self.shard_manager.shard_ids().await;
        tracing::info!(count = shard_ids.len(), "Shutting down shards");
        for shard_id in shard_ids {
            match self.shard_manager.get_shard(shard_id).await {
                Ok(shard) => close_shard(&shard).await,
                Err(error) => tracing::warn!(shard_id, "关闭时取分片失败：{error}"),
            }
        }
    }

    /// 启动静态 peer 自举后台任务并返回任务句柄。
    pub fn spawn_bootstrap(&self) -> tokio::task::JoinHandle<()> {
        let config = self.config.clone();
        let shard_manager = self.shard_manager.clone();
        tokio::spawn(async move { crate::bootstrap::run(&config, shard_manager).await })
    }

    /// 启动公共及可选内部 gRPC listener。
    ///
    /// 公共 listener 仅提供 AggregateStore、RaftRpc 和 RaftAdmin；配置
    /// `internal_listen_addr` 时，内部 listener 仅提供 AggregateStoreInternal。
    /// 地址、证书读取或服务运行失败时返回错误；调用会持续到 listener 退出。
    pub async fn serve(&self) -> Result<()> {
        let public_addr: std::net::SocketAddr = self.config.node.listen_addr.parse()?;
        let aggregate_service = self.aggregate_store_service();
        let raft_service = es_raft::RaftRpcService::new(self.shard_manager.clone());
        let admin_service = es_raft::RaftAdminService::new(self.shard_manager.clone());

        let mut public_server = tonic::transport::Server::builder();
        if let Some(tls) = &self.config.tls {
            let cert = std::fs::read(tls.cert_file.as_ref().expect("配置已验证"))?;
            let key = std::fs::read(tls.key_file.as_ref().expect("配置已验证"))?;
            public_server = public_server.tls_config(
                tonic::transport::ServerTlsConfig::new()
                    .identity(tonic::transport::Identity::from_pem(cert, key)),
            )?;
        }
        let aggregate_store =
            es_proto::eventstore::aggregate_store_server::AggregateStoreServer::new(
                aggregate_service.clone(),
            )
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let raft_rpc = es_proto::eventstore::raft_rpc_server::RaftRpcServer::new(raft_service)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let raft_admin =
            es_proto::eventstore::raft_admin_server::RaftAdminServer::new(admin_service)
                .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
                .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let public_server = public_server
            .add_service(aggregate_store)
            .add_service(raft_rpc)
            .add_service(raft_admin);

        tracing::info!(addr = %public_addr, tls = self.config.tls.is_some(), "公共 gRPC 服务监听");
        if let Some(internal_addr) = &self.config.node.internal_listen_addr {
            let internal_addr: std::net::SocketAddr = internal_addr.parse()?;
            let aggregate_internal = es_proto::eventstore::aggregate_store_internal_server::AggregateStoreInternalServer::new(aggregate_service)
                .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
                .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
            let mut internal_server = tonic::transport::Server::builder();
            if let Some(tls) = &self.config.tls {
                let cert = std::fs::read(tls.cert_file.as_ref().expect("配置已验证"))?;
                let key = std::fs::read(tls.key_file.as_ref().expect("配置已验证"))?;
                internal_server = internal_server.tls_config(
                    tonic::transport::ServerTlsConfig::new()
                        .identity(tonic::transport::Identity::from_pem(cert, key)),
                )?;
            }
            tracing::info!(addr = %internal_addr, tls = self.config.tls.is_some(), "AggregateStore 内部 gRPC 服务监听");
            tokio::try_join!(
                public_server.serve(public_addr),
                internal_server
                    .add_service(aggregate_internal)
                    .serve(internal_addr),
            )?;
        } else {
            public_server.serve(public_addr).await?;
        }
        Ok(())
    }
}

async fn close_shard(shard: &Arc<Shard>) {
    tracing::info!(shard_id = shard.id(), "closing shard");
    if let Err(error) = shard.raft.shutdown().await {
        tracing::warn!(shard_id = shard.id(), "raft shutdown 失败：{error}");
    }
    if let Err(error) = shard.storage.close().await {
        tracing::warn!(shard_id = shard.id(), "storage close 失败：{error}");
    }
}
