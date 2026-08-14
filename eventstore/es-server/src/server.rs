//! 服务器主结构。

use anyhow::Result;
use std::sync::Arc;

use crate::config::Config;
use crate::factory;
use crate::migration_service::MigrationService;
use crate::ownership::StreamOwnership;
use crate::route_table::{routes_path, RouteTableManager};
use crate::service::EsService;
use es_raft::{Shard, ShardManager};

/// EventStore 服务器
pub struct Server {
    config: Config,
    shard_manager: Arc<ShardManager>,
    /// 流路由表（stream → shard 归属；启动加载 + 热更新）
    route_table: Arc<RouteTableManager>,
    /// Stream 强一致归属 module。
    ownership: Arc<StreamOwnership>,
}

impl Server {
    /// 创建服务器实例
    pub fn new(config: Config) -> Result<Self> {
        // 配置启动期校验（fail-fast）：放置表不变式、TLS cert/key 成对且文件存在
        config.validate().map_err(anyhow::Error::msg)?;

        // shard 总数 = 放置表派生值（max shard_id + 1，允许稀疏布局）。
        // 注意：本节点只创建/注册 local_shards，未承载的分片 id < shard_count，
        // register_shard 的上界校验据此保持有效。
        let shard_count = config.shard_count();

        let shard_manager = Arc::new(ShardManager::new(config.node.id, shard_count));

        // 路由表：{data_dir}/routes.json（专门文件 + 热更新）
        let route_table = Arc::new(
            RouteTableManager::new(&config, routes_path(&config.storage.data_dir))
                .map_err(anyhow::Error::msg)?,
        );
        let ownership = Arc::new(
            StreamOwnership::new(&config, shard_manager.clone(), route_table.clone())
                .map_err(anyhow::Error::msg)?,
        );

        Ok(Self {
            config,
            shard_manager,
            route_table,
            ownership,
        })
    }

    /// 获取分片管理器（测试用）
    pub fn shard_manager(&self) -> &Arc<ShardManager> {
        &self.shard_manager
    }

    /// 获取当前配置（watcher/测试用）
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// 获取路由表管理器（测试与 watcher 用）
    pub fn route_table(&self) -> &Arc<RouteTableManager> {
        &self.route_table
    }

    /// 获取 Stream 强一致归属 module（watcher/测试用）。
    pub fn ownership(&self) -> &Arc<StreamOwnership> {
        &self.ownership
    }

    /// 初始化存储与 Raft 节点（本节点承载的 shards，每个 shard 独立 LSM tree）。
    pub async fn init(&self) -> Result<()> {
        tracing::info!("Initializing storage and Raft nodes...");

        // 加载路由表（本地文件优先，缺失时向 peers 拉取）
        self.route_table.load().await.map_err(anyhow::Error::msg)?;

        // 创建数据目录
        std::fs::create_dir_all(&self.config.storage.data_dir)?;

        // 快照目录：缺省 {data_dir}/snapshots，独立于 surrealkv 业务数据文件
        // （create_shard 也会幂等创建，这里先建保证早期失败路径不炸）
        let snap_dir = self
            .config
            .snapshot
            .dir
            .clone()
            .unwrap_or_else(|| self.config.storage.data_dir.join("snapshots"));
        std::fs::create_dir_all(&snap_dir)?;

        // 为每个承载的分片创建存储 + Raft 节点（每 shard 一个独立 LSM tree）
        for shard_id in self.config.local_shards() {
            let shard = factory::create_shard(&self.config, shard_id).await?;
            self.shard_manager.register_shard(shard).await?;
        }

        tracing::info!(
            "Initialization complete: {} shards on node {}",
            self.config.local_shards().len(),
            self.config.node.id
        );

        // 配置了 node.peers 时后台自动组建集群（etcd 静态引导语义）。
        // 不阻塞 serve：组建失败仅告警，可经 RaftAdmin 手动接管。
        self.spawn_bootstrap();

        Ok(())
    }

    /// 优雅关闭：逐 shard 停 Raft 并关闭存储（flush WAL + 释放 LOCK 文件）。
    ///
    /// surrealkv 的 `Tree::drop` 在无 tokio runtime 的路径下不会异步关闭，
    /// 锁会一直持有到进程退出；进程重启前必须显式关闭，否则同目录
    /// 重新打开会报 "already locked"。
    pub async fn shutdown(&self) {
        let ids = self.shard_manager.shard_ids().await;
        tracing::info!("Shutting down {} shards...", ids.len());
        for id in ids {
            match self.shard_manager.get_shard(id).await {
                Ok(shard) => close_shard(&shard).await,
                Err(e) => tracing::warn!(shard_id = id, "关闭时取分片失败：{e}"),
            }
        }
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
    /// 公共端口提供客户端 API、Raft 节点间 RPC 与集群管理 API；内部订阅和
    /// 归属控制 RPC 仅在 `node.internal_listen_addr` 配置的专用端口监听。
    /// 配置 [tls] 时以 TLS（https）监听，否则明文。
    pub async fn serve(&self) -> Result<()> {
        let addr: std::net::SocketAddr = self.config.node.listen_addr.parse()?;
        let es_service = EsService::with_ownership(
            self.shard_manager.clone(),
            self.config.limits.clone(),
            self.route_table.clone(),
            self.ownership.clone(),
            &self.config,
        )
        .map_err(anyhow::Error::msg)?;
        let raft_service = es_raft::RaftRpcService::new(self.shard_manager.clone());
        let admin_service = es_raft::RaftAdminService::new(self.shard_manager.clone());
        let migration_service = MigrationService::new(
            self.route_table.clone(),
            self.shard_manager.clone(),
            self.ownership.clone(),
        );

        let mut public_server = tonic::transport::Server::builder();
        // tls_config 必须在 add_service 之前
        if let Some(tls) = &self.config.tls {
            let cert = std::fs::read(tls.cert_file.as_ref().unwrap())?;
            let key = std::fs::read(tls.key_file.as_ref().unwrap())?;
            let identity = tonic::transport::Identity::from_pem(cert, key);
            public_server = public_server
                .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
        }

        tracing::info!(
            "gRPC 服务监听 {}（TLS: {}）",
            addr,
            if self.config.tls.is_some() {
                "https"
            } else {
                "http"
            }
        );

        // 系统级 8MB 消息契约（es_proto::limits::MAX_GRPC_MESSAGE_SIZE）：
        // - 快照分块默认 3MiB/块 + bincode 头，比 tonic 默认 4MB 需要更宽余量；
        //   openraft 0.9.25 对超限快照块直接放弃传输（无拆小路径），
        //   块大小上限 6MiB 由 config validate 保证不会触线。
        // - append 批量超限由 es-raft 网络层在发送前映射为 openraft
        //   PayloadTooLarge 拆小重试（可自愈），无需依赖这里的上限兜底。
        // tonic 0.14 中该限制在服务级配置；编码方向同样显式设置，
        // 与客户端解码上限对齐。
        let event_store =
            es_proto::eventstore::event_store_server::EventStoreServer::new(es_service.clone())
                .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
                .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let raft_rpc = es_proto::eventstore::raft_rpc_server::RaftRpcServer::new(raft_service)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let raft_admin =
            es_proto::eventstore::raft_admin_server::RaftAdminServer::new(admin_service)
                .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
                .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let migration =
            es_proto::eventstore::migration_server::MigrationServer::new(migration_service.clone())
                .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
                .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
        let public_server = public_server
            .add_service(event_store)
            .add_service(raft_rpc)
            .add_service(raft_admin)
            .add_service(migration);

        match &self.config.node.internal_listen_addr {
            Some(internal_addr) => {
                let internal_addr: std::net::SocketAddr = internal_addr.parse()?;
                let internal_subscription = es_proto::eventstore::internal_subscription_server::InternalSubscriptionServer::new(es_service)
                    .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
                    .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
                let ownership_internal =
                    es_proto::eventstore::ownership_internal_server::OwnershipInternalServer::new(
                        migration_service,
                    )
                    .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
                    .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE);
                let mut internal_server = tonic::transport::Server::builder();
                if let Some(tls) = &self.config.tls {
                    let cert = std::fs::read(tls.cert_file.as_ref().unwrap())?;
                    let key = std::fs::read(tls.key_file.as_ref().unwrap())?;
                    let identity = tonic::transport::Identity::from_pem(cert, key);
                    internal_server = internal_server
                        .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?;
                }
                tracing::info!(
                    "内部订阅服务监听 {}（TLS: {}）",
                    internal_addr,
                    if self.config.tls.is_some() {
                        "https"
                    } else {
                        "http"
                    }
                );
                tokio::try_join!(
                    public_server.serve(addr),
                    internal_server
                        .add_service(internal_subscription)
                        .add_service(ownership_internal)
                        .serve(internal_addr),
                )?;
            }
            None => public_server.serve(addr).await?,
        }

        Ok(())
    }
}

/// 关闭单个分片：先停 Raft（停止心跳/选举任务），再关存储（flush + 释放 LOCK）。
///
/// 顺序不可颠倒：Raft 停止后再关存储，避免关闭期间 Raft 后台任务还在写。
/// openraft 的 shutdown 会等待内部任务退出（存在超时上限，见其实现）。
async fn close_shard(shard: &Arc<Shard>) {
    tracing::info!(shard_id = shard.id(), "closing shard...");
    if let Err(e) = shard.raft.shutdown().await {
        tracing::warn!(shard_id = shard.id(), "raft shutdown 失败：{e}");
    }
    if let Err(e) = shard.storage.close().await {
        tracing::warn!(shard_id = shard.id(), "storage close 失败：{e}");
    }
    tracing::info!(shard_id = shard.id(), "shard closed");
}
