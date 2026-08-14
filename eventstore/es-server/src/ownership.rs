//! Stream 强一致归属 module。
//!
//! external interface 只暴露常用写目标、本地已知归属与条件变更；控制 Shard
//! Raft、内部 gRPC、fencing 与 `routes.json` 投影均隐藏在 implementation 内。

#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use es_core::{Owner, OwnerMatch, OwnershipApply, OwnershipCommand, OwnershipOutcome};
use es_proto::eventstore::ownership_internal_client::OwnershipInternalClient;
use es_proto::eventstore::{CommitOwnershipRequest, InstallOwnershipFenceRequest};
use es_proto::tls::TlsClientConfig;
use es_raft::ShardManager;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Channel;
use uuid::Uuid;

use crate::config::Config;
use crate::route_table::RouteTableManager;

const CONTROL_SHARD_FILE: &str = "ownership-control.json";

#[derive(serde::Serialize, serde::Deserialize)]
struct ControlShardRecord {
    control_shard_id: u64,
}

/// Append 唯一可使用的归属目标。
#[derive(Debug, Clone)]
pub struct AppendTarget {
    owner: Owner,
    created_now: bool,
}

impl AppendTarget {
    /// 返回 Append 必须写入的目标 Shard ID。
    pub fn shard_id(&self) -> u64 {
        self.owner.shard_id()
    }

    /// 返回写入必须携带的归属代次。
    pub fn generation(&self) -> u64 {
        self.owner.generation()
    }

    /// 返回本次调用是否首次创建 Stream 归属。
    pub fn created_now(&self) -> bool {
        self.created_now
    }

    /// 返回完整只读归属，供条件迁移生成匹配值。
    pub fn owner(&self) -> &Owner {
        &self.owner
    }
}

/// Stream 归属变更意图。
#[derive(Debug, Clone)]
pub enum OwnershipChange {
    /// 条件迁移到目标 Shard。
    Move {
        /// 待迁移的 Stream 名称。
        stream: String,
        /// 调用方观察到的当前归属，用于拒绝陈旧迁移。
        expected: OwnerMatch,
        /// 迁移目标 Shard。
        target_shard: u64,
        /// 重试复用的幂等操作 ID。
        operation_id: Uuid,
    },
    /// 将权威中不存在、但存储中已有数据的 Stream 收养到目标 Shard。
    AdoptOrphan {
        /// 孤儿 Stream 名称。
        stream: String,
        /// 发现孤儿数据的源 Shard，发布前同样安装 fence。
        source_shard: u64,
        /// 已完成复制的目标 Shard。
        target_shard: u64,
    },
    /// 更新后续首次归属可使用的 Shard 集合。
    ApplyPlacement {
        /// 新的完整可分配 Shard 集合。
        eligible_shards: BTreeSet<u64>,
    },
    /// 处理运行时 `routes.json` 变化；已有权威归属不能被文件覆盖。
    ImportLegacy {
        /// watcher 从磁盘读取的兼容投影。
        table: es_core::route::RouteTable,
    },
}

/// 成功变更的收据。
#[derive(Debug, Clone)]
pub struct ChangeReceipt {
    /// 变更后的全局 revision。
    pub revision: u64,
    /// 单 Stream 变更后的归属；配置变更为 None。
    pub owner: Option<Owner>,
    /// true 表示权威状态发生变化。
    pub changed: bool,
}

/// 归属 interface 错误。
#[derive(Debug, thiserror::Error)]
pub enum OwnershipError {
    /// 暂时无法取得 leader 或 quorum，可重试。
    #[error("归属权威暂时不可用: {0}")]
    Unavailable(String),
    /// 条件变更使用了过期归属。
    #[error("归属已变化")]
    Conflict(Option<Owner>),
    /// 输入违反归属不变量。
    #[error("归属请求无效: {0}")]
    Invalid(String),
    /// 运行时文件修改试图绕过强一致归属。
    #[error("不安全的 routes.json 修改: {0}")]
    UnsafeLegacyEdit(String),
    /// 本地投影或编码失败。
    #[error("归属内部错误: {0}")]
    Internal(String),
}

impl OwnershipError {
    /// 映射为稳定的 gRPC 状态。
    ///
    /// 返回的状态码供公开和内部 RPC 统一表达可重试、冲突、非法输入与内部错误。
    pub fn into_status(self) -> tonic::Status {
        match self {
            Self::Unavailable(message) => tonic::Status::unavailable(message),
            Self::Conflict(_) => tonic::Status::aborted("stream ownership changed"),
            Self::Invalid(message) | Self::UnsafeLegacyEdit(message) => {
                tonic::Status::failed_precondition(message)
            }
            Self::Internal(message) => tonic::Status::internal(message),
        }
    }
}

/// 归属共识与数据 fencing 的内部 seam。
#[async_trait]
pub(crate) trait OwnershipCommitPort: Send + Sync {
    /// 把命令线性化提交到控制 Shard。
    async fn commit(&self, command: OwnershipCommand) -> Result<OwnershipApply, OwnershipError>;

    /// 在数据 Shard 安装单调递增的 fencing 代次。
    async fn install_fence(
        &self,
        shard_id: u64,
        stream: &str,
        generation: u64,
    ) -> Result<u64, OwnershipError>;
}

/// 调用者与测试使用的 Stream 归属 interface。
pub struct StreamOwnership {
    route_table: Arc<RouteTableManager>,
    eligible_shards: RwLock<BTreeSet<u64>>,
    port: Arc<dyn OwnershipCommitPort>,
    initialized: Mutex<bool>,
    confirmed_fences: RwLock<HashMap<String, (u64, u64)>>,
    fence_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    control_shard_id: u64,
}

impl StreamOwnership {
    /// 使用生产 Raft/gRPC adapter 构造归属 module。
    ///
    /// - `config`：启动配置，用于选择控制 Shard、放置范围、peers 与 TLS。
    /// - `shard_manager`：本节点承载的 Raft Shard。
    /// - `route_table`：`routes.json` 兼容投影管理器。
    /// - 返回：可共享的归属 module；放置表为空或 TLS 配置无效时返回错误。
    pub fn new(
        config: &Config,
        shard_manager: Arc<ShardManager>,
        route_table: Arc<RouteTableManager>,
    ) -> Result<Self, String> {
        let eligible_shards = configured_shards(config);
        let control_shard_id = load_or_persist_control_shard(config, &eligible_shards)?;
        let port = Arc::new(RaftOwnershipPort::new(
            config,
            shard_manager,
            control_shard_id,
        )?);
        Ok(Self::with_port(
            route_table,
            eligible_shards,
            control_shard_id,
            port,
        ))
    }

    /// 注入 adapter 构造归属 module；供 crate 内 interface 测试使用。
    pub(crate) fn with_port(
        route_table: Arc<RouteTableManager>,
        eligible_shards: BTreeSet<u64>,
        control_shard_id: u64,
        port: Arc<dyn OwnershipCommitPort>,
    ) -> Self {
        Self {
            route_table,
            eligible_shards: RwLock::new(eligible_shards),
            port,
            initialized: Mutex::new(false),
            confirmed_fences: RwLock::new(HashMap::new()),
            fence_locks: Mutex::new(HashMap::new()),
            control_shard_id,
        }
    }

    /// 返回本进程启动时选定的控制 Shard ID；运行期不得改变。
    pub fn control_shard_id(&self) -> u64 {
        self.control_shard_id
    }

    /// 返回 Append 的唯一写目标。
    ///
    /// - `stream`：非空 Stream 名称。
    /// - 返回：已安装 fencing 的 Shard、generation 与首次创建标记。
    /// - 错误：控制 Shard 或数据 Shard 不可用、输入非法、投影持久化失败。
    ///
    /// 已知 Stream 命中本地投影；未知 Stream 才提交控制 Shard。任一情况下，
    /// 返回前都保证目标数据 Shard 已安装相同 generation 的 fencing。
    pub async fn for_append(&self, stream: &str) -> Result<AppendTarget, OwnershipError> {
        if stream.is_empty() {
            return Err(OwnershipError::Invalid("stream 不能为空".into()));
        }
        self.ensure_initialized().await?;
        let (owner, created_now) = match self.route_table.lookup_owner(stream).await {
            Some(owner) => (owner, false),
            None => self.ensure_owner(stream).await?,
        };
        if self.confirm_fence(stream, &owner).await? {
            return Ok(AppendTarget { owner, created_now });
        }

        // 数据 Shard 已见到更高代次，说明本地投影落后于迁移提交。
        // 只在这个慢路径查询控制 Shard，更新投影后再确认新目标。
        let (refreshed, _) = self.ensure_owner(stream).await?;
        if !self.confirm_fence(stream, &refreshed).await? {
            return Err(OwnershipError::Unavailable(format!(
                "Stream {stream} 归属投影持续落后"
            )));
        }
        Ok(AppendTarget {
            owner: refreshed,
            created_now: false,
        })
    }

    /// 本地查询已知 Stream；不产生网络 I/O，也不创建归属。
    ///
    /// - `stream`：Stream 名称。
    /// - 返回：本地投影中的归属；未知时返回 `None`。
    pub async fn known(&self, stream: &str) -> Option<Owner> {
        self.route_table.lookup_owner(stream).await
    }

    /// 从控制 Shard 重新取得权威投影；兼容广播只能触发此刷新。
    pub(crate) async fn refresh_projection(&self) -> Result<ChangeReceipt, OwnershipError> {
        let applied = self
            .port
            .commit(OwnershipCommand::Bootstrap {
                legacy: self.route_table.snapshot().await,
                eligible_shards: self.eligible_shards.read().await.clone(),
            })
            .await?;
        self.route_table
            .apply_authoritative(applied.table.clone())
            .await
            .map_err(OwnershipError::Internal)?;
        *self.initialized.lock().await = true;
        Ok(ChangeReceipt {
            revision: applied.table.version,
            owner: None,
            changed: true,
        })
    }

    /// 写状态机拒绝旧代次后刷新归属，并返回唯一可重试目标。
    pub(crate) async fn recover_fenced(
        &self,
        stream: &str,
        attempted_generation: u64,
    ) -> Result<AppendTarget, OwnershipError> {
        let mut confirmed = self.confirmed_fences.write().await;
        if confirmed
            .get(stream)
            .is_some_and(|(_, generation)| *generation <= attempted_generation)
        {
            confirmed.remove(stream);
        }
        drop(confirmed);

        let (owner, _) = self.ensure_owner(stream).await?;
        if owner.generation() <= attempted_generation || !self.confirm_fence(stream, &owner).await?
        {
            return Err(OwnershipError::Unavailable(format!(
                "Stream {stream} 正在切换归属，请重试"
            )));
        }
        Ok(AppendTarget {
            owner,
            created_now: false,
        })
    }

    /// 提交迁移、放置表或旧文件调和。
    ///
    /// - `change`：携带条件与幂等 ID 的归属变更意图。
    /// - 返回：提交后的 revision、可选归属与是否变化。
    /// - 错误：无 quorum、条件冲突、输入非法、不安全旧文件编辑或持久化失败。
    pub async fn change(&self, change: OwnershipChange) -> Result<ChangeReceipt, OwnershipError> {
        self.ensure_initialized().await?;
        match change {
            OwnershipChange::Move {
                stream,
                expected,
                target_shard,
                operation_id,
            } => {
                let prepared = self
                    .port
                    .commit(OwnershipCommand::PrepareMove {
                        operation_id,
                        stream: stream.clone(),
                        expected,
                        target_shard,
                    })
                    .await?;
                let (current, generation, canonical_operation) = match prepared.outcome {
                    OwnershipOutcome::Owner { owner, .. } if owner.shard_id() == target_shard => {
                        self.route_table
                            .apply_authoritative(prepared.table.clone())
                            .await
                            .map_err(OwnershipError::Internal)?;
                        return Ok(ChangeReceipt {
                            revision: prepared.table.version,
                            owner: Some(owner),
                            changed: false,
                        });
                    }
                    OwnershipOutcome::MovePrepared {
                        current,
                        generation,
                        operation_id,
                        ..
                    } => (current, generation, operation_id),
                    OwnershipOutcome::Conflict { current } => {
                        return Err(OwnershipError::Conflict(current));
                    }
                    OwnershipOutcome::Invalid { reason } => {
                        return Err(OwnershipError::Invalid(reason));
                    }
                    other => {
                        return Err(OwnershipError::Internal(format!(
                            "PrepareMove 返回意外结果: {other:?}"
                        )));
                    }
                };

                // 先允许目标新代次，再拒绝源旧代次；发布前可能短暂不可写，
                // 但不会出现源和目标同时接受旧代次的窗口。
                self.port
                    .install_fence(target_shard, &stream, generation)
                    .await?;
                self.port
                    .install_fence(current.shard_id(), &stream, generation)
                    .await?;
                let completed = self
                    .port
                    .commit(OwnershipCommand::CompleteMove {
                        operation_id: canonical_operation,
                        stream: stream.clone(),
                    })
                    .await?;
                self.route_table
                    .apply_authoritative(completed.table.clone())
                    .await
                    .map_err(OwnershipError::Internal)?;
                let owner = match completed.outcome {
                    OwnershipOutcome::Owner { owner, .. } => owner,
                    other => {
                        return Err(OwnershipError::Internal(format!(
                            "CompleteMove 返回意外结果: {other:?}"
                        )));
                    }
                };
                self.confirmed_fences
                    .write()
                    .await
                    .insert(stream, (owner.shard_id(), owner.generation()));
                Ok(ChangeReceipt {
                    revision: completed.table.version,
                    owner: Some(owner),
                    changed: true,
                })
            }
            OwnershipChange::ApplyPlacement { eligible_shards } => {
                let applied = self
                    .port
                    .commit(OwnershipCommand::ApplyPlacement {
                        eligible_shards: eligible_shards.clone(),
                    })
                    .await?;
                match applied.outcome {
                    OwnershipOutcome::Snapshot => {}
                    OwnershipOutcome::Invalid { reason } => {
                        return Err(OwnershipError::Invalid(reason));
                    }
                    other => {
                        return Err(OwnershipError::Internal(format!(
                            "ApplyPlacement 返回意外结果: {other:?}"
                        )));
                    }
                }
                let previous = self.eligible_shards.read().await.clone();
                *self.eligible_shards.write().await = eligible_shards;
                self.route_table
                    .apply_authoritative(applied.table.clone())
                    .await
                    .map_err(OwnershipError::Internal)?;
                Ok(ChangeReceipt {
                    revision: applied.table.version,
                    owner: None,
                    changed: previous != *self.eligible_shards.read().await,
                })
            }
            OwnershipChange::AdoptOrphan {
                stream,
                source_shard,
                target_shard,
            } => {
                let applied = self
                    .port
                    .commit(OwnershipCommand::AdoptOrphan {
                        stream: stream.clone(),
                        target_shard,
                    })
                    .await?;
                let owner = match applied.outcome {
                    OwnershipOutcome::Owner { owner, .. } => owner,
                    OwnershipOutcome::Conflict { current } => {
                        return Err(OwnershipError::Conflict(current));
                    }
                    OwnershipOutcome::Invalid { reason } => {
                        return Err(OwnershipError::Invalid(reason));
                    }
                    other => {
                        return Err(OwnershipError::Internal(format!(
                            "AdoptOrphan 返回意外结果: {other:?}"
                        )));
                    }
                };
                self.port
                    .install_fence(target_shard, &stream, owner.generation())
                    .await?;
                self.port
                    .install_fence(source_shard, &stream, owner.generation())
                    .await?;
                self.route_table
                    .apply_authoritative(applied.table.clone())
                    .await
                    .map_err(OwnershipError::Internal)?;
                self.confirmed_fences
                    .write()
                    .await
                    .insert(stream, (owner.shard_id(), owner.generation()));
                Ok(ChangeReceipt {
                    revision: applied.table.version,
                    owner: Some(owner),
                    changed: true,
                })
            }
            OwnershipChange::ImportLegacy { table } => {
                let current = self.route_table.snapshot().await;
                if current == table {
                    return Ok(ChangeReceipt {
                        revision: current.version,
                        owner: None,
                        changed: false,
                    });
                }
                self.route_table
                    .restore_projection()
                    .await
                    .map_err(OwnershipError::Internal)?;
                Err(OwnershipError::UnsafeLegacyEdit(
                    "运行时文件不能新增、删除或改写 Stream 归属；请使用 create-stream 或 migrate"
                        .into(),
                ))
            }
        }
    }

    async fn ensure_initialized(&self) -> Result<(), OwnershipError> {
        if *self.initialized.lock().await {
            return Ok(());
        }
        let mut initialized = self.initialized.lock().await;
        if *initialized {
            return Ok(());
        }
        let applied = self
            .port
            .commit(OwnershipCommand::Bootstrap {
                legacy: self.route_table.snapshot().await,
                eligible_shards: self.eligible_shards.read().await.clone(),
            })
            .await?;
        self.route_table
            .apply_authoritative(applied.table)
            .await
            .map_err(OwnershipError::Internal)?;
        *initialized = true;
        Ok(())
    }

    async fn ensure_owner(&self, stream: &str) -> Result<(Owner, bool), OwnershipError> {
        let applied = self
            .port
            .commit(OwnershipCommand::Ensure {
                stream: stream.to_string(),
                eligible_shards: self.eligible_shards.read().await.clone(),
            })
            .await?;
        self.route_table
            .apply_authoritative(applied.table.clone())
            .await
            .map_err(OwnershipError::Internal)?;
        match applied.outcome {
            OwnershipOutcome::Owner { owner, created } => Ok((owner, created)),
            OwnershipOutcome::Invalid { reason } => Err(OwnershipError::Invalid(reason)),
            OwnershipOutcome::Conflict { current } => Err(OwnershipError::Conflict(current)),
            other => Err(OwnershipError::Internal(format!(
                "Ensure 返回意外结果: {other:?}"
            ))),
        }
    }

    async fn confirm_fence(&self, stream: &str, owner: &Owner) -> Result<bool, OwnershipError> {
        let expected = (owner.shard_id(), owner.generation());
        let stream_lock = {
            let mut locks = self.fence_locks.lock().await;
            locks
                .entry(stream.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = stream_lock.lock().await;
        if self.confirmed_fences.read().await.get(stream) == Some(&expected) {
            return Ok(true);
        }
        let current = self
            .port
            .install_fence(owner.shard_id(), stream, owner.generation())
            .await?;
        if current > owner.generation() {
            return Ok(false);
        }
        self.confirmed_fences
            .write()
            .await
            .insert(stream.to_string(), expected);
        Ok(true)
    }
}

fn configured_shards(config: &Config) -> BTreeSet<u64> {
    config
        .placement
        .nodes
        .iter()
        .flat_map(|node| node.primary.iter().chain(node.replica.iter()))
        .copied()
        .collect()
}

fn load_or_persist_control_shard(
    config: &Config,
    eligible_shards: &BTreeSet<u64>,
) -> Result<u64, String> {
    let selected = eligible_shards
        .iter()
        .next()
        .copied()
        .ok_or_else(|| "放置表为空，无法选择控制 Shard".to_string())?;
    let path = config.storage.data_dir.join(CONTROL_SHARD_FILE);
    let control_shard_id = match std::fs::read(&path) {
        Ok(bytes) => {
            serde_json::from_slice::<ControlShardRecord>(&bytes)
                .map_err(|error| format!("控制 Shard 元数据损坏: {error}"))?
                .control_shard_id
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(&config.storage.data_dir)
                .map_err(|error| format!("创建数据目录失败: {error}"))?;
            let tmp = path.with_extension("json.tmp");
            let bytes = serde_json::to_vec_pretty(&ControlShardRecord {
                control_shard_id: selected,
            })
            .map_err(|error| format!("编码控制 Shard 元数据失败: {error}"))?;
            {
                use std::io::Write;
                let mut file = std::fs::File::create(&tmp)
                    .map_err(|error| format!("创建控制 Shard 临时文件失败: {error}"))?;
                file.write_all(&bytes)
                    .map_err(|error| format!("写入控制 Shard 元数据失败: {error}"))?;
                file.sync_all()
                    .map_err(|error| format!("同步控制 Shard 元数据失败: {error}"))?;
            }
            std::fs::rename(&tmp, &path)
                .map_err(|error| format!("提交控制 Shard 元数据失败: {error}"))?;
            selected
        }
        Err(error) => return Err(format!("读取控制 Shard 元数据失败: {error}")),
    };
    if !eligible_shards.contains(&control_shard_id) {
        return Err(format!(
            "持久化控制 Shard {control_shard_id} 不在当前放置表中"
        ));
    }
    Ok(control_shard_id)
}

struct RaftOwnershipPort {
    shard_manager: Arc<ShardManager>,
    control_shard_id: u64,
    peer_addrs: std::collections::BTreeMap<u64, String>,
    tls: Option<TlsClientConfig>,
}

impl RaftOwnershipPort {
    fn new(
        config: &Config,
        shard_manager: Arc<ShardManager>,
        control_shard_id: u64,
    ) -> Result<Self, String> {
        let tls = config
            .tls
            .as_ref()
            .map(|tls| tls.client_trust())
            .transpose()?;
        Ok(Self {
            shard_manager,
            control_shard_id,
            peer_addrs: config
                .node
                .peers
                .iter()
                .filter_map(|peer| {
                    peer.internal_addr
                        .as_ref()
                        .map(|addr| (peer.id, es_raft::normalize_endpoint(addr)))
                })
                .collect(),
            tls,
        })
    }

    async fn ownership_client(
        &self,
        addr: &str,
    ) -> Result<OwnershipInternalClient<Channel>, OwnershipError> {
        let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
            .map_err(|error| OwnershipError::Unavailable(error.to_string()))?;
        let endpoint = es_proto::tls::apply_endpoint_tls(endpoint, self.tls.as_ref())
            .map_err(|error| OwnershipError::Unavailable(error.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|error| OwnershipError::Unavailable(error.to_string()))?;
        Ok(OwnershipInternalClient::new(channel)
            .max_encoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE)
            .max_decoding_message_size(es_proto::limits::MAX_GRPC_MESSAGE_SIZE))
    }

    async fn commit_remote(
        &self,
        addr: &str,
        command: &OwnershipCommand,
    ) -> Result<OwnershipApply, OwnershipError> {
        let payload = bincode::serde::encode_to_vec(command, bincode::config::standard())
            .map_err(|error| OwnershipError::Internal(error.to_string()))?;
        let response = self
            .ownership_client(addr)
            .await?
            .commit_ownership(CommitOwnershipRequest {
                control_shard_id: self.control_shard_id,
                payload,
            })
            .await
            .map_err(|error| OwnershipError::Unavailable(error.to_string()))?
            .into_inner();
        let (applied, _): (OwnershipApply, usize) =
            bincode::serde::decode_from_slice(&response.payload, bincode::config::standard())
                .map_err(|error| OwnershipError::Internal(error.to_string()))?;
        Ok(applied)
    }

    async fn fence_remote(
        &self,
        addr: &str,
        shard_id: u64,
        stream: &str,
        generation: u64,
    ) -> Result<u64, OwnershipError> {
        let response = self
            .ownership_client(addr)
            .await?
            .install_ownership_fence(InstallOwnershipFenceRequest {
                shard_id,
                stream_id: stream.to_string(),
                generation,
            })
            .await
            .map_err(|error| OwnershipError::Unavailable(error.to_string()))?
            .into_inner();
        Ok(response.generation)
    }

    fn forward_node_id(
        error: &openraft::error::RaftError<
            u64,
            openraft::error::ClientWriteError<u64, openraft::BasicNode>,
        >,
    ) -> Option<u64> {
        match error {
            openraft::error::RaftError::APIError(
                openraft::error::ClientWriteError::ForwardToLeader(forward),
            ) => forward.leader_id,
            _ => None,
        }
    }
}

#[async_trait]
impl OwnershipCommitPort for RaftOwnershipPort {
    async fn commit(&self, command: OwnershipCommand) -> Result<OwnershipApply, OwnershipError> {
        let mut preferred = None;
        let mut last_error = None;
        if let Ok(shard) = self.shard_manager.get_shard(self.control_shard_id).await {
            match shard
                .raft
                .client_write(es_storage::EsRequest::CommitOwnership {
                    command: command.clone(),
                })
                .await
            {
                Ok(response) => {
                    return match response.data {
                        es_storage::EsResponse::OwnershipApplied(applied) => Ok(applied),
                        other => Err(OwnershipError::Internal(format!(
                            "控制 Shard 返回意外结果: {other:?}"
                        ))),
                    };
                }
                Err(error) => {
                    preferred = Self::forward_node_id(&error)
                        .and_then(|node_id| self.peer_addrs.get(&node_id).cloned());
                    last_error = Some(error.to_string());
                }
            }
        }
        let mut candidates = preferred
            .into_iter()
            .chain(self.peer_addrs.values().cloned());
        for addr in &mut candidates {
            match self.commit_remote(&addr, &command).await {
                Ok(applied) => return Ok(applied),
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        Err(OwnershipError::Unavailable(
            last_error.unwrap_or_else(|| "找不到控制 Shard leader".into()),
        ))
    }

    async fn install_fence(
        &self,
        shard_id: u64,
        stream: &str,
        generation: u64,
    ) -> Result<u64, OwnershipError> {
        let mut preferred = None;
        let mut last_error = None;
        if let Ok(shard) = self.shard_manager.get_shard(shard_id).await {
            match shard
                .raft
                .client_write(es_storage::EsRequest::InstallOwnershipFence {
                    stream_id: stream.to_string(),
                    generation,
                })
                .await
            {
                Ok(response) => {
                    return match response.data {
                        es_storage::EsResponse::OwnershipFenceInstalled {
                            generation: current,
                        } if current >= generation => Ok(current),
                        other => Err(OwnershipError::Internal(format!(
                            "数据 Shard 返回意外 fencing 结果: {other:?}"
                        ))),
                    };
                }
                Err(error) => {
                    preferred = Self::forward_node_id(&error)
                        .and_then(|node_id| self.peer_addrs.get(&node_id).cloned());
                    last_error = Some(error.to_string());
                }
            }
        }
        let mut candidates = preferred
            .into_iter()
            .chain(self.peer_addrs.values().cloned());
        for addr in &mut candidates {
            match self.fence_remote(&addr, shard_id, stream, generation).await {
                Ok(current) => return Ok(current),
                Err(error) => last_error = Some(error.to_string()),
            }
        }
        Err(OwnershipError::Unavailable(last_error.unwrap_or_else(
            || format!("找不到 Shard {shard_id} leader"),
        )))
    }
}

/// interface 测试使用的线性化内存 adapter。
#[cfg(test)]
#[derive(Default)]
struct MemoryOwnershipPort {
    catalog: Mutex<es_core::OwnershipCatalog>,
    fences: Mutex<BTreeMap<(u64, String), u64>>,
    install_calls: Mutex<Vec<(u64, String, u64)>>,
}

#[async_trait]
#[cfg(test)]
impl OwnershipCommitPort for MemoryOwnershipPort {
    async fn commit(&self, command: OwnershipCommand) -> Result<OwnershipApply, OwnershipError> {
        Ok(self.catalog.lock().await.apply(command))
    }

    async fn install_fence(
        &self,
        shard_id: u64,
        stream: &str,
        generation: u64,
    ) -> Result<u64, OwnershipError> {
        self.install_calls
            .lock()
            .await
            .push((shard_id, stream.to_string(), generation));
        let mut fences = self.fences.lock().await;
        let current = fences.entry((shard_id, stream.to_string())).or_default();
        *current = (*current).max(generation);
        Ok(*current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(data_dir: &std::path::Path) -> Config {
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
                    primary: vec![0, 1],
                    replica: Vec::new(),
                }],
            },
            snapshot: Default::default(),
            tls: None,
            limits: Default::default(),
        }
    }

    fn test_ownership() -> (
        tempfile::TempDir,
        Arc<StreamOwnership>,
        Arc<MemoryOwnershipPort>,
    ) {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let config = test_config(dir.path());
        let table = Arc::new(
            RouteTableManager::new(&config, dir.path().join("routes.json"))
                .expect("创建路由表管理器"),
        );
        let port = Arc::new(MemoryOwnershipPort::default());
        let ownership = Arc::new(StreamOwnership::with_port(
            table,
            BTreeSet::from([0, 1]),
            0,
            port.clone(),
        ));
        (dir, ownership, port)
    }

    #[test]
    fn persisted_control_shard_survives_lower_shard_addition() {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let mut initial = test_config(dir.path());
        initial.placement.nodes[0].primary = vec![10, 11];
        let table = Arc::new(
            RouteTableManager::new(&initial, dir.path().join("routes.json"))
                .expect("创建初始路由表"),
        );
        let first = StreamOwnership::new(&initial, Arc::new(ShardManager::new(1, 12)), table)
            .expect("首次选择控制 Shard");
        assert_eq!(first.control_shard_id(), 10);

        let mut expanded = initial;
        expanded.placement.nodes[0].primary.insert(0, 0);
        let table = Arc::new(
            RouteTableManager::new(&expanded, dir.path().join("routes.json"))
                .expect("创建扩容后路由表"),
        );
        let restarted = StreamOwnership::new(&expanded, Arc::new(ShardManager::new(1, 12)), table)
            .expect("重启恢复控制 Shard");
        assert_eq!(restarted.control_shard_id(), 10);
    }

    #[tokio::test]
    async fn concurrent_first_append_returns_one_owner_and_installs_one_fence() {
        let (_dir, ownership, port) = test_ownership();
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..32 {
            let ownership = ownership.clone();
            tasks
                .spawn(async move { ownership.for_append("orders/42").await.expect("取得写目标") });
        }

        let mut targets = Vec::new();
        while let Some(result) = tasks.join_next().await {
            targets.push(result.expect("并发任务完成"));
        }
        assert!(targets
            .iter()
            .all(|target| target.shard_id() == targets[0].shard_id()));
        assert_eq!(
            targets.iter().filter(|target| target.created_now()).count(),
            1,
            "只有一个调用应观察到首次创建"
        );
        assert_eq!(port.install_calls.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn refresh_projection_discards_forged_higher_version() {
        let (_dir, ownership, _port) = test_ownership();
        let owner = ownership
            .for_append("orders/authority")
            .await
            .expect("创建权威归属")
            .owner()
            .clone();
        let mut forged = es_core::route::RouteTable::new();
        forged.insert("orders/authority", 1);
        forged.version = 999;
        ownership
            .route_table
            .apply_authoritative(forged)
            .await
            .expect("注入测试投影");

        ownership.refresh_projection().await.expect("刷新权威投影");
        assert_eq!(ownership.known("orders/authority").await, Some(owner));
        assert_eq!(ownership.route_table.snapshot().await.version, 1);
    }

    #[tokio::test]
    async fn move_installs_new_generation_on_target_and_source_before_publish() {
        let (_dir, ownership, port) = test_ownership();
        let initial = ownership
            .for_append("orders/7")
            .await
            .expect("创建初始归属");
        assert_eq!(initial.shard_id(), 0);

        let receipt = ownership
            .change(OwnershipChange::Move {
                stream: "orders/7".into(),
                expected: initial.owner().match_token(),
                target_shard: 1,
                operation_id: Uuid::new_v4(),
            })
            .await
            .expect("迁移归属");
        let moved = receipt.owner.expect("返回迁移后归属");
        assert_eq!(moved.shard_id(), 1);
        assert_eq!(moved.generation(), 2);

        let fences = port.fences.lock().await;
        assert_eq!(fences.get(&(0, "orders/7".into())), Some(&2));
        assert_eq!(fences.get(&(1, "orders/7".into())), Some(&2));
        drop(fences);
        assert_eq!(ownership.known("orders/7").await, Some(moved));
    }

    #[tokio::test]
    async fn orphan_adoption_is_conditional_and_fences_source_and_target() {
        let (_dir, ownership, port) = test_ownership();
        let receipt = ownership
            .change(OwnershipChange::AdoptOrphan {
                stream: "orders/orphan".into(),
                source_shard: 0,
                target_shard: 1,
            })
            .await
            .expect("收养孤儿流");
        let owner = receipt.owner.expect("返回新归属");
        assert_eq!(owner.shard_id(), 1);
        assert_eq!(owner.generation(), 1);
        let fences = port.fences.lock().await;
        assert_eq!(fences.get(&(0, "orders/orphan".into())), Some(&1));
        assert_eq!(fences.get(&(1, "orders/orphan".into())), Some(&1));
        drop(fences);

        let conflict = ownership
            .change(OwnershipChange::AdoptOrphan {
                stream: "orders/orphan".into(),
                source_shard: 0,
                target_shard: 0,
            })
            .await
            .expect_err("已有不同归属时必须冲突");
        assert!(matches!(conflict, OwnershipError::Conflict(_)));
    }

    #[tokio::test]
    async fn placement_change_only_affects_future_streams() {
        let (_dir, ownership, _port) = test_ownership();
        let existing = ownership
            .for_append("existing")
            .await
            .expect("创建已有 Stream");
        ownership
            .change(OwnershipChange::ApplyPlacement {
                eligible_shards: BTreeSet::from([1]),
            })
            .await
            .expect("更新放置范围");

        let future = ownership
            .for_append("future")
            .await
            .expect("创建后续 Stream");
        assert_eq!(existing.shard_id(), 0);
        assert_eq!(
            ownership.known("existing").await,
            Some(existing.owner().clone())
        );
        assert_eq!(future.shard_id(), 1);
    }

    #[tokio::test]
    async fn fenced_write_refreshes_stale_projection_from_authority() {
        let (_dir, ownership, port) = test_ownership();
        let initial = ownership
            .for_append("stale-route")
            .await
            .expect("创建初始归属");
        let operation_id = Uuid::new_v4();
        let prepared = port
            .commit(OwnershipCommand::PrepareMove {
                operation_id,
                stream: "stale-route".into(),
                expected: initial.owner().match_token(),
                target_shard: 1,
            })
            .await
            .expect("模拟另一节点准备迁移");
        let generation = match prepared.outcome {
            OwnershipOutcome::MovePrepared { generation, .. } => generation,
            other => panic!("预期 MovePrepared，实际 {other:?}"),
        };
        port.install_fence(1, "stale-route", generation)
            .await
            .expect("安装目标 fence");
        port.install_fence(0, "stale-route", generation)
            .await
            .expect("安装源 fence");
        port.commit(OwnershipCommand::CompleteMove {
            operation_id,
            stream: "stale-route".into(),
        })
        .await
        .expect("模拟另一节点完成迁移");

        let refreshed = ownership
            .recover_fenced("stale-route", initial.generation())
            .await
            .expect("旧代次被拒后刷新归属");
        assert_eq!(refreshed.shard_id(), 1);
        assert_eq!(refreshed.generation(), generation);
        assert_eq!(
            ownership
                .known("stale-route")
                .await
                .expect("本地投影已刷新")
                .shard_id(),
            1
        );
    }

    #[test]
    fn ownership_errors_map_to_stable_grpc_codes() {
        use tonic::Code;

        let cases = [
            (
                OwnershipError::Unavailable("quorum unavailable".into()),
                Code::Unavailable,
            ),
            (OwnershipError::Conflict(None), Code::Aborted),
            (
                OwnershipError::Invalid("invalid ownership".into()),
                Code::FailedPrecondition,
            ),
            (
                OwnershipError::UnsafeLegacyEdit("unsafe edit".into()),
                Code::FailedPrecondition,
            ),
            (
                OwnershipError::Internal("projection failure".into()),
                Code::Internal,
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(error.into_status().code(), expected);
        }
    }

    #[tokio::test]
    async fn invalid_append_is_rejected_before_consensus() {
        let (_dir, ownership, port) = test_ownership();

        let error = ownership
            .for_append("")
            .await
            .expect_err("空 Stream 必须拒绝");

        assert!(matches!(error, OwnershipError::Invalid(_)));
        assert_eq!(port.catalog.lock().await.revision(), 0);
        assert!(port.install_calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn move_to_current_shard_is_an_idempotent_noop() {
        let (_dir, ownership, port) = test_ownership();
        let initial = ownership
            .for_append("orders/noop-move")
            .await
            .expect("创建初始归属");
        let calls_before = port.install_calls.lock().await.len();

        let receipt = ownership
            .change(OwnershipChange::Move {
                stream: "orders/noop-move".into(),
                expected: initial.owner().match_token(),
                target_shard: initial.shard_id(),
                operation_id: Uuid::new_v4(),
            })
            .await
            .expect("迁移到当前 Shard 应幂等成功");

        assert!(!receipt.changed);
        assert_eq!(receipt.owner, Some(initial.owner().clone()));
        assert_eq!(port.install_calls.lock().await.len(), calls_before);
    }

    #[tokio::test]
    async fn placement_change_rejects_empty_and_reports_noop() {
        let (_dir, ownership, _port) = test_ownership();

        let error = ownership
            .change(OwnershipChange::ApplyPlacement {
                eligible_shards: BTreeSet::new(),
            })
            .await
            .expect_err("空放置表必须拒绝");
        assert!(matches!(error, OwnershipError::Invalid(_)));

        let receipt = ownership
            .change(OwnershipChange::ApplyPlacement {
                eligible_shards: BTreeSet::from([0, 1]),
            })
            .await
            .expect("相同放置表应幂等成功");
        assert!(!receipt.changed);
        assert!(receipt.owner.is_none());
    }

    #[tokio::test]
    async fn legacy_projection_accepts_same_snapshot_and_restores_tampering() {
        let (_dir, ownership, _port) = test_ownership();
        ownership
            .for_append("orders/canonical")
            .await
            .expect("创建权威归属");
        let canonical = ownership.route_table.snapshot().await;

        let noop = ownership
            .change(OwnershipChange::ImportLegacy {
                table: canonical.clone(),
            })
            .await
            .expect("相同兼容投影无需处理");
        assert!(!noop.changed);

        let mut tampered = canonical.clone();
        tampered.insert("orders/unsafe", 1);
        let error = ownership
            .change(OwnershipChange::ImportLegacy { table: tampered })
            .await
            .expect_err("篡改兼容投影必须拒绝");
        assert!(matches!(error, OwnershipError::UnsafeLegacyEdit(_)));
        assert_eq!(ownership.route_table.snapshot().await, canonical);
    }

    #[tokio::test]
    async fn append_rejects_when_data_fence_stays_ahead_of_authority() {
        let (_dir, ownership, port) = test_ownership();
        port.fences
            .lock()
            .await
            .insert((0, "orders/ahead-fence".into()), 2);

        let error = ownership
            .for_append("orders/ahead-fence")
            .await
            .expect_err("数据 fence 更高时不能使用陈旧归属");

        assert_eq!(
            error.to_string(),
            "归属权威暂时不可用: Stream orders/ahead-fence 归属投影持续落后"
        );
        assert!(ownership.known("orders/ahead-fence").await.is_some());
    }

    #[tokio::test]
    async fn fenced_recovery_handles_missing_cache_and_non_advancing_owner() {
        let (_dir, ownership, _port) = test_ownership();

        let initial = ownership
            .recover_fenced("orders/recover", 0)
            .await
            .expect("没有缓存时从权威创建并确认归属");
        assert_eq!(initial.generation(), 1);

        let error = ownership
            .recover_fenced("orders/recover", 1)
            .await
            .expect_err("权威代次未推进时不能重试旧写");
        assert_eq!(
            error.to_string(),
            "归属权威暂时不可用: Stream orders/recover 正在切换归属，请重试"
        );
    }

    #[tokio::test]
    async fn move_surfaces_missing_stream_conflict_and_invalid_target() {
        let (_dir, ownership, _port) = test_ownership();
        let expected = OwnerMatch {
            shard_id: 0,
            generation: 1,
        };

        let missing = ownership
            .change(OwnershipChange::Move {
                stream: "orders/missing".into(),
                expected,
                target_shard: 1,
                operation_id: Uuid::new_v4(),
            })
            .await
            .expect_err("未知 Stream 不能迁移");
        assert_eq!(missing.to_string(), "归属已变化");

        let current = ownership
            .for_append("orders/invalid-target")
            .await
            .expect("创建初始归属");
        let invalid = ownership
            .change(OwnershipChange::Move {
                stream: "orders/invalid-target".into(),
                expected: current.owner().match_token(),
                target_shard: 99,
                operation_id: Uuid::new_v4(),
            })
            .await
            .expect_err("放置表外目标必须拒绝");
        assert_eq!(
            invalid.to_string(),
            "归属请求无效: 目标 Shard 99 不在可分配集合中"
        );
    }

    #[tokio::test]
    async fn raft_adapter_without_local_shard_or_peer_is_unavailable() {
        let dir = tempfile::tempdir().expect("创建临时目录");
        let config = test_config(dir.path());
        let port = RaftOwnershipPort::new(&config, Arc::new(ShardManager::new(1, 2)), 0)
            .expect("构造生产 adapter");

        let commit_error = port
            .commit(OwnershipCommand::ApplyPlacement {
                eligible_shards: BTreeSet::from([0, 1]),
            })
            .await
            .expect_err("没有控制 Shard leader 时必须拒绝提交");
        assert_eq!(
            commit_error.to_string(),
            "归属权威暂时不可用: 找不到控制 Shard leader"
        );

        let fence_error = port
            .install_fence(1, "orders/unavailable", 1)
            .await
            .expect_err("没有数据 Shard leader 时必须拒绝 fencing");
        assert_eq!(
            fence_error.to_string(),
            "归属权威暂时不可用: 找不到 Shard 1 leader"
        );
    }
}
