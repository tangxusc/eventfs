//! esctl 命令分发上下文。

use anyhow::Result;

use crate::cli::{Format, GlobalArgs};
use crate::client::ClusterClient;
use crate::shards::{ShardScope, resolve_shard_scope};

pub mod aggregate;
pub mod init;
pub mod member;
pub mod snapshot;
pub mod status;

/// 命令执行上下文：集群连接、全局参数、输出格式与惰性 Shard 范围。
pub struct Ctx {
    pub cluster: ClusterClient,
    pub global: GlobalArgs,
    pub format: Format,
    shard_scope: tokio::sync::Mutex<Option<ShardScope>>,
}

impl Ctx {
    /// 从集群客户端与全局参数创建命令上下文。
    pub fn new(cluster: ClusterClient, global: GlobalArgs) -> Self {
        Self {
            format: global.write_out,
            cluster,
            global,
            shard_scope: tokio::sync::Mutex::new(None),
        }
    }

    /// 探测并缓存当前命令使用的 Shard 范围。
    ///
    /// 显式 `--shards` 优先；否则合并各端点 ListShards 结果，探测失败返回错误。
    pub async fn shards(&self) -> Result<ShardScope> {
        let mut cached = self.shard_scope.lock().await;
        if let Some(scope) = cached.as_ref() {
            return Ok(scope.clone());
        }
        let scope = resolve_shard_scope(&self.cluster, &self.global).await?;
        *cached = Some(scope.clone());
        Ok(scope)
    }
}
