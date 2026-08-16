//! 分片探测：`--shards` 显式 > 各端点 ListShards 并集 > 默认 8 回退。
//!
//! 节点只承载放置表分配的部分分片（非全量），旧「对 0,1,2,... 连续
//! GetRaftState 扫描、第一个 NotFound 即分片总数」的方案已失效——
//! 单个节点上扫描的第一个分片可能就是 NotFound，得到 0。
//!
//! 新方案：对每个端点调 ListShards 取各节点承载分片的并集，得到集群
//! 全部分片集合。探测逻辑与 IO 分离：并集/派生纯逻辑可单测。

use anyhow::Result;

use crate::cli::GlobalArgs;
use crate::client::ClusterClient;

/// 配置默认值（与 es-server Config::default 一致）
pub const DEFAULT_SHARD_COUNT: u64 = 8;

/// 分片范围来源
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ShardScopeSource {
    /// --shards 显式指定（兼容旧服务端 / 用户覆盖）
    Flag,
    /// ListShards 并集探测
    ListShards,
    /// 探测失败回退默认值
    DefaultFallback,
}

/// 分片范围探测结果：集群全部分片 ID 集合（升序）
#[derive(Debug, Clone, PartialEq)]
pub struct ShardScope {
    ids: Vec<u64>,
    source: ShardScopeSource,
}

impl ShardScope {
    /// 全部分片 ID（升序；init/member/status 的目标列表）。
    pub fn all_ids(&self) -> Vec<u64> {
        self.ids.clone()
    }

    /// 分片总数 = max(id) + 1（与服务端 hash 路由的 shard_count 语义一致，
    /// 支持稀疏布局——动态扩容后 id 可能不连续）。
    pub fn count(&self) -> u64 {
        self.ids.last().map_or(0, |m| m + 1)
    }

    pub fn source(&self) -> ShardScopeSource {
        self.source
    }
}

/// 探测分片范围。
///
/// 1. `--shards` 显式 → 0..N（不触网）。
/// 2. 否则逐端点 ListShards，全部响应的并集为集群分片集。
/// 3. 一个端点都没成功（或并集为空）→ 回退默认 0..8（调用方负责告警）。
pub async fn detect_shard_scope(cluster: &ClusterClient, flag: Option<u64>) -> Result<ShardScope> {
    if let Some(n) = flag {
        return Ok(ShardScope {
            ids: (0..n).collect(),
            source: ShardScopeSource::Flag,
        });
    }

    let mut all: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
    for ep in cluster.endpoints() {
        match cluster.list_shards_via(ep).await {
            Ok(resp) => all.extend(resp.shard_ids),
            // 该端点探测失败（未就绪/不承载任何分片）：跳过，继续看下一个
            Err(_) => continue,
        }
    }

    if all.is_empty() {
        return Ok(ShardScope {
            ids: (0..DEFAULT_SHARD_COUNT).collect(),
            source: ShardScopeSource::DefaultFallback,
        });
    }
    Ok(ShardScope {
        ids: all.into_iter().collect(),
        source: ShardScopeSource::ListShards,
    })
}

/// 按全局参数取分片范围；`DefaultFallback` 时向 stderr 告警。
pub async fn resolve_shard_scope(
    cluster: &ClusterClient,
    global: &GlobalArgs,
) -> Result<ShardScope> {
    let scope = detect_shard_scope(cluster, global.shards).await?;
    if scope.source() == ShardScopeSource::DefaultFallback {
        eprintln!(
            "警告：未能探测分片，按默认 0..{} 处理；请用 --shards 显式指定",
            scope.count()
        );
    }
    Ok(scope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_scope_is_contiguous_range() {
        let scope = ShardScope {
            ids: (0..4).collect(),
            source: ShardScopeSource::Flag,
        };
        assert_eq!(scope.all_ids(), vec![0, 1, 2, 3]);
        assert_eq!(scope.count(), 4);
        assert_eq!(scope.source(), ShardScopeSource::Flag);
    }

    #[test]
    fn sparse_ids_count_is_max_plus_one() {
        // 动态扩容后稀疏布局：shard 8 存在而 7 不存在 → count = 9
        let scope = ShardScope {
            ids: vec![0, 1, 8],
            source: ShardScopeSource::ListShards,
        };
        assert_eq!(scope.count(), 9);
        assert_eq!(scope.all_ids(), vec![0, 1, 8]);
    }

    #[test]
    fn empty_scope() {
        let scope = ShardScope {
            ids: Vec::new(),
            source: ShardScopeSource::DefaultFallback,
        };
        assert!(scope.all_ids().is_empty());
        assert_eq!(scope.count(), 0);
    }

    #[test]
    fn union_of_node_shard_sets() {
        // 模拟三个节点各自的承载列表，验证并集与升序
        let mut all: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        all.extend(vec![0, 1, 2]); // node1 primary
        all.extend(vec![2, 3]); // node2 primary + replica
        all.extend(vec![4]); // node3 primary
        let scope = ShardScope {
            ids: all.into_iter().collect(),
            source: ShardScopeSource::ListShards,
        };
        assert_eq!(scope.all_ids(), vec![0, 1, 2, 3, 4]);
        assert_eq!(scope.count(), 5);
    }
}
