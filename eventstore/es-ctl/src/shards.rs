//! 分片数探测：`--shards` 显式 > GetRaftState 扫描 > 默认 8 回退。
//!
//! 服务端分片从 0 到 num_shards-1 连续注册、无空洞（server.rs `init`），
//! 因此对 shard 0,1,2,... 逐次 GetRaftState，第一个 `NotFound` 的 shard_id
//! 就是分片总数。探测逻辑与 IO 分离：消费响应序列的纯逻辑可单测。

use anyhow::Result;

use crate::cli::GlobalArgs;
use crate::client::ClusterClient;

/// 配置默认值（与 es-server Config::default 一致）
pub const DEFAULT_SHARD_COUNT: u64 = 8;

/// 探测上限：防止异常端点（如全部返回非 NotFound 错误）导致死循环
pub const MAX_SCAN_SHARDS: u64 = 1024;

/// 分片数来源
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ShardCountSource {
    /// --shards 显式指定
    Flag,
    /// GetRaftState 扫描探测
    Probe,
    /// 探测失败回退默认值
    DefaultFallback,
}

/// 分片数探测结果
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShardScope {
    pub count: u64,
    pub source: ShardCountSource,
}

impl ShardScope {
    /// 全部分片 ID：0..count
    pub fn all_ids(&self) -> Vec<u64> {
        (0..self.count).collect()
    }
}

/// 单个端点上一次探测的响应
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProbeStep {
    /// 分片存在（GetRaftState 成功）
    Ok,
    /// 分片不存在（not_found）：探测终止信号
    NotFound,
    /// 其它错误（连接失败等）：该端点探测失败
    Err,
}

/// 单端点响应序列的消费结果
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProbeOutcome {
    /// 在 shard_id=N 处遇到 NotFound，分片数为 N
    Found(u64),
    /// 序列中出现 Err：该端点不可靠
    EndpointFailed,
    /// 扫描超过上限
    Exceeded,
}

/// 纯逻辑：消费单端点的探测响应序列，得出该端点的探测结果。
///
/// 约定：`Ok` 计数并继续；`NotFound` 立即终止并返回当前 shard_id 为分片数
/// （0..N 无空洞 ⇒ 第一个 NotFound 处即边界）；`Err` 视为该端点探测失败。
pub fn probe_count_from_sequence(steps: impl IntoIterator<Item = ProbeStep>) -> ProbeOutcome {
    let mut count: u64 = 0;
    for step in steps {
        if count >= MAX_SCAN_SHARDS {
            return ProbeOutcome::Exceeded;
        }
        match step {
            ProbeStep::Ok => count += 1,
            ProbeStep::NotFound => return ProbeOutcome::Found(count),
            ProbeStep::Err => return ProbeOutcome::EndpointFailed,
        }
    }
    ProbeOutcome::Exceeded
}

/// 探测分片数。
///
/// 1. `--shards` 显式 → 直接用（不触网）。
/// 2. 否则依序对每个端点生成探测响应序列并消费：`Found` 即定；
///    `EndpointFailed` 换下一个端点；`Exceeded` 中止扫描。
/// 3. 全部失败 → 回退默认 8（调用方负责告警）。
pub async fn detect_shard_count(cluster: &ClusterClient, flag: Option<u64>) -> Result<ShardScope> {
    if let Some(n) = flag {
        return Ok(ShardScope {
            count: n,
            source: ShardCountSource::Flag,
        });
    }

    for ep in cluster.endpoints() {
        let outcome = probe_count_from_sequence(scan_endpoint(cluster, ep).await);
        match outcome {
            ProbeOutcome::Found(n) => {
                return Ok(ShardScope {
                    count: n,
                    source: ShardCountSource::Probe,
                });
            }
            ProbeOutcome::EndpointFailed => continue,
            ProbeOutcome::Exceeded => break,
        }
    }

    Ok(ShardScope {
        count: DEFAULT_SHARD_COUNT,
        source: ShardCountSource::DefaultFallback,
    })
}

/// IO 部分：对单个端点生成 shard 0,1,2,... 的 GetRaftState 响应序列。
async fn scan_endpoint(cluster: &ClusterClient, endpoint: &str) -> Vec<ProbeStep> {
    let mut steps = Vec::new();
    for shard_id in 0..MAX_SCAN_SHARDS {
        match cluster.get_raft_state_via(endpoint, shard_id).await {
            Ok(_) => steps.push(ProbeStep::Ok),
            Err(status) if status.code() == tonic::Code::NotFound => {
                steps.push(ProbeStep::NotFound);
                break;
            }
            Err(_) => {
                steps.push(ProbeStep::Err);
                break;
            }
        }
    }
    steps
}

/// 按全局参数取分片范围；`DefaultFallback` 时向 stderr 告警。
pub async fn resolve_shard_scope(
    cluster: &ClusterClient,
    global: &GlobalArgs,
) -> Result<ShardScope> {
    let scope = detect_shard_count(cluster, global.shards).await?;
    if scope.source == ShardCountSource::DefaultFallback {
        eprintln!(
            "警告：未能探测分片数，按默认 {} 处理；请用 --shards 显式指定",
            scope.count
        );
    }
    Ok(scope)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_ok_then_not_found_counts() {
        assert_eq!(
            probe_count_from_sequence([
                ProbeStep::Ok,
                ProbeStep::Ok,
                ProbeStep::Ok,
                ProbeStep::NotFound
            ]),
            ProbeOutcome::Found(3)
        );
    }

    #[test]
    fn first_not_found_means_zero() {
        assert_eq!(
            probe_count_from_sequence([ProbeStep::NotFound]),
            ProbeOutcome::Found(0)
        );
    }

    #[test]
    fn probe_err_means_endpoint_failed() {
        assert_eq!(
            probe_count_from_sequence([ProbeStep::Ok, ProbeStep::Err]),
            ProbeOutcome::EndpointFailed
        );
    }

    #[test]
    fn scan_over_limit_returns_exceeded() {
        // Ok × 1024 后仍继续 → 超过上限
        let steps = std::iter::repeat_n(ProbeStep::Ok, MAX_SCAN_SHARDS as usize + 1);
        assert_eq!(probe_count_from_sequence(steps), ProbeOutcome::Exceeded);
    }

    #[test]
    fn empty_sequence_returns_exceeded() {
        assert_eq!(probe_count_from_sequence([]), ProbeOutcome::Exceeded);
    }

    #[test]
    fn shard_scope_all_ids() {
        let scope = ShardScope {
            count: 3,
            source: ShardCountSource::Probe,
        };
        assert_eq!(scope.all_ids(), vec![0, 1, 2]);
        let empty = ShardScope {
            count: 0,
            source: ShardCountSource::Probe,
        };
        assert_eq!(empty.all_ids(), Vec::<u64>::new());
    }
}
