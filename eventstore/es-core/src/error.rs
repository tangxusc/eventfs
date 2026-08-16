//! 统一错误类型。

use thiserror::Error;

/// EventFS 领域错误。
#[derive(Error, Debug)]
pub enum Error {
    /// 非 leader 节点，应重定向到 leader
    #[error("非 leader 节点，请重定向到: {leader_addr:?}")]
    NotLeader { leader_addr: Option<String> },

    /// Raft 共识层错误
    #[error("Raft 错误: {0}")]
    Raft(String),

    /// 存储层错误
    #[error("存储错误: {0}")]
    Storage(String),

    /// 序列化/反序列化错误
    #[error("序列化错误: {0}")]
    Serde(String),

    /// 分片不在本节点
    #[error("分片 {shard_id} 不由本节点服务")]
    ShardNotLocal { shard_id: u64 },

    /// 资源未找到
    #[error("未找到: {0}")]
    NotFound(String),

    /// 请求无效
    #[error("无效请求: {0}")]
    InvalidRequest(String),

    /// 输入无效
    #[error("无效输入: {0}")]
    InvalidInput(String),

    /// 未知错误
    #[error("内部错误: {0}")]
    Internal(String),
}

/// 结果类型别名
pub type Result<T> = std::result::Result<T, Error>;
