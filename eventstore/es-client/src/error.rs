//! AggregateStore 客户端错误。

use tonic::{Code, Status};

/// AggregateStore 客户端的连接、重定向和 RPC 错误。
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// 节点列表、地址或 TLS 配置非法。
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// 无法建立到目标节点的连接。
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// RPC 返回永久错误或重试预算内无法恢复的状态。
    #[error("RPC failed ({code:?}): {message}")]
    RpcFailed {
        /// gRPC 状态码。
        code: Code,
        /// 服务端错误消息。
        message: String,
    },

    /// 重试预算耗尽，包含最近一次 leader hint。
    #[error("Not leader, redirect to: {0:?}")]
    NotLeader(Option<String>),

    /// 本地请求构造阶段发现 payload 超限。
    #[error("Payload too large: {0}")]
    PayloadTooLarge(String),

    /// 轮换所有节点后仍无法完成请求。
    #[error("All nodes failed: {0}")]
    AllNodesFailed(String),
}

impl ClientError {
    /// 将 gRPC 状态转换为保留状态码和消息的客户端错误。
    pub(crate) fn from_status(status: Status) -> Self {
        Self::RpcFailed {
            code: status.code(),
            message: status.message().to_string(),
        }
    }
}
