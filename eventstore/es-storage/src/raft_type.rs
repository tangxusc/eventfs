//! Raft 类型配置与请求响应定义。

use serde::{Deserialize, Serialize};
use std::io::Cursor;

use es_core::{ExpectedVersion, Hlc, NewEvent};

/// Raft 应用层请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EsRequest {
    /// 追加事件
    Append {
        stream_id: String,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent>,
        /// leader 在提交前分配的 HLC。
        ///
        /// 必须由 leader 分配而非各节点 apply 时各自取本地时钟：
        /// 后者会让同一条事件在不同副本上带不同时间戳，状态机不再确定性。
        hlc: Hlc,
    },
}

/// Raft 应用层响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EsResponse {
    /// 追加成功
    AppendOk {
        next_expected_version: u64,
        first_position: u64,
        last_position: u64,
    },
    /// 乐观并发冲突
    OptimisticConflict { actual_version: u64 },
}

// Raft 类型配置：绑定应用请求/响应、节点 ID、日志条目与快照数据类型
openraft::declare_raft_types!(
    pub TypeConfig:
        D = EsRequest,
        R = EsResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
