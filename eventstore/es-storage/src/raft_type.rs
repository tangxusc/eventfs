//! Raft 类型配置与请求响应定义。

use serde::{Deserialize, Serialize};

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
    /// 删除流（在线迁移清尾用）：同事务删除该流全部事件、StreamMeta、
    /// 幂等索引与 position 指针。删除不存在的流 = no-op（幂等）。
    DeleteStream { stream_id: String },
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
    /// 删除流成功（含删除不存在流的幂等 no-op）
    DeleteOk,
}

// Raft 类型配置：绑定应用请求/响应、节点 ID、日志条目与快照数据类型
openraft::declare_raft_types!(
    pub TypeConfig:
        D = EsRequest,
        R = EsResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        // 快照数据为文件句柄：openraft 默认分块传输（Chunked）直接流式读文件，
        // 不再一次性载入内存（docs/snapshot.md）
        SnapshotData = crate::snapshot::SnapshotFile,
        AsyncRuntime = openraft::TokioRuntime,
);
