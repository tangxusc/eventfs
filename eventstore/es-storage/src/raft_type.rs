//! Raft 类型配置与请求响应定义。

use serde::{Deserialize, Serialize};

use es_core::{ExpectedVersion, Hlc, NewEvent, OwnershipApply, OwnershipCommand};

/// 控制 Shard 上的持久化订阅命令。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PersistentSubscriptionCommand {
    /// 创建组；同名组已存在时返回冲突。
    Create { group: es_core::PersistentGroup },
    /// 用 revision CAS 替换组状态，供配置更新/reset 使用。
    Replace {
        name: String,
        expected_revision: u64,
        group: es_core::PersistentGroup,
    },
    /// 删除组。
    Delete {
        name: String,
        expected_revision: u64,
    },
    /// 把 `$all` 新发现的 Stream 原子加入进度表。
    EnsureStreams {
        name: String,
        streams: std::collections::BTreeMap<String, es_core::StreamProgress>,
    },
    /// 原子取得 delivery 租约。
    Claim {
        name: String,
        consumer_id: String,
        now_ms: u64,
        deadline_ms: u64,
        candidates: Vec<es_core::DeliveryCandidate>,
    },
    /// 批量确认或失败处理。
    Settle {
        name: String,
        consumer_id: String,
        group_epoch: u64,
        now_ms: u64,
        settlements: Vec<es_core::Settlement>,
    },
    /// 回收超时租约。
    Expire { name: String, now_ms: u64 },
    /// 全量重放 parked。
    ReplayParked { name: String, now_ms: u64 },
    /// ownership generation 变化时重置受影响 Stream，避免迁移版本重排造成漏投。
    ReconcileOwnership {
        name: String,
        generations: std::collections::BTreeMap<String, u64>,
    },
}

/// 持久化订阅命令结果。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PersistentSubscriptionResponse {
    Group(es_core::PersistentGroup),
    Claimed(Vec<es_core::PersistentDelivery>),
    Settled(Vec<es_core::SettlementResult>),
    Deleted,
    Count(u64),
    NotFound,
    Conflict { actual_revision: u64 },
    Invalid { reason: String },
}

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
    /// 携带已提交归属代次的公开写入；状态机拒绝过期代次。
    ///
    /// 新变体必须追加在旧 `Append`、`DeleteStream` 之后，保持 bincode 编号稳定。
    AppendOwned {
        stream_id: String,
        ownership_generation: u64,
        expected_version: ExpectedVersion,
        events: Vec<NewEvent>,
        hlc: Hlc,
    },
    /// 在控制 Shard 串行提交归属命令。
    CommitOwnership { command: OwnershipCommand },
    /// 在数据 Shard 安装单调递增的归属代次 fencing。
    InstallOwnershipFence { stream_id: String, generation: u64 },
    /// 在控制 Shard 串行提交持久化订阅状态转换。
    ///
    /// 新变体只能追加，保持已有 bincode 枚举编号稳定。
    PersistentSubscription {
        command: PersistentSubscriptionCommand,
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
    /// 删除流成功（含删除不存在流的幂等 no-op）
    DeleteOk,
    /// 归属状态机命令结果。
    OwnershipApplied(OwnershipApply),
    /// 数据 Shard 已安装的当前 fencing 代次。
    OwnershipFenceInstalled { generation: u64 },
    /// 写入携带的归属代次已过期或尚未安装。
    OwnershipFenced { current_generation: u64 },
    /// 持久化订阅状态转换结果。
    PersistentSubscription(PersistentSubscriptionResponse),
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

#[cfg(test)]
mod compatibility_tests {
    use super::*;

    #[allow(dead_code)]
    #[derive(Serialize)]
    enum LegacyEsRequest {
        Append {
            stream_id: String,
            expected_version: ExpectedVersion,
            events: Vec<NewEvent>,
            hlc: Hlc,
        },
        DeleteStream {
            stream_id: String,
        },
    }

    #[allow(dead_code)]
    #[derive(Serialize)]
    enum PreviousEsRequest {
        Append {
            stream_id: String,
            expected_version: ExpectedVersion,
            events: Vec<NewEvent>,
            hlc: Hlc,
        },
        DeleteStream {
            stream_id: String,
        },
        AppendOwned {
            stream_id: String,
            ownership_generation: u64,
            expected_version: ExpectedVersion,
            events: Vec<NewEvent>,
            hlc: Hlc,
        },
        CommitOwnership {
            command: OwnershipCommand,
        },
        InstallOwnershipFence {
            stream_id: String,
            generation: u64,
        },
    }

    #[test]
    fn decodes_delete_stream_from_previous_raft_format() {
        let bytes = bincode::serde::encode_to_vec(
            LegacyEsRequest::DeleteStream {
                stream_id: "orders/legacy".into(),
            },
            bincode::config::standard(),
        )
        .expect("编码旧请求");
        let (decoded, consumed): (EsRequest, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .expect("新版本必须能读取旧 Raft 日志");
        assert_eq!(consumed, bytes.len());
        assert!(matches!(
            decoded,
            EsRequest::DeleteStream { stream_id } if stream_id == "orders/legacy"
        ));
    }

    #[test]
    fn decodes_last_variant_from_format_before_persistent_subscriptions() {
        let bytes = bincode::serde::encode_to_vec(
            PreviousEsRequest::InstallOwnershipFence {
                stream_id: "orders/legacy".into(),
                generation: 7,
            },
            bincode::config::standard(),
        )
        .expect("编码旧请求");
        let (decoded, consumed): (EsRequest, usize) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard())
                .expect("新增订阅变体后仍必须能读取旧 Raft 日志");
        assert_eq!(consumed, bytes.len());
        assert!(matches!(
            decoded,
            EsRequest::InstallOwnershipFence { stream_id, generation }
                if stream_id == "orders/legacy" && generation == 7
        ));
    }
}
