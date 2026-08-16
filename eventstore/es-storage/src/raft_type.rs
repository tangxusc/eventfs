//! Raft 类型配置与请求响应定义。

use serde::{Deserialize, Serialize};

use es_core::{
    AggregateCatalogApply, AggregateCatalogCommand, AggregateTypeId, ExpectedAggregateVersion,
    ExpectedStateRevision, Hlc, NewAggregateEvent,
};

/// 数据分区上的聚合消费者组状态转换。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregateGroupPartitionCommand {
    Claim {
        consumer_id: String,
        now_ms: u64,
        deadline_ms: u64,
        max_claim: u32,
        max_bytes: u64,
        candidates: Vec<es_core::AggregateDeliveryCandidate>,
    },
    Settle {
        consumer_id: String,
        now_ms: u64,
        settlements: Vec<es_core::AggregateSettlement>,
    },
    Renew {
        consumer_id: String,
        deadline_ms: u64,
        delivery_ids: Vec<uuid::Uuid>,
    },
    Expire {
        now_ms: u64,
    },
}

/// Raft 应用层请求
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EsRequest {
    /// 追加单条聚合事件；实例版本和分区位置在状态机中分配。
    AggregateAppend {
        aggregate_type: AggregateTypeId,
        partition_id: u16,
        partition_generation: u64,
        aggregate_id: String,
        expected_version: ExpectedAggregateVersion,
        event: NewAggregateEvent,
        hlc: Hlc,
    },
    /// 用打开时 revision 对业务状态文档执行 CAS 覆盖。
    PutAggregateState {
        aggregate_type: AggregateTypeId,
        partition_id: u16,
        partition_generation: u64,
        aggregate_id: String,
        expected_revision: ExpectedStateRevision,
        data: Vec<u8>,
        hlc: es_core::Hlc,
    },
    /// 在数据 Shard 安装虚拟事件分区的单调 generation fence。
    InstallAggregatePartitionFence {
        aggregate_type: AggregateTypeId,
        partition_id: u16,
        generation: u64,
    },
    /// 在控制 Shard 串行提交聚合类型 catalog 命令。
    CommitAggregateCatalog { command: AggregateCatalogCommand },
    /// 在控制 Shard 串行提交聚合消费者组定义。
    CommitAggregateGroupCatalog {
        command: es_core::AggregateGroupCatalogCommand,
    },
    /// 在数据 Shard 原子推进单个组分区的 checkpoint、lease 与重试。
    AggregateGroupPartition {
        aggregate_type: AggregateTypeId,
        partition_id: u16,
        partition_generation: u64,
        group_name: String,
        group_epoch: u64,
        start_position: u64,
        settings: es_core::AggregateGroupSettings,
        command: AggregateGroupPartitionCommand,
    },
}

/// Raft 应用层响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EsResponse {
    /// Raft 空日志或成员变更没有领域返回值。
    Noop,
    /// 聚合事件追加成功。
    AggregateAppendOk {
        aggregate_version: u64,
        partition_position: u64,
    },
    /// 聚合实例版本条件冲突；`None` 表示实例不存在。
    AggregateOptimisticConflict { actual_version: Option<u64> },
    /// 同一 `event_id` 已绑定到不同内容。
    AggregateIdempotencyConflict,
    /// 目标聚合实例不存在。
    AggregateNotFound,
    /// 业务状态文档提交成功。
    AggregateStateStored {
        state: es_core::AggregateStateDocument,
    },
    /// 业务状态 revision 条件冲突；`None` 表示文档不存在。
    AggregateStateConflict { actual_revision: Option<u64> },
    /// 虚拟事件分区 fence 已安装。
    AggregatePartitionFenceInstalled { generation: u64 },
    /// 请求携带的虚拟事件分区 generation 已过期或尚未安装。
    AggregatePartitionFenced { current_generation: u64 },
    /// 聚合类型 catalog 状态转换结果。
    AggregateCatalogApplied(AggregateCatalogApply),
    /// 聚合存储请求违反稳定输入约束。
    AggregateInvalid { reason: String },
    /// 聚合消费者组 catalog 状态转换结果。
    AggregateGroupCatalogApplied(es_core::AggregateGroupCatalogApply),
    /// 聚合消费者组取得的 delivery 引用。
    AggregateGroupClaimed(Vec<es_core::AggregateGroupDelivery>),
    /// 聚合消费者组结算结果。
    AggregateGroupSettled(Vec<es_core::AggregateSettlementResult>),
    /// 聚合消费者组续租结果。
    AggregateGroupRenewed(Vec<es_core::AggregateSettlementResult>),
    /// 聚合消费者组回收的超时 delivery 数量。
    AggregateGroupExpired(u64),
    /// 请求携带的组 epoch 已过期。
    AggregateGroupStaleEpoch { current_epoch: u64 },
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
