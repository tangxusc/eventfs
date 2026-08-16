//! EventFS AggregateStore、Raft 与成员管理 gRPC 契约及生成代码。
//!
//! 两组服务：
//! - [`eventstore`]：AggregateStore、Raft 管理与节点内部 API
//! - [`raft`]：节点间 Raft 通信，消息带 `shard_id` 用于路由到对应 Raft 实例

/// 客户端 API，对应 `proto/eventstore.proto`
pub mod eventstore {
    tonic::include_proto!("eventstore.v1");
}

/// 节点间 Raft 通信，对应 `proto/raft.proto`
pub mod raft {
    tonic::include_proto!("eventstore.raft.v1");
}

/// 客户端 TLS 信任策略与端点装配（https 支持）
pub mod tls;

/// 端点地址归一化（裸地址补 http:// 前缀）
pub mod endpoint;

/// 系统级 gRPC 消息大小上限
pub mod limits;

pub use tls::{TlsClientConfig, apply_endpoint_tls};

#[cfg(test)]
mod tests {
    use super::*;

    /// 固化生成代码的 Aggregate-only 对外契约。
    /// proto 改名或 package 变动会在此处编译失败，而非在下游 crate 里才暴露。
    #[test]
    fn generated_code_has_public_and_internal_service_types() {
        fn types_exist<T>() {}
        types_exist::<
            eventstore::aggregate_store_client::AggregateStoreClient<tonic::transport::Channel>,
        >();
        types_exist::<
            eventstore::aggregate_store_internal_client::AggregateStoreInternalClient<
                tonic::transport::Channel,
            >,
        >();
        types_exist::<eventstore::raft_admin_client::RaftAdminClient<tonic::transport::Channel>>();
        types_exist::<raft::raft_internal_client::RaftInternalClient<tonic::transport::Channel>>();
        let _ = std::any::type_name::<eventstore::AppendAggregateEventRequest>();
        let _ = std::any::type_name::<eventstore::FollowAggregateTypeEventsRequest>();
        let _ = std::any::type_name::<raft::RaftRequest>();
    }

    /// Aggregate OCC oneof 的四种取值必须可构造并保真携带精确版本。
    #[test]
    fn expected_aggregate_version_four_variants_constructible() {
        use eventstore::expected_aggregate_version::Kind;
        let kinds = [
            Kind::Any(eventstore::Empty {}),
            Kind::NoAggregate(eventstore::Empty {}),
            Kind::AggregateExists(eventstore::Empty {}),
            Kind::Exact(42),
        ];
        assert_eq!(kinds.len(), 4);
        // Exact 必须保真携带版本号
        match &kinds[3] {
            Kind::Exact(v) => assert_eq!(*v, 42),
            _ => panic!("第 4 项应为 Exact"),
        }
    }
}
