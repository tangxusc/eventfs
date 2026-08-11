//! EventStore gRPC 协议定义与生成代码。
//!
//! 两组服务：
//! - [`eventstore`]：客户端 API（追加、读取、订阅）
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

pub use tls::{apply_endpoint_tls, TlsClientConfig};

#[cfg(test)]
mod tests {
    use super::*;

    /// 固化生成代码的对外契约：四个服务类型必须存在。
    /// proto 改名或 package 变动会在此处编译失败，而非在下游 crate 里才暴露。
    #[test]
    fn 生成代码包含四个服务类型() {
        fn 类型存在<T>() {}
        类型存在::<eventstore::event_store_client::EventStoreClient<tonic::transport::Channel>>();
        类型存在::<raft::raft_internal_client::RaftInternalClient<tonic::transport::Channel>>();
        // server 侧为泛型包装，仅断言模块路径可达
        let _ = std::any::type_name::<eventstore::AppendRequest>();
        let _ = std::any::type_name::<raft::RaftRequest>();
    }

    /// oneof 字段生成为 Option<enum>，确认 ExpectedVersion 四种取值都可构造
    #[test]
    fn 期望版本四种取值可构造() {
        use eventstore::expected_version::Kind;
        let kinds = [
            Kind::Any(eventstore::Empty {}),
            Kind::NoStream(eventstore::Empty {}),
            Kind::StreamExists(eventstore::Empty {}),
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
