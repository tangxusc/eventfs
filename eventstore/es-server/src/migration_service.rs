//! Migration 服务：路由表同步 RPC（节点间）。
//!
//! 路由表（stream → shard）由「专门文件 + 热更新」承载，跨节点一致性
//! 靠整表广播 + 版本号仲裁：变更节点落盘后 PushRouteTable 全表广播，
//! 接收方只采纳版本更高的表（幂等，重复广播无害）。本服务是广播的
//! 传输面，仲裁逻辑在 [`crate::route_table::RouteTableManager`]。

use std::sync::Arc;

use tonic::{Request, Response, Status};

use es_proto::eventstore::migration_server::Migration;
use es_proto::eventstore::*;

use crate::route_table::{RouteTableManager, proto_to_table, table_to_proto};

/// Migration gRPC 服务
pub struct MigrationService {
    route_table: Arc<RouteTableManager>,
}

impl MigrationService {
    pub fn new(route_table: Arc<RouteTableManager>) -> Self {
        Self { route_table }
    }
}

#[tonic::async_trait]
impl Migration for MigrationService {
    /// 拉取当前路由表（重启恢复 / 探测用）。
    async fn get_route_table(
        &self,
        _request: Request<GetRouteTableRequest>,
    ) -> Result<Response<GetRouteTableResponse>, Status> {
        let t = self.route_table.snapshot().await;
        Ok(Response::new(GetRouteTableResponse {
            table: Some(table_to_proto(&t)),
        }))
    }

    /// 接收整表广播：版本不高于本地的被忽略（幂等）。
    async fn push_route_table(
        &self,
        request: Request<PushRouteTableRequest>,
    ) -> Result<Response<PushRouteTableResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("table 不能为空"))?;
        let t = proto_to_table(Some(table));        self.route_table
            .apply_remote(t)
            .await
            .map_err(|e| Status::internal(format!("应用路由表失败: {e}")))?;
        Ok(Response::new(PushRouteTableResponse {}))
    }

    /// 原子切换流归属（迁移切换点），返回切换后的表。
    async fn set_stream_shard(
        &self,
        request: Request<SetStreamShardRequest>,
    ) -> Result<Response<SetStreamShardResponse>, Status> {
        let req = request.into_inner();
        if req.stream_id.is_empty() {
            return Err(Status::invalid_argument("stream_id 不能为空"));
        }
        let t = self
            .route_table
            .set_stream_shard(&req.stream_id, req.shard_id)
            .await
            .map_err(|e| Status::internal(format!("切换流归属失败: {e}")))?;
        Ok(Response::new(SetStreamShardResponse {
            table: Some(table_to_proto(&t)),
        }))
    }

    /// 校准 per-shard 流计数（recount），返回校准后的表。
    async fn recount_streams(
        &self,
        _request: Request<RecountStreamsRequest>,
    ) -> Result<Response<RecountStreamsResponse>, Status> {
        let t = self
            .route_table
            .recount()
            .await
            .map_err(|e| Status::internal(format!("recount 失败: {e}")))?;
        Ok(Response::new(RecountStreamsResponse {
            table: Some(table_to_proto(&t)),
        }))
    }
}
