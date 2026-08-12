//! Migration 服务：路由表同步 + 在线迁移原语（节点间）。
//!
//! 路由表（stream → shard）由「专门文件 + 热更新」承载，跨节点一致性
//! 靠整表广播 + 版本号仲裁：变更节点落盘后 PushRouteTable 全表广播，
//! 接收方只采纳版本更高的表（幂等，重复广播无害）。仲裁逻辑在
//! [`crate::route_table::RouteTableManager`]。
//!
//! 迁移原语（AppendMigrated/DeleteStreamFromShard/ReadStreamFromShard 等）
//! 按**显式 shard** 寻址，不走路由表——迁移切换后路由表已指向目标，
//! 排水阶段必须仍能读源。写入原语在目标 shard leader 上执行（非 leader
//! 返回标准重定向提示），读取原语走本地存储（无需 leader）。

use std::sync::Arc;

use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;

use es_proto::eventstore::migration_server::Migration;
use es_proto::eventstore::*;
use es_raft::ShardManager;

use crate::route_table::{RouteTableManager, proto_to_table, table_to_proto};
use crate::service::client_write_to_status;

/// Migration gRPC 服务
pub struct MigrationService {
    route_table: Arc<RouteTableManager>,
    shard_manager: Arc<ShardManager>,
}

impl MigrationService {
    pub fn new(route_table: Arc<RouteTableManager>, shard_manager: Arc<ShardManager>) -> Self {
        Self {
            route_table,
            shard_manager,
        }
    }

    /// 按显式 shard 取本地分片；不承载 → Unavailable（迁移工具先定位 leader，
    /// 打到错误节点应尽快转走）。
    async fn shard(&self, shard_id: u64) -> Result<Arc<es_raft::Shard>, Status> {
        self.shard_manager
            .get_shard(shard_id)
            .await
            .map_err(|_| {
                Status::unavailable(format!(
                    "shard {shard_id} not on this node, retry other nodes"
                ))
            })
    }

    /// 写请求打到非 leader → 标准重定向提示（与数据面 append 同格式）。
    async fn client_write(
        &self,
        shard: &Arc<es_raft::Shard>,
        req: es_storage::EsRequest,
    ) -> Result<es_storage::EsResponse, Status> {
        shard
            .raft
            .client_write(req)
            .await
            .map_err(client_write_to_status)
            .map(|r| r.data)
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
        let t = proto_to_table(Some(table));
        self.route_table
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

    /// 迁移复制写入：单事件一条 raft 日志，hlc 保留源值，Exact 版本链由
    /// 迁移工具驱动（expected_version 原样透传）。幂等索引逐事件记录，
    /// 重放安全（迁移断点续传不重复）。
    async fn append_migrated(
        &self,
        request: Request<AppendMigratedRequest>,
    ) -> Result<Response<AppendMigratedResponse>, Status> {
        let req = request.into_inner();
        if req.stream_id.is_empty() {
            return Err(Status::invalid_argument("stream_id 不能为空"));
        }
        let ev = req
            .event
            .ok_or_else(|| Status::invalid_argument("event 不能为空"))?;
        let hlc = ev
            .hlc
            .ok_or_else(|| Status::invalid_argument("event.hlc 不能为空"))?;

        let expected = match req.expected_version {
            Some(ev) => crate::service::proto_to_expected_version(ev),
            None => return Err(Status::invalid_argument("expected_version 不能为空")),
        };

        let shard = self.shard(req.shard_id).await?;
        let es_req = es_storage::EsRequest::Append {
            stream_id: req.stream_id,
            expected_version: expected,
            events: vec![es_core::NewEvent {
                event_id: uuid::Uuid::from_slice(&ev.event_id).unwrap_or_else(|_| uuid::Uuid::new_v4()),
                event_type: ev.event_type,
                data: ev.data,
                metadata: ev.metadata,
            }],
            hlc: es_core::Hlc {
                wall: hlc.wall,
                logical: hlc.logical,
            },
        };

        match self.client_write(&shard, es_req).await? {
            es_storage::EsResponse::AppendOk {
                next_expected_version,
                first_position,
                last_position,
            } => Ok(Response::new(AppendMigratedResponse {
                shard_id: req.shard_id,
                next_expected_version,
                first_position,
                last_position,
            })),
            es_storage::EsResponse::OptimisticConflict { actual_version } => {
                Err(Status::failed_precondition(format!(
                    "optimistic conflict: actual_version={}",
                    actual_version
                )))
            }
            es_storage::EsResponse::DeleteOk => Err(Status::internal("迁移写入返回 DeleteOk（不应发生）")),
        }
    }

    /// 迁移清尾：删除源 shard 上的流（幂等：不存在的流 no-op）。
    async fn delete_stream_from_shard(
        &self,
        request: Request<DeleteStreamFromShardRequest>,
    ) -> Result<Response<DeleteStreamFromShardResponse>, Status> {
        let req = request.into_inner();
        if req.stream_id.is_empty() {
            return Err(Status::invalid_argument("stream_id 不能为空"));
        }
        let shard = self.shard(req.shard_id).await?;
        self.client_write(&shard, es_storage::EsRequest::DeleteStream {
            stream_id: req.stream_id,
        })
        .await?;
        Ok(Response::new(DeleteStreamFromShardResponse {}))
    }

    type ReadStreamFromShardStream = ReceiverStream<Result<ReadEventsResponse, Status>>;

    /// 显式 shard 读流：本地存储读（不走路由表——排水/校验阶段路由表
    /// 已指向目标，按流名读会落错分片）。
    async fn read_stream_from_shard(
        &self,
        request: Request<ReadStreamFromShardRequest>,
    ) -> Result<Response<Self::ReadStreamFromShardStream>, Status> {
        let req = request.into_inner();
        let shard = self.shard(req.shard_id).await?;
        let events = shard
            .storage
            .read_stream_events(&req.stream_id, req.from_version, req.max_count)
            .map_err(|e| Status::internal(format!("read_stream_events 失败: {e}")))?;

        let proto_events: Vec<Event> = events
            .into_iter()
            .map(|e| Event {
                stream_id: e.stream_id,
                version: e.version,
                event_id: e.event_id.as_bytes().to_vec(),
                event_type: e.event_type,
                data: e.data,
                metadata: e.metadata,
                hlc: Some(Hlc {
                    wall: e.hlc.wall,
                    logical: e.hlc.logical,
                }),
                position: e.position,
                shard_id: req.shard_id,
            })
            .collect();

        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let resp = ReadEventsResponse {
                events: proto_events,
                next_positions: Vec::new(),
            };
            let _ = tx.send(Ok(resp)).await;
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// 显式 shard 读流元数据（迁移排水收敛判据）。
    async fn get_stream_meta_from_shard(
        &self,
        request: Request<GetStreamMetaFromShardRequest>,
    ) -> Result<Response<GetStreamMetaResponse>, Status> {
        let req = request.into_inner();
        let shard = self.shard(req.shard_id).await?;
        match shard
            .storage
            .read_stream_meta(&req.stream_id)
            .map_err(|e| Status::internal(format!("read_stream_meta 失败: {e}")))?
        {
            Some(m) => Ok(Response::new(GetStreamMetaResponse {
                shard_id: req.shard_id,
                exists: true,
                current_version: m.current_version,
            })),
            None => Ok(Response::new(GetStreamMetaResponse {
                shard_id: req.shard_id,
                exists: false,
                current_version: 0,
            })),
        }
    }

    /// 列出 shard 上的全部流（迁移枚举 / route check 孤儿检测）。
    async fn list_streams(
        &self,
        request: Request<ListStreamsRequest>,
    ) -> Result<Response<ListStreamsResponse>, Status> {
        let req = request.into_inner();
        let shard = self.shard(req.shard_id).await?;
        let streams = shard
            .storage
            .list_streams()
            .map_err(|e| Status::internal(format!("list_streams 失败: {e}")))?;
        Ok(Response::new(ListStreamsResponse {
            stream_ids: streams.into_iter().map(|(s, _)| s).collect(),
        }))
    }
}
