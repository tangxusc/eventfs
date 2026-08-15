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

use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use es_proto::eventstore::migration_server::Migration;
use es_proto::eventstore::ownership_internal_server::OwnershipInternal;
use es_proto::eventstore::*;
use es_raft::ShardManager;

use crate::ownership::{OwnershipChange, StreamOwnership};
use crate::route_table::{RouteTableManager, table_to_proto};
use crate::service::client_write_to_status;

/// Migration gRPC 服务
#[derive(Clone)]
pub struct MigrationService {
    route_table: Arc<RouteTableManager>,
    shard_manager: Arc<ShardManager>,
    ownership: Arc<StreamOwnership>,
}

impl MigrationService {
    /// 构造节点间迁移协议实现。
    ///
    /// - `route_table`：归属兼容投影，用于读取和发布结果。
    /// - `shard_manager`：显式 Shard 读写与 Raft 提交入口。
    /// - `ownership`：迁移切换必须经过的强一致归属 module。
    /// - 返回：可注册到 tonic 的迁移协议实现；构造过程不执行 I/O。
    pub fn new(
        route_table: Arc<RouteTableManager>,
        shard_manager: Arc<ShardManager>,
        ownership: Arc<StreamOwnership>,
    ) -> Self {
        Self {
            route_table,
            shard_manager,
            ownership,
        }
    }

    /// 按显式 shard 取本地分片；不承载 → Unavailable（迁移工具先定位 leader，
    /// 打到错误节点应尽快转走）。
    async fn shard(&self, shard_id: u64) -> Result<Arc<es_raft::Shard>, Status> {
        self.shard_manager.get_shard(shard_id).await.map_err(|_| {
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

fn move_change(req: SetStreamShardRequest) -> Result<OwnershipChange, Status> {
    if req.stream_id.is_empty() {
        return Err(Status::invalid_argument("stream_id 不能为空"));
    }
    let operation_id = uuid::Uuid::from_slice(&req.operation_id)
        .map_err(|_| Status::invalid_argument("operation_id 必须是 16 字节 UUID"))?;
    if req.expected_generation == 0 {
        return Ok(OwnershipChange::AdoptOrphan {
            stream: req.stream_id,
            source_shard: req.expected_shard_id,
            target_shard: req.shard_id,
        });
    }
    Ok(OwnershipChange::Move {
        stream: req.stream_id,
        expected: es_core::OwnerMatch {
            shard_id: req.expected_shard_id,
            generation: req.expected_generation,
        },
        target_shard: req.shard_id,
        operation_id,
    })
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

    /// 接收兼容广播通知；payload 不参与归属裁决，必须回控制 Shard 刷新。
    async fn push_route_table(
        &self,
        request: Request<PushRouteTableRequest>,
    ) -> Result<Response<PushRouteTableResponse>, Status> {
        let req = request.into_inner();
        let table = req
            .table
            .ok_or_else(|| Status::invalid_argument("table 不能为空"))?;
        let _ = table;
        self.ownership
            .refresh_projection()
            .await
            .map_err(crate::ownership::OwnershipError::into_status)?;
        Ok(Response::new(PushRouteTableResponse {}))
    }

    /// 原子切换流归属（迁移切换点），返回切换后的表。
    async fn set_stream_shard(
        &self,
        request: Request<SetStreamShardRequest>,
    ) -> Result<Response<SetStreamShardResponse>, Status> {
        self.ownership
            .change(move_change(request.into_inner())?)
            .await
            .map_err(crate::ownership::OwnershipError::into_status)?;
        let t = self.route_table.snapshot().await;
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

        // event_id 必须是合法 16 字节 UUID（幂等索引以它为键，静默替换
        // 会让重试重复追加）
        let event_id = uuid::Uuid::from_slice(&ev.event_id).map_err(|_| {
            Status::invalid_argument(format!(
                "event_id 必须是 16 字节 UUID，实际 {} 字节",
                ev.event_id.len()
            ))
        })?;

        let shard = self.shard(req.shard_id).await?;
        let es_req = es_storage::EsRequest::Append {
            stream_id: req.stream_id,
            expected_version: expected,
            events: vec![es_core::NewEvent {
                event_id,
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
            other => Err(Status::internal(format!("迁移写入返回意外结果: {other:?}"))),
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
        self.client_write(
            &shard,
            es_storage::EsRequest::DeleteStream {
                stream_id: req.stream_id,
            },
        )
        .await?;
        Ok(Response::new(DeleteStreamFromShardResponse {}))
    }

    type ReadStreamFromShardStream = ReceiverStream<Result<ReadEventsResponse, Status>>;

    /// 显式 shard 读流：本地存储读（不走路由表——排水/校验阶段路由表
    /// 已指向目标，按流名读会落错分片）。
    ///
    /// 分块发送：整条流打包进单条 gRPC 消息会突破 8MB 消息上限
    /// （大流迁移/校验必然失败），按 CHUNK 条一块流式发送。
    async fn read_stream_from_shard(
        &self,
        request: Request<ReadStreamFromShardRequest>,
    ) -> Result<Response<Self::ReadStreamFromShardStream>, Status> {
        const CHUNK: usize = 200;

        let req = request.into_inner();
        let shard_id = req.shard_id;
        let shard = self.shard(shard_id).await?;
        let events = shard
            .storage
            .read_stream_events(&req.stream_id, req.from_version, req.max_count)
            .map_err(|e| Status::internal(format!("read_stream_events 失败: {e}")))?;

        let to_proto = move |e: es_core::Event| Event {
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
            shard_id,
        };

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tokio::spawn(async move {
            for chunk in events.chunks(CHUNK) {
                let resp = ReadEventsResponse {
                    events: chunk.iter().cloned().map(to_proto).collect(),
                    next_positions: Vec::new(),
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return; // 客户端断开
                }
            }
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

#[tonic::async_trait]
impl OwnershipInternal for MigrationService {
    /// 把归属命令提交到控制 Shard，并发布 `routes.json` 兼容投影。
    async fn commit_ownership(
        &self,
        request: Request<CommitOwnershipRequest>,
    ) -> Result<Response<CommitOwnershipResponse>, Status> {
        let request = request.into_inner();
        let (command, _): (es_core::OwnershipCommand, usize) =
            bincode::serde::decode_from_slice(&request.payload, bincode::config::standard())
                .map_err(|error| Status::invalid_argument(format!("归属命令解码失败: {error}")))?;
        let shard = self.shard(request.control_shard_id).await?;
        let applied = match self
            .client_write(&shard, es_storage::EsRequest::CommitOwnership { command })
            .await?
        {
            es_storage::EsResponse::OwnershipApplied(applied) => applied,
            other => {
                return Err(Status::internal(format!(
                    "控制 Shard 返回意外结果: {other:?}"
                )));
            }
        };
        self.route_table
            .publish_authoritative(applied.table.clone())
            .await
            .map_err(|error| Status::internal(format!("发布归属投影失败: {error}")))?;
        let payload = bincode::serde::encode_to_vec(&applied, bincode::config::standard())
            .map_err(|error| Status::internal(format!("归属结果编码失败: {error}")))?;
        Ok(Response::new(CommitOwnershipResponse { payload }))
    }

    /// 在数据 Shard 安装归属 fencing；只能在该 Shard leader 上提交。
    async fn install_ownership_fence(
        &self,
        request: Request<InstallOwnershipFenceRequest>,
    ) -> Result<Response<InstallOwnershipFenceResponse>, Status> {
        let request = request.into_inner();
        if request.stream_id.is_empty() || request.generation == 0 {
            return Err(Status::invalid_argument(
                "stream_id 不能为空且 generation 必须大于 0",
            ));
        }
        let shard = self.shard(request.shard_id).await?;
        match self
            .client_write(
                &shard,
                es_storage::EsRequest::InstallOwnershipFence {
                    stream_id: request.stream_id,
                    generation: request.generation,
                },
            )
            .await?
        {
            es_storage::EsResponse::OwnershipFenceInstalled { generation } => {
                Ok(Response::new(InstallOwnershipFenceResponse { generation }))
            }
            other => Err(Status::internal(format!(
                "数据 Shard 返回意外 fencing 结果: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_request_preserves_caller_observation_and_operation_id() {
        let operation_id = uuid::Uuid::new_v4();
        let change = move_change(SetStreamShardRequest {
            stream_id: "orders/concurrent".into(),
            shard_id: 9,
            expected_shard_id: 4,
            expected_generation: 7,
            operation_id: operation_id.as_bytes().to_vec(),
        })
        .expect("解析条件迁移");
        match change {
            OwnershipChange::Move {
                stream,
                expected,
                target_shard,
                operation_id: parsed_operation_id,
            } => {
                assert_eq!(stream, "orders/concurrent");
                assert_eq!(expected.shard_id, 4);
                assert_eq!(expected.generation, 7);
                assert_eq!(target_shard, 9);
                assert_eq!(parsed_operation_id, operation_id);
            }
            other => panic!("应生成 Move，实际 {other:?}"),
        }
    }

    #[test]
    fn move_request_rejects_missing_cas_token() {
        let status = move_change(SetStreamShardRequest {
            stream_id: "orders/missing-token".into(),
            shard_id: 1,
            expected_shard_id: 0,
            expected_generation: 1,
            operation_id: Vec::new(),
        })
        .expect_err("缺失 operation ID 必须拒绝");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn generation_zero_preserves_orphan_source_for_adoption() {
        let change = move_change(SetStreamShardRequest {
            stream_id: "orders/orphan".into(),
            shard_id: 8,
            expected_shard_id: 3,
            expected_generation: 0,
            operation_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        })
        .expect("解析孤儿收养");
        let OwnershipChange::AdoptOrphan {
            stream,
            source_shard,
            target_shard,
        } = change
        else {
            panic!("generation=0 必须生成孤儿收养变更");
        };
        assert_eq!(stream, "orders/orphan");
        assert_eq!(source_shard, 3);
        assert_eq!(target_shard, 8);
    }
}
