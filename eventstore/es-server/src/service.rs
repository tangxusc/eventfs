//! gRPC 服务实现。

use std::sync::Arc;
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;

use es_proto::eventstore::event_store_server::EventStore;
use es_proto::eventstore::*;
use es_raft::ShardManager;

/// 归并用的堆元素：按 (hlc, shard_id, position) 定序
#[derive(PartialEq, Eq)]
struct MergeHead {
    hlc: (u64, u32),
    shard_id: u64,
    position: u64,
    /// 该元素来自第几路输入
    stream_idx: usize,
}

impl Ord for MergeHead {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.hlc
            .cmp(&other.hlc)
            .then(self.shard_id.cmp(&other.shard_id))
            .then(self.position.cmp(&other.position))
    }
}

impl PartialOrd for MergeHead {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// 把多个已排序的分片流归并为一路，按 HLC 定序。
///
/// 用 k 路归并而非「合并后整体排序」：整体排序会让同一分片内的
/// 事件顺序完全由 HLC 决定，而 HLC 由各 leader 的墙上时钟推进，
/// 时钟回拨时同分片内的事件顺序就会被打乱——这违反了分片内
/// 严格按提交序（position）的保证。k 路归并只在「各路队首」之间比较，
/// 每路内部顺序原样保留，因此分片内的 position 序恒成立。
///
/// - `streams`: 各分片的事件流(已按 position 排序,升序或降序由调用方保证)
/// - `limit`: 最多返回多少条,0 表示不限量
/// - `descending`: true 表示降序归并(反向读),false 表示升序归并(正向读)
///
/// 返回 `(合并事件, 每路最后消费的 position)`：
/// 归并可能因 `limit` 全局截断而把某些路已读到的缓冲尾部丢弃，
/// 游标必须按「消费水位」推进（未消费的路为 None，下一页重读），
/// 否则被丢弃的数据会永久丢失。
fn merge_by_hlc(
    streams: Vec<Vec<Event>>,
    limit: u64,
    descending: bool,
) -> (Vec<Event>, Vec<Option<u64>>) {
    let mut consumed: Vec<Option<u64>> = vec![None; streams.len()];

    // 单路无需归并，直接返回，保持 position 序
    if streams.len() == 1 {
        let mut only = streams.into_iter().next().unwrap_or_default();
        if limit != 0 && only.len() as u64 > limit {
            only.truncate(limit as usize);
        }
        if let Some(last) = only.last() {
            consumed[0] = Some(last.position);
        }
        return (only, consumed);
    }

    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // 堆里只放游标而非事件本身：prost 生成的 Event 只有 PartialEq 没有 Ord，
    // 放进堆会导致类型不满足约束。事件留在 streams 里按下标取。
    let mut cursors = vec![0usize; streams.len()];

    let head_of = |idx: usize, e: &Event| MergeHead {
        hlc: e.hlc.as_ref().map(|h| (h.wall, h.logical)).unwrap_or((0, 0)),
        shard_id: e.shard_id,
        position: e.position,
        stream_idx: idx,
    };

    let mut out = Vec::new();

    if descending {
        // 降序归并:用最大堆(不包 Reverse)
        let mut heap: BinaryHeap<MergeHead> = BinaryHeap::new();
        for (idx, s) in streams.iter().enumerate() {
            if let Some(e) = s.first() {
                heap.push(head_of(idx, e));
            }
        }

        while let Some(head) = heap.pop() {
            let idx = head.stream_idx;
            let pos = cursors[idx];
            consumed[idx] = Some(streams[idx][pos].position);
            out.push(streams[idx][pos].clone());
            cursors[idx] += 1;

            if limit != 0 && out.len() as u64 >= limit {
                break;
            }
            if let Some(next) = streams[idx].get(cursors[idx]) {
                heap.push(head_of(idx, next));
            }
        }
    } else {
        // 升序归并:用最小堆(包 Reverse)
        let mut heap: BinaryHeap<Reverse<MergeHead>> = BinaryHeap::new();
        for (idx, s) in streams.iter().enumerate() {
            if let Some(e) = s.first() {
                heap.push(Reverse(head_of(idx, e)));
            }
        }

        while let Some(Reverse(head)) = heap.pop() {
            let idx = head.stream_idx;
            let pos = cursors[idx];
            consumed[idx] = Some(streams[idx][pos].position);
            out.push(streams[idx][pos].clone());
            cursors[idx] += 1;

            if limit != 0 && out.len() as u64 >= limit {
                break;
            }
            if let Some(next) = streams[idx].get(cursors[idx]) {
                heap.push(Reverse(head_of(idx, next)));
            }
        }
    }

    (out, consumed)
}

/// 把 Raft 写入错误映射为 gRPC 状态。
///
/// 重点是 `ForwardToLeader`：本节点不是 leader 时 openraft 会返回它，且带上
/// leader 的 node_id 与地址。必须把地址透出给客户端，否则客户端只能盲目重试
/// 其它节点。错误码用 `Unavailable`——它是 gRPC 的可重试语义，
/// 客户端拿到后应重定向到 message 中的地址。
fn client_write_to_status(
    e: openraft::error::RaftError<u64, openraft::error::ClientWriteError<u64, openraft::BasicNode>>,
) -> Status {
    use openraft::error::{ClientWriteError, RaftError};

    match e {
        RaftError::APIError(ClientWriteError::ForwardToLeader(fwd)) => {
            let addr = fwd
                .leader_node
                .as_ref()
                .map(|n| n.addr.clone())
                .unwrap_or_default();
            match fwd.leader_id {
                Some(id) => Status::unavailable(format!(
                    "not leader; leader_id={id} leader_addr={addr}"
                )),
                // 选举中，暂无 leader，客户端应稍后重试
                None => Status::unavailable("not leader; leader unknown, retry later"),
            }
        }
        RaftError::APIError(ClientWriteError::ChangeMembershipError(err)) => {
            Status::failed_precondition(format!("成员变更错误: {err}"))
        }
        RaftError::Fatal(f) => Status::internal(format!("Raft 致命错误: {f}")),
    }
}

/// proto ExpectedVersion 转领域模型
fn proto_to_expected_version(ev: ExpectedVersion) -> es_core::ExpectedVersion {
    match ev.kind {
        Some(expected_version::Kind::Any(_)) => es_core::ExpectedVersion::Any,
        Some(expected_version::Kind::NoStream(_)) => es_core::ExpectedVersion::NoStream,
        Some(expected_version::Kind::StreamExists(_)) => es_core::ExpectedVersion::StreamExists,
        Some(expected_version::Kind::Exact(v)) => es_core::ExpectedVersion::Exact(v),
        None => es_core::ExpectedVersion::Any, // 默认
    }
}

/// EventStore gRPC 服务
pub struct EsService {
    shard_manager: Arc<ShardManager>,
}

impl EsService {
    /// 创建服务实例
    pub fn new(shard_manager: Arc<ShardManager>) -> Self {
        Self { shard_manager }
    }
}

#[tonic::async_trait]
impl EventStore for EsService {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let req = request.into_inner();
        let stream_id = &req.stream_id;

        tracing::debug!("Append request for stream: {}", stream_id);

        // 1. 根据 stream_id 路由到对应分片
        let shard = self
            .shard_manager
            .route_shard(stream_id)
            .await
            .map_err(|e| Status::internal(format!("route shard failed: {}", e)))?;

        // 2. 转换 proto 请求为领域模型
        let expected_version = req
            .expected_version
            .ok_or_else(|| Status::invalid_argument("expected_version is required"))?;
        let expected = proto_to_expected_version(expected_version);

        let events: Vec<es_core::NewEvent> = req
            .events
            .into_iter()
            .map(|e| es_core::NewEvent {
                event_id: uuid::Uuid::from_slice(&e.event_id)
                    .unwrap_or_else(|_| uuid::Uuid::new_v4()),
                event_type: e.event_type,
                data: e.data,
                metadata: e.metadata,
            })
            .collect();

        // 3. 分配 HLC（leader 在提交前分配，保证所有副本一致）
        let hlc = es_core::Hlc::now();

        let es_request = es_storage::EsRequest::Append {
            stream_id: stream_id.clone(),
            expected_version: expected,
            events,
            hlc,
        };

        // 4. 通过 Raft 提交（client_write 返回 apply 后的响应）
        let resp = shard
            .raft
            .client_write(es_request)
            .await
            .map_err(client_write_to_status)?;

        // 5. 转换响应
        match resp.data {
            es_storage::EsResponse::AppendOk {
                next_expected_version,
                first_position,
                last_position,
            } => Ok(Response::new(AppendResponse {
                shard_id: shard.id(),
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
        }
    }

    type ReadStreamStream = ReceiverStream<Result<ReadEventsResponse, Status>>;

    async fn read_stream(
        &self,
        request: Request<ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        let req = request.into_inner();
        tracing::debug!("ReadStream request for stream: {}", req.stream_id);

        // 路由到分片
        let shard = self
            .shard_manager
            .route_shard(&req.stream_id)
            .await
            .map_err(|e| Status::internal(format!("route shard failed: {}", e)))?;

        // 读取事件（直接从存储层读，无需走 Raft —— 读本地副本即可）
        let desc = req.direction == Direction::Backward as i32;
        let events = if desc {
            shard
                .storage
                .read_stream_events_backward(&req.stream_id, req.from_version, req.max_count)
        } else {
            shard
                .storage
                .read_stream_events(&req.stream_id, req.from_version, req.max_count)
        }
        .map_err(|e| Status::internal(format!("read_stream_events 失败: {e}")))?;

        // 转换为 proto Event
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
                shard_id: shard.id(),
            })
            .collect();

        // 流式返回（简化实现：一次性发送全部）
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let resp = ReadEventsResponse {
                events: proto_events,
                next_positions: Vec::new(), // 单流读不填充，仅 ReadAll 使用
            };
            let _ = tx.send(Ok(resp)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type ReadAllStream = ReceiverStream<Result<ReadEventsResponse, Status>>;

    async fn read_all(
        &self,
        request: Request<ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        let req = request.into_inner();
        tracing::debug!("ReadAll request for shards: {:?}", req.shard_ids);

        // from_positions 非空时它自带分片列表，此时 shard_ids 允许为空
        if req.shard_ids.is_empty() && req.from_positions.is_empty() {
            return Err(Status::invalid_argument(
                "shard_ids 与 from_positions 不能同时为空",
            ));
        }

        let desc = req.direction == Direction::Backward as i32;

        // 确定要读的分片及各自起点：from_positions 非空时优先，用于翻页
        let cursors: Vec<ShardPosition> = if !req.from_positions.is_empty() {
            req.from_positions
        } else {
            req.shard_ids
                .iter()
                .map(|&s| ShardPosition {
                    shard_id: s,
                    from_position: req.from_position,
                    ended: false,
                })
                .collect()
        };

        // 逐分片读取。每个分片内部已按 position 排序(升序或降序)。
        // 各取 max_count 条即可保证归并出的首 max_count 条正确。
        let per_shard_limit = req.max_count;
        let mut streams: Vec<Vec<Event>> = Vec::with_capacity(cursors.len());
        for cursor in &cursors {
            let shard_id = cursor.shard_id;
            let shard = self
                .shard_manager
                .get_shard(shard_id)
                .await
                .map_err(|e| Status::not_found(format!("分片 {shard_id}: {e}")))?;

            // 反向读尽（ended=true）的分片不再有更早事件，直接给空流；
            // 不能仅靠 from==0 判断——「消费到 position 1 → 游标 0」时
            // position 0 仍未读，from=0 必须能读到它
            let events = if desc && cursor.ended {
                Vec::new()
            } else if desc {
                shard
                    .storage
                    .read_all_events_backward(cursor.from_position, per_shard_limit)
                    .map_err(|e| Status::internal(format!("read_all_events_backward 失败: {e}")))?
            } else {
                shard
                    .storage
                    .read_all_events(cursor.from_position, per_shard_limit)
                    .map_err(|e| Status::internal(format!("read_all_events 失败: {e}")))?
            };

            streams.push(
                events
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
                        shard_id,
                    })
                    .collect(),
            );
        }

        let (proto_events, consumed) = merge_by_hlc(streams, req.max_count, desc);

        // 每分片「下一页的续读起点」= 归并消费水位推进，而非读水位：
        // 读到但被全局截断丢弃的缓冲尾部必须留在游标之后（下一页重读），
        // 否则数据永久丢失。服务端驱动游标，客户端翻页原样透传。
        // 正序 = 最后消费 position + 1；倒序 = 最后消费 position - 1；
        // 倒序消费到 position 0 时置 ended=true（该分片已读尽，服务端对
        // 它返回空页且游标不变）——空页是正反两个方向的统一终止条件。
        // 未消费的路 = 起点不变（含 ended 状态）。
        let mut next_positions: Vec<ShardPosition> = Vec::with_capacity(cursors.len());
        for (cursor, consumed_pos) in cursors.iter().zip(consumed.iter()) {
            // 每分片恒得一条游标（正反向统一，含读尽标记），客户端翻页原样透传
            let next = match (desc, consumed_pos) {
                (false, Some(p)) => ShardPosition {
                    shard_id: cursor.shard_id,
                    from_position: p.saturating_add(1),
                    ended: false,
                },
                (true, Some(p)) => ShardPosition {
                    shard_id: cursor.shard_id,
                    from_position: p.saturating_sub(1),
                    ended: *p == 0,
                },
                (_, None) => ShardPosition {
                    shard_id: cursor.shard_id,
                    from_position: cursor.from_position,
                    ended: cursor.ended, // 未消费：起点不变，下一页重读
                },
            };
            next_positions.push(next);
        }

        // 流式返回
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let resp = ReadEventsResponse {
                events: proto_events,
                next_positions,
            };
            let _ = tx.send(Ok(resp)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SubscribeStream = ReceiverStream<Result<SubscribeResponse, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();
        tracing::debug!("Subscribe request: target={:?}", req.target);

        // 解析订阅目标
        let target = req
            .target
            .ok_or_else(|| Status::invalid_argument("target is required"))?;

        use subscribe_request::Target;
        let (stream_id, shard_id, from_position) = match target {
            Target::StreamId(sid) => {
                // 订阅单个流：路由到分片
                let shard = self
                    .shard_manager
                    .route_shard(&sid)
                    .await
                    .map_err(|e| Status::internal(format!("route failed: {}", e)))?;

                // 起始位置：from_start=true 从头，否则用 from_exclusive+1
                // （saturating_add 防 u64::MAX 溢出回绕）
                let from_version = if req.from_start {
                    0
                } else {
                    req.from_exclusive.saturating_add(1)
                };

                (Some(sid), shard.id(), from_version)
            }
            Target::All(_) => {
                // 订阅 $all：按请求指定分片订阅（proto SubscribeRequest.shard_id，
                // 默认 0）。一次订阅一个分片的 $all，多分片需各自发起订阅。
                let from_position = if req.from_start {
                    0
                } else {
                    req.from_exclusive.saturating_add(1)
                };
                (None, req.shard_id, from_position)
            }
        };

        let shard = self
            .shard_manager
            .get_shard(shard_id)
            .await
            .map_err(|e| Status::not_found(format!("shard {} not found: {}", shard_id, e)))?;

        // 创建响应流
        let (tx, rx) = tokio::sync::mpsc::channel(100);

        // 启动后台任务处理订阅
        let storage = shard.storage.clone();
        tokio::spawn(async move {
            // Phase 0: 先注册实时广播接收器——必须在读历史之前。
            // 若注册在「读历史 → 发 caught_up」之后,窗口内提交的事件
            // 既不在快照也不在广播里(broadcast 对无接收者的 send 直接丢弃),
            // 订阅者会在无感知的情况下永久丢失事件。
            let mut event_rx = storage.subscribe_events();

            // Phase 1: Catch-up（补齐历史）
            let historical = match stream_id {
                Some(ref sid) => {
                    // 订阅单流：读取该流从 from_position 开始的事件
                    // 简化：这里用 from_version，生产应转为 position
                    storage
                        .read_stream_events(sid, from_position, 0)
                        .unwrap_or_default()
                }
                None => {
                    // 订阅 $all：读取分片内所有事件
                    storage
                        .read_all_events(from_position, 0)
                        .unwrap_or_default()
                }
            };

            // 历史尾水位：Phase 2 消费实时事件时跳过注册与快照之间的
            // 重复窗口（注册先于快照，窗口内提交的事件缓冲与快照各有一份）。
            let last_version = historical.last().map(|e| e.version);
            let last_position = historical.last().map(|e| e.position);

            // 发送历史事件
            for ev in historical {
                let proto_event = Event {
                    stream_id: ev.stream_id.clone(),
                    version: ev.version,
                    event_id: ev.event_id.as_bytes().to_vec(),
                    event_type: ev.event_type.clone(),
                    data: ev.data.clone(),
                    metadata: ev.metadata.clone(),
                    hlc: Some(Hlc {
                        wall: ev.hlc.wall,
                        logical: ev.hlc.logical,
                    }),
                    position: ev.position,
                    shard_id,
                };

                if tx
                    .send(Ok(SubscribeResponse {
                        payload: Some(subscribe_response::Payload::Event(proto_event)),
                    }))
                    .await
                    .is_err()
                {
                    // 客户端断开
                    return;
                }
            }

            // 发送 caught_up 信号
            if tx
                .send(Ok(SubscribeResponse {
                    payload: Some(subscribe_response::Payload::CaughtUp(Empty {})),
                }))
                .await
                .is_err()
            {
                return;
            }

            // Phase 2: Live（实时推送）
            loop {
                match event_rx.recv().await {
                    Ok(ev) => {
                        // 跳过快照已含的事件（注册先于快照的重复窗口），
                        // 再按订阅目标过滤
                        if let Some(ref sid) = stream_id {
                            if ev.stream_id != *sid
                                || last_version.is_some_and(|v| ev.version <= v)
                            {
                                continue;
                            }
                        } else if last_position.is_some_and(|p| ev.position <= p) {
                            continue;
                        }

                        let proto_event = Event {
                            stream_id: ev.stream_id,
                            version: ev.version,
                            event_id: ev.event_id.as_bytes().to_vec(),
                            event_type: ev.event_type,
                            data: ev.data,
                            metadata: ev.metadata,
                            hlc: Some(Hlc {
                                wall: ev.hlc.wall,
                                logical: ev.hlc.logical,
                            }),
                            position: ev.position,
                            shard_id,
                        };

                        if tx
                            .send(Ok(SubscribeResponse {
                                payload: Some(subscribe_response::Payload::Event(proto_event)),
                            }))
                            .await
                            .is_err()
                        {
                            // 客户端断开
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // 订阅者跟不上，关闭订阅（客户端应重新订阅）
                        tracing::warn!("Subscriber lagged, closing subscription");
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        // 广播通道关闭（不应发生）
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_stream_meta(
        &self,
        request: Request<GetStreamMetaRequest>,
    ) -> Result<Response<GetStreamMetaResponse>, Status> {
        let req = request.into_inner();
        tracing::debug!("GetStreamMeta request for stream: {}", req.stream_id);

        let shard = self
            .shard_manager
            .route_shard(&req.stream_id)
            .await
            .map_err(|e| Status::internal(format!("route shard failed: {}", e)))?;

        let meta = shard
            .storage
            .read_stream_meta(&req.stream_id)
            .map_err(|e| Status::internal(format!("read_stream_meta failed: {}", e)))?;

        match meta {
            Some(m) => Ok(Response::new(GetStreamMetaResponse {
                shard_id: shard.id(),
                exists: true,
                current_version: m.current_version,
            })),
            None => Ok(Response::new(GetStreamMetaResponse {
                shard_id: shard.id(),
                exists: false,
                current_version: 0,
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造事件：hlc 相同（wall, logical）时按 shard_id 再按 position 定序
    fn ev(shard: u64, pos: u64, wall: u64) -> Event {
        Event {
            stream_id: format!("s{shard}"),
            version: pos,
            event_id: Vec::new(),
            event_type: "T".into(),
            data: Vec::new(),
            metadata: Vec::new(),
            hlc: Some(Hlc { wall, logical: 0 }),
            position: pos,
            shard_id: shard,
        }
    }

    /// 消费水位语义：全局截断丢弃的路不推进（None），否则下一页会丢数据
    #[test]
    fn merge_truncated_unconsumed_watermark_none() {
        // 分片 0 的 HLC 最早，limit=2 时两条全来自分片 0；分片 1 被丢弃
        let streams = vec![
            vec![ev(0, 1, 100), ev(0, 2, 200), ev(0, 3, 300)],
            vec![ev(1, 1, 1000)],
        ];
        let (events, consumed) = merge_by_hlc(streams, 2, false);
        assert_eq!(events.len(), 2);
        assert_eq!(consumed, vec![Some(2), None], "分片 1 未消费应为 None");
    }

    /// 正序：消费水位 = 最后消费的 position，续读从 +1 开始
    #[test]
    fn forward_merge_watermark_advances() {
        let streams = vec![
            vec![ev(0, 1, 100), ev(0, 2, 300)],
            vec![ev(1, 1, 200), ev(1, 2, 400)],
        ];
        // 顺序：100(0,1) → 200(1,1) → 300(0,2) → 400(1,2)
        let (events, consumed) = merge_by_hlc(streams, 0, false);
        assert_eq!(events.len(), 4);
        assert_eq!(consumed, vec![Some(2), Some(2)]);
    }

    /// 倒序：消费水位 = 最后消费（最小）position，续读从 -1 开始
    #[test]
    fn backward_merge_watermark_min_position() {
        let streams = vec![
            vec![ev(0, 9, 100), ev(0, 8, 300), ev(0, 7, 500)],
            vec![ev(1, 5, 200)],
        ];
        // 降序：500(0,7) → 300(0,8) → 200(1,5) → 100(0,9)
        let (_, consumed) = merge_by_hlc(streams, 0, true);
        assert_eq!(consumed, vec![Some(7), Some(5)], "倒序水位 = 各路最后输出的最小 position");
    }

    /// 单路：截断后水位 = 最后输出的 position
    #[test]
    fn single_stream_truncated_watermark() {
        let streams = vec![vec![ev(0, 1, 1), ev(0, 2, 1), ev(0, 3, 1)]];
        let (events, consumed) = merge_by_hlc(streams, 2, false);
        assert_eq!(events.len(), 2);
        assert_eq!(consumed, vec![Some(2)]);
    }
}
