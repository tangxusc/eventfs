//! FUSE 语义到 AggregateStore 客户端的后端边界。

use async_trait::async_trait;
use es_client::{AggregateStoreClient, ClientError, TlsClientConfig};
use es_proto::eventstore::*;
use tokio_stream::StreamExt;
use tonic::Code;
use uuid::Uuid;

use crate::codec::{self, EventEnvelope, ExpectedVersion, Settlement, SettlementAction};

/// 聚合事件集身份。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventSet {
    /// 业务空间。
    pub business_space: String,
    /// 聚合根类型。
    pub aggregate_type: String,
}

impl EventSet {
    /// 构造并校验事件集身份。
    ///
    /// # 错误
    /// 任一标识符不符合公共路径规则时返回 [`BackendError::InvalidArgument`]。
    pub fn new(
        business_space: impl Into<String>,
        aggregate_type: impl Into<String>,
    ) -> BackendResult<Self> {
        let value = Self {
            business_space: business_space.into(),
            aggregate_type: aggregate_type.into(),
        };
        es_core::EventSetId::new(&value.business_space, &value.aggregate_type)
            .map_err(|error| BackendError::InvalidArgument(error.to_string()))?;
        Ok(value)
    }

    fn proto(&self) -> AggregateEventSetRef {
        AggregateEventSetRef {
            business_space: self.business_space.clone(),
            aggregate_type: self.aggregate_type.clone(),
        }
    }
}

/// 服务端协商后的 FUSE 限制。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// 单个事件 envelope 的最大字节数。
    pub max_event_bytes: usize,
    /// 单个状态文档的最大字节数。
    pub max_state_bytes: usize,
}

/// 已提交状态文档。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateDocument {
    /// 服务端 CAS revision。
    pub revision: u64,
    /// 原始业务 JSON 字节。
    pub data: Vec<u8>,
    /// 服务端提交时间；零表示旧状态没有时间元数据。
    pub modified_unix_millis: u64,
}

/// 一页聚合状态身份及服务端 opaque 续页 token。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatePage {
    /// 本页按稳定顺序返回的聚合实例 ID。
    pub aggregate_ids: Vec<String>,
    /// 服务端 opaque 续页 token；空值表示枚举完成。
    pub next_page_token: Vec<u8>,
}

fn validate_capabilities(value: AggregateStoreCapabilities) -> BackendResult<Capabilities> {
    if value.api_version != "1.0"
        || value.partition_count != u32::from(es_core::EVENT_PARTITION_COUNT)
        || !value.state_revision_cas
        || !value.explicit_group_settlement
        || !value.state_modified_time
    {
        return Err(BackendError::Unsupported(format!(
            "服务端能力不满足 eventfs-fuse: version={} partitions={} state_cas={} explicit_settlement={} state_modified_time={}",
            value.api_version,
            value.partition_count,
            value.state_revision_cas,
            value.explicit_group_settlement,
            value.state_modified_time,
        )));
    }
    Ok(Capabilities {
        max_event_bytes: usize::try_from(value.max_event_bytes).unwrap_or(usize::MAX),
        max_state_bytes: usize::try_from(value.max_state_bytes).unwrap_or(usize::MAX),
    })
}

/// 后端返回类型。
pub type BackendResult<T> = Result<T, BackendError>;

/// FUSE 所需的最小 AggregateStore 接口。
#[async_trait]
pub trait EventFsBackend: Send + Sync + 'static {
    /// 协商协议能力；不满足 FUSE 必需语义时返回错误。
    async fn capabilities(&self) -> BackendResult<Capabilities>;
    /// 枚举已激活事件集。
    async fn list_event_sets(&self) -> BackendResult<Vec<EventSet>>;
    /// 幂等创建并激活事件集。
    async fn create_event_set(&self, event_set: &EventSet, operation_id: Uuid)
    -> BackendResult<()>;
    /// 追加单条事件，服务端执行实例级 OCC。
    async fn append(&self, event_set: &EventSet, event: &EventEnvelope) -> BackendResult<u64>;
    /// 从 Beginning 跟随事件，返回已编码 JSONL frame。
    async fn follow(
        &self,
        event_set: &EventSet,
    ) -> BackendResult<tokio::sync::mpsc::Receiver<BackendResult<Vec<u8>>>>;
    /// 分页枚举状态身份；`page_token` 只能原样传回服务端。
    async fn list_states_page(
        &self,
        event_set: &EventSet,
        page_token: Vec<u8>,
        page_size: u32,
    ) -> BackendResult<StatePage>;
    /// 枚举全部状态身份；只供管理和测试使用，FUSE `readdir` 必须使用分页接口。
    async fn list_states(&self, event_set: &EventSet) -> BackendResult<Vec<String>> {
        let mut token = Vec::new();
        let mut states = Vec::new();
        loop {
            let page = self.list_states_page(event_set, token, 1_000).await?;
            states.extend(page.aggregate_ids);
            if page.next_page_token.is_empty() {
                return Ok(states);
            }
            token = page.next_page_token;
        }
    }
    /// 读取状态；不存在时返回 `None`。
    async fn get_state(
        &self,
        event_set: &EventSet,
        aggregate_id: &str,
    ) -> BackendResult<Option<StateDocument>>;
    /// 使用打开时 revision CAS 覆盖状态。
    async fn put_state(
        &self,
        event_set: &EventSet,
        aggregate_id: &str,
        revision: Option<u64>,
        data: Vec<u8>,
    ) -> BackendResult<StateDocument>;
    /// 枚举消费者组名称。
    async fn list_groups(&self, event_set: &EventSet) -> BackendResult<Vec<String>>;
    /// 从 Beginning 幂等创建消费者组。
    async fn create_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        operation_id: Uuid,
    ) -> BackendResult<()>;
    /// 长轮询一批 delivery。
    async fn fetch_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        consumer_id: &str,
    ) -> BackendResult<FetchAggregateGroupResponse>;
    /// 显式结算一批 delivery。
    async fn settle_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        consumer_id: &str,
        settlements: &[Settlement],
    ) -> BackendResult<SettleAggregateGroupResponse>;
    /// 续租仍由当前读句柄持有的 delivery。
    async fn renew_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        consumer_id: &str,
        delivery_ids: Vec<Vec<u8>>,
    ) -> BackendResult<RenewAggregateGroupResponse>;
}

/// 使用官方客户端的生产后端。
pub struct GrpcBackend {
    client: AggregateStoreClient,
}

impl GrpcBackend {
    /// 连接服务端候选节点。
    ///
    /// # 参数
    /// `endpoints` 是 HTTP/HTTPS 地址；`tls` 是 HTTPS 信任策略。
    ///
    /// # 返回
    /// 返回可在线程间共享的后端。
    ///
    /// # 错误
    /// 地址非法或首节点不可达时返回映射后的 [`BackendError`]。
    pub async fn connect(
        endpoints: Vec<String>,
        tls: Option<TlsClientConfig>,
    ) -> BackendResult<Self> {
        let client = AggregateStoreClient::connect_with_tls(endpoints, tls)
            .await
            .map_err(BackendError::from)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl EventFsBackend for GrpcBackend {
    async fn capabilities(&self) -> BackendResult<Capabilities> {
        let value = self.client.clone().capabilities().await?;
        validate_capabilities(value)
    }

    async fn list_event_sets(&self) -> BackendResult<Vec<EventSet>> {
        let infos = self.client.clone().list_event_sets().await?;
        infos
            .into_iter()
            .filter(|info| info.status == AggregateEventSetStatus::AggregateEventSetActive as i32)
            .map(|info| {
                let identity = info
                    .event_set
                    .ok_or_else(|| BackendError::Internal("事件集缺少身份".into()))?;
                EventSet::new(identity.business_space, identity.aggregate_type)
            })
            .collect()
    }

    async fn create_event_set(
        &self,
        event_set: &EventSet,
        operation_id: Uuid,
    ) -> BackendResult<()> {
        self.client
            .clone()
            .create_event_set(CreateEventSetRequest {
                event_set: Some(event_set.proto()),
                operation_id: operation_id.as_bytes().to_vec(),
            })
            .await?;
        Ok(())
    }

    async fn append(&self, event_set: &EventSet, event: &EventEnvelope) -> BackendResult<u64> {
        let kind = match event.expected_version {
            ExpectedVersion::Any => expected_aggregate_version::Kind::Any(Empty {}),
            ExpectedVersion::NoAggregate => expected_aggregate_version::Kind::NoAggregate(Empty {}),
            ExpectedVersion::Exists => expected_aggregate_version::Kind::AggregateExists(Empty {}),
            ExpectedVersion::Exact(version) => expected_aggregate_version::Kind::Exact(version),
        };
        let response = self
            .client
            .clone()
            .append(AppendAggregateEventRequest {
                event_set: Some(event_set.proto()),
                aggregate_id: event.aggregate_id.clone(),
                expected_version: Some(ExpectedAggregateVersion { kind: Some(kind) }),
                event: Some(NewAggregateEvent {
                    event_id: event.event_id.as_bytes().to_vec(),
                    event_type: event.event_type.clone(),
                    data: event.data.clone(),
                    metadata: event.metadata.clone(),
                }),
            })
            .await?;
        Ok(response.aggregate_version)
    }

    async fn follow(
        &self,
        event_set: &EventSet,
    ) -> BackendResult<tokio::sync::mpsc::Receiver<BackendResult<Vec<u8>>>> {
        let mut stream = self
            .client
            .clone()
            .follow(ReadAggregateEventsRequest {
                event_set: Some(event_set.proto()),
                start: Some(AggregateReadStart {
                    kind: Some(aggregate_read_start::Kind::Beginning(Empty {})),
                }),
            })
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                let encoded = match frame {
                    Err(error) => Err(BackendError::from(error)),
                    Ok(frame) => match frame.payload {
                        Some(read_aggregate_events_response::Payload::Event(event)) => {
                            codec::event_frame(&event).map_err(BackendError::from)
                        }
                        Some(read_aggregate_events_response::Payload::CaughtUp(_)) => {
                            Ok(codec::status_frame("caught_up", None))
                        }
                        Some(read_aggregate_events_response::Payload::Degraded(value)) => Ok(
                            codec::status_frame("degraded", Some(value.unavailable_source_count)),
                        ),
                        Some(read_aggregate_events_response::Payload::Recovered(_)) => {
                            Ok(codec::status_frame("recovered", None))
                        }
                        None => continue,
                    },
                };
                if tx.send(encoded).await.is_err() {
                    break;
                }
            }
        });
        Ok(rx)
    }

    async fn list_states_page(
        &self,
        event_set: &EventSet,
        page_token: Vec<u8>,
        page_size: u32,
    ) -> BackendResult<StatePage> {
        let page = self
            .client
            .clone()
            .list_states(ListAggregateStatesRequest {
                event_set: Some(event_set.proto()),
                page_size,
                page_token,
            })
            .await?;
        Ok(StatePage {
            aggregate_ids: page
                .states
                .into_iter()
                .map(|state| state.aggregate_id)
                .collect(),
            next_page_token: page.next_page_token,
        })
    }

    async fn get_state(
        &self,
        event_set: &EventSet,
        aggregate_id: &str,
    ) -> BackendResult<Option<StateDocument>> {
        match self
            .client
            .clone()
            .get_state(GetAggregateStateRequest {
                event_set: Some(event_set.proto()),
                aggregate_id: aggregate_id.into(),
            })
            .await
        {
            Ok(value) => Ok(Some(StateDocument {
                revision: value.revision,
                data: value.data,
                modified_unix_millis: value.modified_unix_millis,
            })),
            Err(ClientError::RpcFailed {
                code: Code::NotFound,
                ..
            }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    async fn put_state(
        &self,
        event_set: &EventSet,
        aggregate_id: &str,
        revision: Option<u64>,
        data: Vec<u8>,
    ) -> BackendResult<StateDocument> {
        let kind = revision
            .map(expected_state_revision::Kind::Exact)
            .unwrap_or_else(|| expected_state_revision::Kind::Absent(Empty {}));
        let stored_data = data.clone();
        let value = self
            .client
            .clone()
            .put_state(PutAggregateStateRequest {
                event_set: Some(event_set.proto()),
                aggregate_id: aggregate_id.into(),
                expected_revision: Some(ExpectedStateRevision { kind: Some(kind) }),
                data,
            })
            .await?;
        Ok(StateDocument {
            revision: value.revision,
            data: stored_data,
            modified_unix_millis: value.modified_unix_millis,
        })
    }

    async fn list_groups(&self, event_set: &EventSet) -> BackendResult<Vec<String>> {
        Ok(self
            .client
            .clone()
            .list_groups(event_set.proto())
            .await?
            .into_iter()
            .map(|group| group.name)
            .collect())
    }

    async fn create_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        operation_id: Uuid,
    ) -> BackendResult<()> {
        self.client
            .clone()
            .create_group(CreateAggregateGroupRequest {
                event_set: Some(event_set.proto()),
                name: group_name.into(),
                start: Some(AggregateGroupStart {
                    kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
                }),
                settings: None,
                operation_id: operation_id.as_bytes().to_vec(),
            })
            .await?;
        Ok(())
    }

    async fn fetch_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        consumer_id: &str,
    ) -> BackendResult<FetchAggregateGroupResponse> {
        self.client
            .clone()
            .fetch_group(FetchAggregateGroupRequest {
                event_set: Some(event_set.proto()),
                name: group_name.into(),
                consumer_id: consumer_id.into(),
                max_events: 128,
                max_bytes: 4 * 1024 * 1024,
                // 短轮询让关闭的 FUSE 句柄能及时取消成员任务。
                wait_ms: 1_000,
            })
            .await
            .map_err(Into::into)
    }

    async fn settle_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        consumer_id: &str,
        settlements: &[Settlement],
    ) -> BackendResult<SettleAggregateGroupResponse> {
        let settlements = settlements
            .iter()
            .map(|settlement| AggregateGroupSettlement {
                delivery_id: settlement.delivery_id.clone(),
                action: match settlement.action {
                    SettlementAction::Ack => {
                        AggregateGroupSettlementAction::AggregateGroupSettlementAck
                    }
                    SettlementAction::Retry => {
                        AggregateGroupSettlementAction::AggregateGroupSettlementRetry
                    }
                    SettlementAction::Park => {
                        AggregateGroupSettlementAction::AggregateGroupSettlementPark
                    }
                    SettlementAction::Skip => {
                        AggregateGroupSettlementAction::AggregateGroupSettlementSkip
                    }
                } as i32,
                reason: settlement.reason.clone(),
            })
            .collect();
        self.client
            .clone()
            .settle_group(SettleAggregateGroupRequest {
                event_set: Some(event_set.proto()),
                name: group_name.into(),
                consumer_id: consumer_id.into(),
                settlements,
            })
            .await
            .map_err(Into::into)
    }

    async fn renew_group(
        &self,
        event_set: &EventSet,
        group_name: &str,
        consumer_id: &str,
        delivery_ids: Vec<Vec<u8>>,
    ) -> BackendResult<RenewAggregateGroupResponse> {
        self.client
            .clone()
            .renew_group(RenewAggregateGroupRequest {
                event_set: Some(event_set.proto()),
                name: group_name.into(),
                consumer_id: consumer_id.into(),
                delivery_ids,
            })
            .await
            .map_err(Into::into)
    }
}

/// 后端稳定错误分类；Linux 层只依赖该分类映射 errno。
#[derive(Debug, thiserror::Error)]
pub enum BackendError {
    #[error("非法参数: {0}")]
    InvalidArgument(String),
    #[error("不存在: {0}")]
    NotFound(String),
    #[error("已存在: {0}")]
    AlreadyExists(String),
    #[error("并发冲突: {0}")]
    Conflict(String),
    #[error("payload 超限: {0}")]
    TooLarge(String),
    #[error("租约已失效: {0}")]
    Stale(String),
    #[error("没有权限: {0}")]
    PermissionDenied(String),
    #[error("请求超时: {0}")]
    Timeout(String),
    #[error("服务不可用: {0}")]
    Unavailable(String),
    #[error("资源忙: {0}")]
    Busy(String),
    #[error("能力不兼容: {0}")]
    Unsupported(String),
    #[error("内部错误: {0}")]
    Internal(String),
}

impl From<ClientError> for BackendError {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::RpcFailed { code, message } => match code {
                Code::InvalidArgument => Self::InvalidArgument(message),
                Code::NotFound => Self::NotFound(message),
                Code::AlreadyExists => Self::AlreadyExists(message),
                Code::Aborted => Self::Conflict(message),
                Code::FailedPrecondition if message.contains("payload") => Self::TooLarge(message),
                Code::FailedPrecondition if message.contains("stale") => Self::Stale(message),
                Code::FailedPrecondition => Self::Conflict(message),
                Code::ResourceExhausted | Code::OutOfRange => Self::TooLarge(message),
                Code::PermissionDenied | Code::Unauthenticated => Self::PermissionDenied(message),
                Code::DeadlineExceeded => Self::Timeout(message),
                Code::Unavailable => Self::Unavailable(message),
                _ => Self::Internal(message),
            },
            ClientError::PayloadTooLarge(message) => Self::TooLarge(message),
            ClientError::InvalidConfig(message) => Self::InvalidArgument(message),
            ClientError::ConnectionFailed(message) | ClientError::AllNodesFailed(message) => {
                Self::Unavailable(message)
            }
            ClientError::NotLeader(address) => Self::Unavailable(format!("leader={address:?}")),
        }
    }
}

impl From<codec::CodecError> for BackendError {
    fn from(error: codec::CodecError) -> Self {
        Self::Internal(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compatible_capabilities() -> AggregateStoreCapabilities {
        AggregateStoreCapabilities {
            api_version: "1.0".into(),
            partition_count: u32::from(es_core::EVENT_PARTITION_COUNT),
            max_event_bytes: 1_024,
            max_state_bytes: 2_048,
            state_revision_cas: true,
            explicit_group_settlement: true,
            state_modified_time: true,
        }
    }

    #[test]
    fn capabilities_require_every_fuse_protocol_guarantee() {
        assert_eq!(
            validate_capabilities(compatible_capabilities()).unwrap(),
            Capabilities {
                max_event_bytes: 1_024,
                max_state_bytes: 2_048,
            }
        );

        let mut incompatible = compatible_capabilities();
        incompatible.api_version = "2.0".into();
        assert!(matches!(
            validate_capabilities(incompatible),
            Err(BackendError::Unsupported(_))
        ));

        let mut incompatible = compatible_capabilities();
        incompatible.partition_count += 1;
        assert!(matches!(
            validate_capabilities(incompatible),
            Err(BackendError::Unsupported(_))
        ));

        let mut incompatible = compatible_capabilities();
        incompatible.state_revision_cas = false;
        assert!(matches!(
            validate_capabilities(incompatible),
            Err(BackendError::Unsupported(_))
        ));

        let mut incompatible = compatible_capabilities();
        incompatible.explicit_group_settlement = false;
        assert!(matches!(
            validate_capabilities(incompatible),
            Err(BackendError::Unsupported(_))
        ));

        let mut incompatible = compatible_capabilities();
        incompatible.state_modified_time = false;
        assert!(matches!(
            validate_capabilities(incompatible),
            Err(BackendError::Unsupported(_))
        ));
    }

    #[test]
    fn grpc_codes_map_to_stable_categories() {
        let error = |code, message: &str| ClientError::RpcFailed {
            code,
            message: message.into(),
        };
        assert!(matches!(
            BackendError::from(error(Code::InvalidArgument, "bad")),
            BackendError::InvalidArgument(_)
        ));
        assert!(matches!(
            BackendError::from(error(Code::Aborted, "occ")),
            BackendError::Conflict(_)
        ));
        assert!(matches!(
            BackendError::from(error(Code::FailedPrecondition, "payload too large")),
            BackendError::TooLarge(_)
        ));
        assert!(matches!(
            BackendError::from(error(Code::Unavailable, "down")),
            BackendError::Unavailable(_)
        ));
        for (code, message, expected) in [
            (Code::NotFound, "missing", "not_found"),
            (Code::AlreadyExists, "exists", "already_exists"),
            (Code::FailedPrecondition, "stale lease", "stale"),
            (Code::FailedPrecondition, "occ", "conflict"),
            (Code::ResourceExhausted, "large", "too_large"),
            (Code::OutOfRange, "large", "too_large"),
            (Code::PermissionDenied, "denied", "permission"),
            (Code::Unauthenticated, "denied", "permission"),
            (Code::DeadlineExceeded, "timeout", "timeout"),
            (Code::Unknown, "unknown", "internal"),
        ] {
            let mapped = BackendError::from(error(code, message));
            let actual = match mapped {
                BackendError::NotFound(_) => "not_found",
                BackendError::AlreadyExists(_) => "already_exists",
                BackendError::Stale(_) => "stale",
                BackendError::Conflict(_) => "conflict",
                BackendError::TooLarge(_) => "too_large",
                BackendError::PermissionDenied(_) => "permission",
                BackendError::Timeout(_) => "timeout",
                BackendError::Internal(_) => "internal",
                other => panic!("意外映射: {other}"),
            };
            assert_eq!(actual, expected);
        }

        for error in [
            ClientError::PayloadTooLarge("large".into()),
            ClientError::InvalidConfig("bad".into()),
            ClientError::ConnectionFailed("down".into()),
            ClientError::AllNodesFailed("down".into()),
            ClientError::NotLeader(None),
        ] {
            let _ = BackendError::from(error);
        }
        assert!(matches!(
            BackendError::from(codec::CodecError::InvalidToken),
            BackendError::Internal(_)
        ));
    }
}
