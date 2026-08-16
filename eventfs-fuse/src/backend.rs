//! FUSE 语义到 AggregateStore 客户端的后端边界。

use async_trait::async_trait;
use es_client::{AggregateStoreClient, ClientError, TlsClientConfig};
use es_proto::eventstore::*;
use tokio_stream::StreamExt;
use tonic::Code;
use uuid::Uuid;

use crate::codec::{
    self, AggregateVersionExpectation, EventEnvelope, Settlement, SettlementAction,
};

/// 聚合类型身份。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AggregateType {
    /// 业务空间。
    pub business_space: String,
    /// 聚合根类型。
    pub aggregate_type: String,
}

impl AggregateType {
    /// 构造并校验聚合类型身份。
    ///
    /// # 参数
    /// `business_space` 与 `aggregate_type` 是路径和 RPC 共用的身份字段。
    ///
    /// # 返回
    /// 返回已校验、可转换为 protobuf 的聚合类型身份。
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
        es_core::AggregateTypeId::new(&value.business_space, &value.aggregate_type)
            .map_err(|error| BackendError::InvalidArgument(error.to_string()))?;
        Ok(value)
    }

    fn proto(&self) -> AggregateTypeRef {
        AggregateTypeRef {
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
    /// 协商协议能力。
    ///
    /// # 返回
    /// 返回 payload 上限；服务端必须支持 256 分区、状态 CAS 和显式结算。
    ///
    /// # 错误
    /// 连接失败或能力不满足 FUSE 契约时返回 [`BackendError`]。
    async fn capabilities(&self) -> BackendResult<Capabilities>;
    /// 枚举已激活聚合类型，按服务端 catalog 顺序返回。
    ///
    /// # 错误
    /// catalog 不可用或响应身份非法时返回 [`BackendError`]。
    async fn list_aggregate_types(&self) -> BackendResult<Vec<AggregateType>>;
    /// 幂等注册并激活聚合类型。
    ///
    /// # 参数
    /// `operation_id` 在结果未知的重试中必须保持不变。
    ///
    /// # 错误
    /// 身份非法、操作冲突或 catalog 不可用时返回 [`BackendError`]。
    async fn register_aggregate_type(
        &self,
        aggregate_type: &AggregateType,
        operation_id: Uuid,
    ) -> BackendResult<()>;
    /// 追加单条事件，服务端执行实例级 OCC。
    ///
    /// # 返回
    /// 返回新分配的实例内 aggregate version。
    ///
    /// # 错误
    /// OCC、幂等、payload 或可用性失败时返回 [`BackendError`]。
    async fn append(
        &self,
        aggregate_type: &AggregateType,
        event: &EventEnvelope,
    ) -> BackendResult<u64>;
    /// 从 Beginning 跟随类型级事件，保持各实例内版本顺序。
    ///
    /// # 返回
    /// 返回承载 event/caught-up/degraded/recovered JSONL frame 的有界接收端。
    ///
    /// # 错误
    /// 首次建流失败时返回 [`BackendError`]；建流后错误作为接收端元素返回。
    async fn follow(
        &self,
        aggregate_type: &AggregateType,
    ) -> BackendResult<tokio::sync::mpsc::Receiver<BackendResult<Vec<u8>>>>;
    /// 分页枚举状态身份。
    ///
    /// # 参数
    /// `page_token` 只能原样续传；`page_size` 是本页最大条目数。
    ///
    /// # 返回
    /// 返回稳定排序的实例 ID 和下一页 opaque token。
    ///
    /// # 错误
    /// token 非法或任一数据分区不可用时返回 [`BackendError`]。
    async fn list_states_page(
        &self,
        aggregate_type: &AggregateType,
        page_token: Vec<u8>,
        page_size: u32,
    ) -> BackendResult<StatePage>;
    /// 枚举全部状态身份；只供管理和测试使用，FUSE `readdir` 必须使用分页接口。
    ///
    /// # 返回
    /// 返回服务端全部状态实例 ID。
    ///
    /// # 错误
    /// 任一分页请求失败时返回 [`BackendError`]。
    async fn list_states(&self, aggregate_type: &AggregateType) -> BackendResult<Vec<String>> {
        let mut token = Vec::new();
        let mut states = Vec::new();
        loop {
            let page = self.list_states_page(aggregate_type, token, 1_000).await?;
            states.extend(page.aggregate_ids);
            if page.next_page_token.is_empty() {
                return Ok(states);
            }
            token = page.next_page_token;
        }
    }
    /// 读取一个聚合实例状态。
    ///
    /// # 返回
    /// 状态存在时返回正文、revision 和修改时间，不存在时返回 `None`。
    ///
    /// # 错误
    /// 参数非法或数据分区不可用时返回 [`BackendError`]。
    async fn get_state(
        &self,
        aggregate_type: &AggregateType,
        aggregate_id: &str,
    ) -> BackendResult<Option<StateDocument>>;
    /// 使用打开时 revision CAS 覆盖状态。
    ///
    /// # 参数
    /// `revision=None` 表示状态必须不存在，`Some(n)` 表示精确匹配。
    ///
    /// # 返回
    /// 返回提交后的状态 revision、正文和修改时间。
    ///
    /// # 错误
    /// CAS 冲突、正文超限或数据分区不可用时返回 [`BackendError`]。
    async fn put_state(
        &self,
        aggregate_type: &AggregateType,
        aggregate_id: &str,
        revision: Option<u64>,
        data: Vec<u8>,
    ) -> BackendResult<StateDocument>;
    /// 枚举聚合类型下的消费者组名称。
    ///
    /// # 错误
    /// catalog 不可用时返回 [`BackendError`]。
    async fn list_groups(&self, aggregate_type: &AggregateType) -> BackendResult<Vec<String>>;
    /// 从 Beginning 幂等创建消费者组。
    ///
    /// # 参数
    /// `operation_id` 在结果未知的重试中必须保持不变。
    ///
    /// # 错误
    /// 组已存在、标识符非法或 catalog 不可用时返回 [`BackendError`]。
    async fn create_group(
        &self,
        aggregate_type: &AggregateType,
        group_name: &str,
        operation_id: Uuid,
    ) -> BackendResult<()>;
    /// 为指定消费成员长轮询一批 delivery。
    ///
    /// # 返回
    /// 返回带 opaque token 和租约的投递，顺序只保证到分区级。
    ///
    /// # 错误
    /// 组不存在或数据分区不可用时返回 [`BackendError`]。
    async fn fetch_group(
        &self,
        aggregate_type: &AggregateType,
        group_name: &str,
        consumer_id: &str,
    ) -> BackendResult<FetchAggregateGroupResponse>;
    /// 显式结算一批 delivery。
    ///
    /// # 返回
    /// 返回与输入顺序一致的逐项结算结果。
    ///
    /// # 错误
    /// token 非法、组不存在或数据分区不可用时返回 [`BackendError`]。
    async fn settle_group(
        &self,
        aggregate_type: &AggregateType,
        group_name: &str,
        consumer_id: &str,
        settlements: &[Settlement],
    ) -> BackendResult<SettleAggregateGroupResponse>;
    /// 续租仍由当前读句柄持有的 delivery。
    ///
    /// # 返回
    /// 返回与输入顺序一致的新 deadline 或逐项拒绝结果。
    ///
    /// # 错误
    /// token 非法、组不存在或数据分区不可用时返回 [`BackendError`]。
    async fn renew_group(
        &self,
        aggregate_type: &AggregateType,
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

    async fn list_aggregate_types(&self) -> BackendResult<Vec<AggregateType>> {
        let infos = self.client.clone().list_aggregate_types().await?;
        infos
            .into_iter()
            .filter(|info| info.status == AggregateTypeStatus::AggregateTypeActive as i32)
            .map(|info| {
                let identity = info
                    .aggregate_type
                    .ok_or_else(|| BackendError::Internal("聚合类型缺少身份".into()))?;
                AggregateType::new(identity.business_space, identity.aggregate_type)
            })
            .collect()
    }

    async fn register_aggregate_type(
        &self,
        aggregate_type: &AggregateType,
        operation_id: Uuid,
    ) -> BackendResult<()> {
        self.client
            .clone()
            .register_aggregate_type(RegisterAggregateTypeRequest {
                aggregate_type: Some(aggregate_type.proto()),
                operation_id: operation_id.as_bytes().to_vec(),
            })
            .await?;
        Ok(())
    }

    async fn append(
        &self,
        aggregate_type: &AggregateType,
        event: &EventEnvelope,
    ) -> BackendResult<u64> {
        let kind = match event.expected_version {
            AggregateVersionExpectation::Any => expected_aggregate_version::Kind::Any(Empty {}),
            AggregateVersionExpectation::NoAggregate => {
                expected_aggregate_version::Kind::NoAggregate(Empty {})
            }
            AggregateVersionExpectation::Exists => {
                expected_aggregate_version::Kind::AggregateExists(Empty {})
            }
            AggregateVersionExpectation::Exact(version) => {
                expected_aggregate_version::Kind::Exact(version)
            }
        };
        let response = self
            .client
            .clone()
            .append(AppendAggregateEventRequest {
                aggregate_type: Some(aggregate_type.proto()),
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
        aggregate_type: &AggregateType,
    ) -> BackendResult<tokio::sync::mpsc::Receiver<BackendResult<Vec<u8>>>> {
        let mut stream = self
            .client
            .clone()
            .follow(FollowAggregateTypeEventsRequest {
                aggregate_type: Some(aggregate_type.proto()),
                start: Some(AggregateFollowStart {
                    kind: Some(aggregate_follow_start::Kind::Beginning(Empty {})),
                }),
            })
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(frame) = stream.next().await {
                let encoded = match frame {
                    Err(error) => Err(BackendError::from(error)),
                    Ok(frame) => match frame.payload {
                        Some(follow_aggregate_type_events_response::Payload::Event(event)) => {
                            codec::event_frame(&event).map_err(BackendError::from)
                        }
                        Some(follow_aggregate_type_events_response::Payload::CaughtUp(_)) => {
                            Ok(codec::status_frame("caught_up", None))
                        }
                        Some(follow_aggregate_type_events_response::Payload::Degraded(value)) => {
                            Ok(codec::status_frame(
                                "degraded",
                                Some(value.unavailable_source_count),
                            ))
                        }
                        Some(follow_aggregate_type_events_response::Payload::Recovered(_)) => {
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
        aggregate_type: &AggregateType,
        page_token: Vec<u8>,
        page_size: u32,
    ) -> BackendResult<StatePage> {
        let page = self
            .client
            .clone()
            .list_states(ListAggregateStatesRequest {
                aggregate_type: Some(aggregate_type.proto()),
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
        aggregate_type: &AggregateType,
        aggregate_id: &str,
    ) -> BackendResult<Option<StateDocument>> {
        match self
            .client
            .clone()
            .get_state(GetAggregateStateRequest {
                aggregate_type: Some(aggregate_type.proto()),
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
        aggregate_type: &AggregateType,
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
                aggregate_type: Some(aggregate_type.proto()),
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

    async fn list_groups(&self, aggregate_type: &AggregateType) -> BackendResult<Vec<String>> {
        Ok(self
            .client
            .clone()
            .list_groups(aggregate_type.proto())
            .await?
            .into_iter()
            .map(|group| group.name)
            .collect())
    }

    async fn create_group(
        &self,
        aggregate_type: &AggregateType,
        group_name: &str,
        operation_id: Uuid,
    ) -> BackendResult<()> {
        self.client
            .clone()
            .create_group(CreateAggregateGroupRequest {
                aggregate_type: Some(aggregate_type.proto()),
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
        aggregate_type: &AggregateType,
        group_name: &str,
        consumer_id: &str,
    ) -> BackendResult<FetchAggregateGroupResponse> {
        self.client
            .clone()
            .fetch_group(FetchAggregateGroupRequest {
                aggregate_type: Some(aggregate_type.proto()),
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
        aggregate_type: &AggregateType,
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
                aggregate_type: Some(aggregate_type.proto()),
                name: group_name.into(),
                consumer_id: consumer_id.into(),
                settlements,
            })
            .await
            .map_err(Into::into)
    }

    async fn renew_group(
        &self,
        aggregate_type: &AggregateType,
        group_name: &str,
        consumer_id: &str,
        delivery_ids: Vec<Vec<u8>>,
    ) -> BackendResult<RenewAggregateGroupResponse> {
        self.client
            .clone()
            .renew_group(RenewAggregateGroupRequest {
                aggregate_type: Some(aggregate_type.proto()),
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
