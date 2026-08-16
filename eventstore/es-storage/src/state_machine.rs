//! RaftStateMachine trait 实现。
//!
//! apply 是单个 Raft group 的串行执行点。聚合版本、分区位置、幂等索引与
//! `last_applied` 必须在同一个 surrealkv 事务提交，避免崩溃后出现版本回退。

use std::io::Write;

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, RaftTypeConfig, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use surrealkv::LSMIterator;

use super::EsStorage;
use crate::key;
use crate::raft_type::TypeConfig;
use crate::snapshot;
use crate::{EsRequest, EsResponse};
use es_core::{
    AggregateCatalog, AggregateEvent, AggregateGroupCatalog, AggregateGroupPartition,
    AggregateMeta, AggregateState, AggregateStateDocument, AggregateTypeId,
    ExpectedAggregateVersion, ExpectedStateRevision, Hlc, NewAggregateEvent,
};

fn sm_read_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::read_state_machine(&std::io::Error::other(e.to_string())).into()
}

fn sm_write_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageIOError::write_state_machine(&std::io::Error::other(e.to_string())).into()
}

fn decode_aggregate_event(bytes: &[u8]) -> es_core::Result<AggregateEvent> {
    crate::encode::decode(bytes)
        .map_err(|error| es_core::Error::Serde(format!("AggregateEvent 反序列化失败: {error}")))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct AggregateIdempotencyRecord {
    fingerprint: u128,
    result: es_core::AggregateAppendResult,
}

impl EsStorage {
    /// 扫描 key 区间 `[start, end)` 的所有值。
    ///
    /// `desc` 为 true 时从末尾倒扫（`seek_last` + `prev`）。
    /// `limit` 为 0 表示不限量。
    ///
    /// 独立成同步函数：surrealkv 迭代器不是 `Send`，不能跨 await 存活。
    pub(crate) fn scan_values(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
        desc: bool,
        limit: u64,
    ) -> es_core::Result<Vec<Vec<u8>>> {
        if start >= end {
            return Ok(Vec::new());
        }
        let txn = self
            .tree()
            .begin()
            .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| es_core::Error::Storage(format!("range 失败: {e}")))?;

        if desc {
            it.seek_last()
                .map_err(|e| es_core::Error::Storage(format!("seek_last 失败: {e}")))?;
        } else {
            it.seek_first()
                .map_err(|e| es_core::Error::Storage(format!("seek_first 失败: {e}")))?;
        }

        let mut out = Vec::new();
        while it.valid() {
            let v = it
                .value()
                .map_err(|e| es_core::Error::Storage(format!("value 失败: {e}")))?;
            out.push(v.to_vec());
            if limit != 0 && out.len() as u64 >= limit {
                break;
            }
            let moved = if desc {
                it.prev()
                    .map_err(|e| es_core::Error::Storage(format!("prev 失败: {e}")))?
            } else {
                it.next()
                    .map_err(|e| es_core::Error::Storage(format!("next 失败: {e}")))?
            };
            if !moved {
                break;
            }
        }
        Ok(out)
    }
}

/// 已应用状态的持久化形态
///
/// pub(crate)：离线 restore（snapshot.rs）需写回同格式的 applied 状态。
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct AppliedState {
    pub(crate) last_applied: Option<LogId<u64>>,
    pub(crate) membership: StoredMembership<u64, openraft::BasicNode>,
}

impl EsStorage {
    /// 从持久化状态恢复 last_applied 与 membership。
    ///
    /// openraft 在启动时调用 `applied_state`，必须返回真实落盘的值，
    /// 否则会从错误的位置重放日志。**必须在第一次调用 `apply` 前调用。**
    ///
    /// 顺带执行快照文件化的启动清理：删除旧版快照 key（树内单 key 格式已废弃，
    /// 快照是可重建缓存，不迁移）与 incoming 残留临时文件。
    pub async fn restore_applied_state(&self) -> es_core::Result<()> {
        let k = key::sm_applied_state(self.shard_id());
        if let Some(bytes) = self.get(&k)? {
            let st: AppliedState = crate::encode::decode(&bytes)
                .map_err(|e| es_core::Error::Serde(format!("已应用状态反序列化失败: {e}")))?;
            let mut cache = self.sm_cache().write().await;
            cache.last_applied = st.last_applied;
            cache.membership = st.membership;
        }
        self.cleanup_legacy_snapshot().await?;
        Ok(())
    }

    /// 启动清理：删除旧版 `snapshot_current` key + incoming 残留临时文件。
    ///
    /// 快照是状态机的缓存，旧格式数据可直接丢弃（openraft 会按需重建）；
    /// 残留临时文件是上次传输中断留下的，启动时无进行中的传输，安全删除。
    async fn cleanup_legacy_snapshot(&self) -> es_core::Result<()> {
        let k = key::snapshot_current(self.shard_id());
        if self.get(&k)?.is_some() {
            self.delete_batch(&[k]).await?;
            tracing::info!("已清理旧版快照 key（快照已改为独立文件格式）");
        }
        let n = self
            .snapshot_store()
            .cleanup_incoming()
            .map_err(|e| es_core::Error::Storage(format!("清理快照临时文件失败: {e}")))?;
        if n > 0 {
            tracing::info!("已清理 {n} 个残留快照临时文件");
        }
        Ok(())
    }

    /// 读取流当前元数据。流不存在时返回 None。
    pub fn read_aggregate_meta(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateMeta>> {
        let key =
            key::sm_aggregate_meta(self.shard_id(), aggregate_type, partition_id, aggregate_id);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateMeta 反序列化失败: {error}"))
                })
            })
            .transpose()
    }

    /// 按聚合版本读取单条聚合事件，供分区索引解析使用。
    pub(crate) fn read_aggregate_event(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        aggregate_id: &str,
        aggregate_version: u64,
    ) -> es_core::Result<Option<AggregateEvent>> {
        let key = key::sm_aggregate_event(
            self.shard_id(),
            aggregate_type,
            partition_id,
            aggregate_id,
            aggregate_version,
        );
        self.get(&key)?
            .map(|bytes| decode_aggregate_event(&bytes))
            .transpose()
    }

    /// 按服务端分配的分区位置读取一个虚拟事件分区。
    ///
    /// 聚合类型和分区由调用方从权威 catalog 获得；返回顺序仅在该分区内稳定。
    pub fn read_aggregate_partition_events(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        from_position: u64,
        limit: u64,
    ) -> es_core::Result<Vec<AggregateEvent>> {
        let shard = self.shard_id();
        let prefix = key::sm_aggregate_partition_index_prefix(shard, aggregate_type, partition_id);
        let start =
            key::sm_aggregate_partition_index(shard, aggregate_type, partition_id, from_position);
        let end = key::successor(&prefix)
            .ok_or_else(|| es_core::Error::Internal("聚合分区索引前缀无后继".into()))?;
        let pointers = self.scan_values(start, end, false, limit)?;
        let mut events = Vec::with_capacity(pointers.len());
        for pointer in pointers {
            let (aggregate_id, aggregate_version): (String, u64) = crate::encode::decode(&pointer)
                .map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区索引反序列化失败: {error}"))
                })?;
            let event = self
                .read_aggregate_event(
                    aggregate_type,
                    partition_id,
                    &aggregate_id,
                    aggregate_version,
                )?
                .ok_or_else(|| {
                    es_core::Error::Storage(format!(
                        "聚合分区索引指向缺失事件: {aggregate_type}/{partition_id}/{aggregate_id}/{aggregate_version}"
                    ))
                })?;
            events.push(event);
        }
        Ok(events)
    }

    /// 读取聚合实例的业务状态文档。
    pub fn read_aggregate_state(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateState>> {
        let key =
            key::sm_aggregate_state(self.shard_id(), aggregate_type, partition_id, aggregate_id);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateState 反序列化失败: {error}"))
                })
            })
            .transpose()
    }

    /// 在同一存储快照中读取状态内容与最后提交时间。
    ///
    /// 旧状态缺少独立时间 key 时返回 Unix epoch 对应的零 HLC。
    pub fn read_aggregate_state_document(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateStateDocument>> {
        let state_key =
            key::sm_aggregate_state(self.shard_id(), aggregate_type, partition_id, aggregate_id);
        let modified_key = key::sm_aggregate_state_modified(
            self.shard_id(),
            aggregate_type,
            partition_id,
            aggregate_id,
        );
        let txn = self
            .tree()
            .begin()
            .map_err(|error| es_core::Error::Storage(format!("begin 失败: {error}")))?;
        let Some(state_bytes) = txn
            .get(state_key)
            .map_err(|error| es_core::Error::Storage(format!("读取聚合状态失败: {error}")))?
        else {
            return Ok(None);
        };
        let state: AggregateState = crate::encode::decode(&state_bytes).map_err(|error| {
            es_core::Error::Serde(format!("AggregateState 反序列化失败: {error}"))
        })?;
        let modified_hlc = txn
            .get(modified_key)
            .map_err(|error| es_core::Error::Storage(format!("读取状态修改时间失败: {error}")))?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("状态修改时间反序列化失败: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(Hlc {
                wall: 0,
                logical: 0,
            });
        Ok(Some(AggregateStateDocument {
            revision: state.revision,
            data: state.data,
            modified_hlc,
        }))
    }

    /// 按聚合实例 ID 词典序分页读取单个虚拟分区的状态元数据。
    ///
    /// - `after_aggregate_id`：排他起点；`None` 从分区首项开始。
    /// - `limit`：最大返回数，零表示不限量。
    /// - 返回：`(aggregate_id, state)`，严格按实例 ID 升序。
    /// - 错误：存储扫描、key 解码或状态反序列化失败。
    pub fn list_aggregate_partition_states(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        after_aggregate_id: Option<&str>,
        limit: u64,
    ) -> es_core::Result<Vec<(String, AggregateStateDocument)>> {
        let prefix = key::sm_aggregate_state_prefix(self.shard_id(), aggregate_type, partition_id);
        let start = match after_aggregate_id {
            Some(aggregate_id) => key::upper_including(&key::sm_aggregate_state(
                self.shard_id(),
                aggregate_type,
                partition_id,
                aggregate_id,
            )),
            None => prefix.clone(),
        };
        let end = key::successor(&prefix)
            .ok_or_else(|| es_core::Error::Internal("聚合状态前缀无后继".into()))?;
        let txn = self
            .tree()
            .begin()
            .map_err(|error| es_core::Error::Storage(format!("begin 失败: {error}")))?;
        let mut iterator = txn
            .range(start, end)
            .map_err(|error| es_core::Error::Storage(format!("range 失败: {error}")))?;
        let mut encoded_states = Vec::new();
        iterator
            .seek_first()
            .map_err(|error| es_core::Error::Storage(format!("seek_first 失败: {error}")))?;
        while iterator.valid() && (limit == 0 || encoded_states.len() < limit as usize) {
            let key_bytes = iterator.key().user_key().to_vec();
            let value = iterator
                .value()
                .map_err(|error| es_core::Error::Storage(format!("value 失败: {error}")))?
                .to_vec();
            encoded_states.push((key_bytes, value));
            if !iterator
                .next()
                .map_err(|error| es_core::Error::Storage(format!("next 失败: {error}")))?
            {
                break;
            }
        }
        drop(iterator);

        encoded_states
            .into_iter()
            .map(|(key_bytes, value)| {
                let aggregate_id =
                    key::decode_aggregate_state_key(&key_bytes).ok_or_else(|| {
                        es_core::Error::Serde(format!("聚合状态 key 损坏: {key_bytes:?}"))
                    })?;
                let state: AggregateState = crate::encode::decode(&value).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateState 反序列化失败: {error}"))
                })?;
                let modified_key = key::sm_aggregate_state_modified(
                    self.shard_id(),
                    aggregate_type,
                    partition_id,
                    &aggregate_id,
                );
                let modified_hlc = txn
                    .get(modified_key)
                    .map_err(|error| {
                        es_core::Error::Storage(format!("读取状态修改时间失败: {error}"))
                    })?
                    .map(|bytes| {
                        crate::encode::decode(&bytes).map_err(|error| {
                            es_core::Error::Serde(format!("状态修改时间反序列化失败: {error}"))
                        })
                    })
                    .transpose()?
                    .unwrap_or(Hlc {
                        wall: 0,
                        logical: 0,
                    });
                Ok((
                    aggregate_id,
                    AggregateStateDocument {
                        revision: state.revision,
                        data: state.data,
                        modified_hlc,
                    },
                ))
            })
            .collect()
    }

    /// 读取虚拟事件分区的下一个可用位置，供 `Now` 游标捕获 head。
    pub fn read_aggregate_partition_head(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
    ) -> es_core::Result<u64> {
        let key = key::sm_aggregate_next_position(self.shard_id(), aggregate_type, partition_id);
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区 next_position 反序列化失败: {error}"))
                })
            })
            .transpose()
            .map(|position| position.unwrap_or(0))
    }

    /// 读取控制 Shard 上的聚合类型 catalog；尚未注册时返回空 catalog。
    pub fn read_aggregate_catalog(&self) -> es_core::Result<AggregateCatalog> {
        let key = key::sm_aggregate_catalog(self.shard_id());
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateCatalog 反序列化失败: {error}"))
                })
            })
            .transpose()
            .map(|catalog| catalog.unwrap_or_default())
    }

    /// 读取控制 Shard 上的聚合消费者组 catalog；尚未创建时返回空 catalog。
    pub fn read_aggregate_group_catalog(&self) -> es_core::Result<AggregateGroupCatalog> {
        let key = key::sm_aggregate_group_catalog(self.shard_id());
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateGroupCatalog 反序列化失败: {error}"))
                })
            })
            .transpose()
            .map(|catalog| catalog.unwrap_or_default())
    }

    /// 读取数据 Shard 上单个组分区的 checkpoint、lease 与重试状态。
    pub fn read_aggregate_group_partition(
        &self,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        group_name: &str,
    ) -> es_core::Result<Option<AggregateGroupPartition>> {
        let key = key::sm_aggregate_group_partition(
            self.shard_id(),
            aggregate_type,
            partition_id,
            group_name,
        );
        self.get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("AggregateGroupPartition 反序列化失败: {error}"))
                })
            })
            .transpose()
    }
}

/// apply 过程中在单个事务内累积的写入。
///
/// surrealkv 事务内的读不保证看到同事务未提交的写，因此批内聚合版本、状态和
/// 分区位置在内存中串接，最终一次提交。
#[derive(Debug)]
enum ApplyOp {
    Put(Vec<u8>, Vec<u8>),
}

struct ApplyBatch {
    /// 待执行的有序操作
    ops: Vec<ApplyOp>,
    /// 本批已读取或修改的聚合实例当前版本。
    aggregate_versions: std::collections::HashMap<(AggregateTypeId, u16, String), Option<u64>>,
    /// 本批各虚拟事件分区的下一个可用位置。
    aggregate_partition_positions: std::collections::HashMap<(AggregateTypeId, u16), u64>,
    /// 本批已读取或修改的业务状态文档。
    aggregate_states:
        std::collections::HashMap<(AggregateTypeId, u16, String), Option<AggregateState>>,
    /// 本批已读取或创建的聚合追加幂等记录。
    aggregate_idempotency:
        std::collections::HashMap<(AggregateTypeId, u16, uuid::Uuid), AggregateIdempotencyRecord>,
    /// 本批已读取或安装的虚拟事件分区 fence。
    aggregate_partition_fences: std::collections::HashMap<(AggregateTypeId, u16), u64>,
    /// 本批控制 Shard 的聚合类型 catalog。
    aggregate_catalog: Option<AggregateCatalog>,
    /// 本批控制 Shard 的聚合消费者组 catalog。
    aggregate_group_catalog: Option<AggregateGroupCatalog>,
    /// 本批已读取或修改的聚合消费者组分区状态。
    aggregate_group_partitions:
        std::collections::HashMap<(AggregateTypeId, u16, String), AggregateGroupPartition>,
    /// 本批新产生的聚合事件，事务提交后广播。
    new_aggregate_events: Vec<AggregateEvent>,
}

impl EsStorage {
    fn batch_aggregate_version(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<u64>> {
        let identity = (
            aggregate_type.clone(),
            partition_id,
            aggregate_id.to_string(),
        );
        if let Some(version) = batch.aggregate_versions.get(&identity) {
            return Ok(*version);
        }
        let version = self
            .read_aggregate_meta(aggregate_type, partition_id, aggregate_id)?
            .map(|meta| meta.current_version);
        batch.aggregate_versions.insert(identity, version);
        Ok(version)
    }

    fn batch_aggregate_state(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        aggregate_id: &str,
    ) -> es_core::Result<Option<AggregateState>> {
        let identity = (
            aggregate_type.clone(),
            partition_id,
            aggregate_id.to_string(),
        );
        if let Some(state) = batch.aggregate_states.get(&identity) {
            return Ok(state.clone());
        }
        let state = self.read_aggregate_state(aggregate_type, partition_id, aggregate_id)?;
        batch.aggregate_states.insert(identity, state.clone());
        Ok(state)
    }

    fn batch_aggregate_partition_position(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
    ) -> es_core::Result<u64> {
        let identity = (aggregate_type.clone(), partition_id);
        if let Some(position) = batch.aggregate_partition_positions.get(&identity) {
            return Ok(*position);
        }
        let key = key::sm_aggregate_next_position(self.shard_id(), aggregate_type, partition_id);
        let position = self
            .get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区 next_position 反序列化失败: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(0);
        batch
            .aggregate_partition_positions
            .insert(identity, position);
        Ok(position)
    }

    fn batch_aggregate_partition_fence(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
    ) -> es_core::Result<u64> {
        let identity = (aggregate_type.clone(), partition_id);
        if let Some(generation) = batch.aggregate_partition_fences.get(&identity) {
            return Ok(*generation);
        }
        let key = key::sm_aggregate_partition_fence(self.shard_id(), aggregate_type, partition_id);
        let generation = self
            .get(&key)?
            .map(|bytes| {
                crate::encode::decode(&bytes).map_err(|error| {
                    es_core::Error::Serde(format!("聚合分区 fence 反序列化失败: {error}"))
                })
            })
            .transpose()?
            .unwrap_or(0);
        batch
            .aggregate_partition_fences
            .insert(identity, generation);
        Ok(generation)
    }

    fn check_expected_aggregate_version(
        expected: ExpectedAggregateVersion,
        current: Option<u64>,
    ) -> bool {
        match expected {
            ExpectedAggregateVersion::Any => true,
            ExpectedAggregateVersion::NoAggregate => current.is_none(),
            ExpectedAggregateVersion::AggregateExists => current.is_some(),
            ExpectedAggregateVersion::Exact(expected) => current == Some(expected),
        }
    }

    // apply 参数逐一对应 Raft command 字段，保持显式可审计。
    #[allow(clippy::too_many_arguments)]
    fn apply_aggregate_append(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        partition_generation: u64,
        aggregate_id: &str,
        expected_version: ExpectedAggregateVersion,
        event: &NewAggregateEvent,
        hlc: es_core::Hlc,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = aggregate_type.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(error) = es_core::validate_aggregate_identifier("aggregate_id", aggregate_id) {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if event.event_type.is_empty() {
            return Ok(EsResponse::AggregateInvalid {
                reason: "event_type 不能为空".into(),
            });
        }
        let shard = self.shard_id();
        let idempotency_identity = (aggregate_type.clone(), partition_id, event.event_id);
        let fingerprint = es_core::aggregate_append_fingerprint(
            aggregate_type,
            aggregate_id,
            expected_version,
            event,
        );
        let existing = if let Some(record) = batch.aggregate_idempotency.get(&idempotency_identity)
        {
            Some(record.clone())
        } else {
            let key =
                key::sm_aggregate_idempotency(shard, aggregate_type, partition_id, &event.event_id);
            let record: Option<AggregateIdempotencyRecord> = self
                .get(&key)?
                .map(|bytes| {
                    crate::encode::decode(&bytes).map_err(|error| {
                        es_core::Error::Serde(format!("聚合事件幂等记录反序列化失败: {error}"))
                    })
                })
                .transpose()?;
            if let Some(record) = &record {
                batch
                    .aggregate_idempotency
                    .insert(idempotency_identity.clone(), record.clone());
            }
            record
        };
        if let Some(existing) = existing {
            return if existing.fingerprint == fingerprint {
                Ok(EsResponse::AggregateAppendOk {
                    aggregate_version: existing.result.aggregate_version,
                    partition_position: existing.result.partition_position,
                })
            } else {
                Ok(EsResponse::AggregateIdempotencyConflict)
            };
        }

        let current_fence =
            self.batch_aggregate_partition_fence(batch, aggregate_type, partition_id)?;
        if current_fence != partition_generation {
            return Ok(EsResponse::AggregatePartitionFenced {
                current_generation: current_fence,
            });
        }

        let current =
            self.batch_aggregate_version(batch, aggregate_type, partition_id, aggregate_id)?;
        if !Self::check_expected_aggregate_version(expected_version, current) {
            return Ok(EsResponse::AggregateOptimisticConflict {
                actual_version: current,
            });
        }
        let aggregate_version = match current {
            None => 0,
            Some(version) => match version.checked_add(1) {
                Some(next) => next,
                None => {
                    return Ok(EsResponse::AggregateInvalid {
                        reason: "聚合版本已耗尽".into(),
                    });
                }
            },
        };
        let partition_position =
            self.batch_aggregate_partition_position(batch, aggregate_type, partition_id)?;
        let Some(next_partition_position) = partition_position.checked_add(1) else {
            return Ok(EsResponse::AggregateInvalid {
                reason: "分区位置已耗尽".into(),
            });
        };

        let stored = AggregateEvent {
            aggregate_type: aggregate_type.clone(),
            partition_id,
            aggregate_id: aggregate_id.to_string(),
            aggregate_version,
            event_id: event.event_id,
            event_type: event.event_type.clone(),
            data: event.data.clone(),
            metadata: event.metadata.clone(),
            hlc,
            partition_position,
        };
        let bytes = crate::encode::encode(&stored).map_err(|error| {
            es_core::Error::Serde(format!("AggregateEvent 序列化失败: {error}"))
        })?;
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_event(
                shard,
                aggregate_type,
                partition_id,
                aggregate_id,
                aggregate_version,
            ),
            bytes,
        ));
        let pointer = crate::encode::encode(&(aggregate_id, aggregate_version))
            .map_err(|error| es_core::Error::Serde(format!("聚合分区索引序列化失败: {error}")))?;
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_partition_index(
                shard,
                aggregate_type,
                partition_id,
                partition_position,
            ),
            pointer,
        ));
        let meta = AggregateMeta {
            current_version: aggregate_version,
        };
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_meta(shard, aggregate_type, partition_id, aggregate_id),
            crate::encode::encode(&meta).map_err(|error| {
                es_core::Error::Serde(format!("AggregateMeta 序列化失败: {error}"))
            })?,
        ));
        let result = es_core::AggregateAppendResult {
            aggregate_version,
            partition_position,
        };
        let idempotency = AggregateIdempotencyRecord {
            fingerprint,
            result,
        };
        batch.ops.push(ApplyOp::Put(
            key::sm_aggregate_idempotency(shard, aggregate_type, partition_id, &event.event_id),
            crate::encode::encode(&idempotency).map_err(|error| {
                es_core::Error::Serde(format!("聚合事件幂等记录序列化失败: {error}"))
            })?,
        ));

        batch.aggregate_versions.insert(
            (
                aggregate_type.clone(),
                partition_id,
                aggregate_id.to_string(),
            ),
            Some(aggregate_version),
        );
        batch.aggregate_partition_positions.insert(
            (aggregate_type.clone(), partition_id),
            next_partition_position,
        );
        batch
            .aggregate_idempotency
            .insert(idempotency_identity, idempotency);
        batch.new_aggregate_events.push(stored);
        Ok(EsResponse::AggregateAppendOk {
            aggregate_version,
            partition_position,
        })
    }

    // apply 参数逐一对应 Raft command 字段，保持显式可审计。
    #[allow(clippy::too_many_arguments)]
    fn apply_put_aggregate_state(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        partition_generation: u64,
        aggregate_id: &str,
        expected_revision: ExpectedStateRevision,
        data: &[u8],
        hlc: Hlc,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = aggregate_type.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(error) = es_core::validate_aggregate_identifier("aggregate_id", aggregate_id) {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        let current_fence =
            self.batch_aggregate_partition_fence(batch, aggregate_type, partition_id)?;
        if current_fence != partition_generation {
            return Ok(EsResponse::AggregatePartitionFenced {
                current_generation: current_fence,
            });
        }
        if self
            .batch_aggregate_version(batch, aggregate_type, partition_id, aggregate_id)?
            .is_none()
        {
            return Ok(EsResponse::AggregateNotFound);
        }
        let current =
            self.batch_aggregate_state(batch, aggregate_type, partition_id, aggregate_id)?;
        let matches = match expected_revision {
            ExpectedStateRevision::Absent => current.is_none(),
            ExpectedStateRevision::Exact(expected) => current
                .as_ref()
                .is_some_and(|state| state.revision == expected),
        };
        if !matches {
            return Ok(EsResponse::AggregateStateConflict {
                actual_revision: current.as_ref().map(|state| state.revision),
            });
        }
        let revision = match current {
            None => 0,
            Some(state) => match state.revision.checked_add(1) {
                Some(next) => next,
                None => {
                    return Ok(EsResponse::AggregateInvalid {
                        reason: "状态 revision 已耗尽".into(),
                    });
                }
            },
        };
        let state = AggregateState {
            revision,
            data: data.to_vec(),
        };
        let key =
            key::sm_aggregate_state(self.shard_id(), aggregate_type, partition_id, aggregate_id);
        let bytes = crate::encode::encode(&state).map_err(|error| {
            es_core::Error::Serde(format!("AggregateState 序列化失败: {error}"))
        })?;
        batch.ops.push(ApplyOp::Put(key, bytes));
        let modified_key = key::sm_aggregate_state_modified(
            self.shard_id(),
            aggregate_type,
            partition_id,
            aggregate_id,
        );
        let modified_bytes = crate::encode::encode(&hlc)
            .map_err(|error| es_core::Error::Serde(format!("状态修改时间序列化失败: {error}")))?;
        batch.ops.push(ApplyOp::Put(modified_key, modified_bytes));
        batch.aggregate_states.insert(
            (
                aggregate_type.clone(),
                partition_id,
                aggregate_id.to_string(),
            ),
            Some(state.clone()),
        );
        Ok(EsResponse::AggregateStateStored {
            state: AggregateStateDocument {
                revision: state.revision,
                data: state.data,
                modified_hlc: hlc,
            },
        })
    }

    fn apply_aggregate_partition_fence(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        generation: u64,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = aggregate_type.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if generation == 0 {
            return Ok(EsResponse::AggregateInvalid {
                reason: "聚合分区 generation 必须大于 0".into(),
            });
        }
        let current = self.batch_aggregate_partition_fence(batch, aggregate_type, partition_id)?;
        let installed = current.max(generation);
        if installed != current {
            let key =
                key::sm_aggregate_partition_fence(self.shard_id(), aggregate_type, partition_id);
            let bytes = crate::encode::encode(&installed).map_err(|error| {
                es_core::Error::Serde(format!("聚合分区 fence 序列化失败: {error}"))
            })?;
            batch.ops.push(ApplyOp::Put(key, bytes));
            batch
                .aggregate_partition_fences
                .insert((aggregate_type.clone(), partition_id), installed);
        }
        Ok(EsResponse::AggregatePartitionFenceInstalled {
            generation: installed,
        })
    }

    fn apply_aggregate_catalog_command(
        &self,
        batch: &mut ApplyBatch,
        command: es_core::AggregateCatalogCommand,
    ) -> es_core::Result<EsResponse> {
        if batch.aggregate_catalog.is_none() {
            batch.aggregate_catalog = Some(self.read_aggregate_catalog()?);
        }
        let catalog = batch.aggregate_catalog.as_mut().expect("上方已初始化");
        let applied = catalog.apply(command);
        let changed = matches!(
            &applied.outcome,
            es_core::AggregateCatalogOutcome::AggregateType { changed: true, .. }
        );
        if changed {
            let bytes = crate::encode::encode(catalog).map_err(|error| {
                es_core::Error::Serde(format!("AggregateCatalog 序列化失败: {error}"))
            })?;
            batch.ops.push(ApplyOp::Put(
                key::sm_aggregate_catalog(self.shard_id()),
                bytes,
            ));
        }
        Ok(EsResponse::AggregateCatalogApplied(applied))
    }

    fn apply_aggregate_group_catalog_command(
        &self,
        batch: &mut ApplyBatch,
        command: es_core::AggregateGroupCatalogCommand,
    ) -> es_core::Result<EsResponse> {
        if batch.aggregate_group_catalog.is_none() {
            batch.aggregate_group_catalog = Some(self.read_aggregate_group_catalog()?);
        }
        let catalog = batch
            .aggregate_group_catalog
            .as_mut()
            .expect("上方已初始化");
        let previous_revision = catalog.revision;
        let applied = catalog.apply(command);
        if catalog.revision != previous_revision {
            let bytes = crate::encode::encode(catalog).map_err(|error| {
                es_core::Error::Serde(format!("AggregateGroupCatalog 序列化失败: {error}"))
            })?;
            batch.ops.push(ApplyOp::Put(
                key::sm_aggregate_group_catalog(self.shard_id()),
                bytes,
            ));
        }
        Ok(EsResponse::AggregateGroupCatalogApplied(applied))
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_aggregate_group_partition(
        &self,
        batch: &mut ApplyBatch,
        aggregate_type: &AggregateTypeId,
        partition_id: u16,
        partition_generation: u64,
        group_name: &str,
        group_epoch: u64,
        start_position: u64,
        settings: &es_core::AggregateGroupSettings,
        command: crate::AggregateGroupPartitionCommand,
    ) -> es_core::Result<EsResponse> {
        if let Err(error) = aggregate_type.validate() {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(error) = es_core::validate_aggregate_identifier("group_name", group_name) {
            return Ok(EsResponse::AggregateInvalid {
                reason: error.to_string(),
            });
        }
        if let Err(reason) = settings.validate() {
            return Ok(EsResponse::AggregateInvalid { reason });
        }
        let current_fence =
            self.batch_aggregate_partition_fence(batch, aggregate_type, partition_id)?;
        if current_fence != partition_generation {
            return Ok(EsResponse::AggregatePartitionFenced {
                current_generation: current_fence,
            });
        }
        let identity = (aggregate_type.clone(), partition_id, group_name.to_string());
        let mut state = match batch.aggregate_group_partitions.get(&identity) {
            Some(state) => state.clone(),
            None => self
                .read_aggregate_group_partition(aggregate_type, partition_id, group_name)?
                .unwrap_or_else(|| AggregateGroupPartition::new(group_epoch, start_position)),
        };
        if state.epoch > group_epoch {
            return Ok(EsResponse::AggregateGroupStaleEpoch {
                current_epoch: state.epoch,
            });
        }
        if state.epoch < group_epoch {
            state.reset(group_epoch, start_position);
        }
        let response = match command {
            crate::AggregateGroupPartitionCommand::Claim {
                consumer_id,
                now_ms,
                deadline_ms,
                max_claim,
                max_bytes,
                candidates,
            } => EsResponse::AggregateGroupClaimed(state.claim(
                &consumer_id,
                now_ms,
                deadline_ms,
                settings,
                max_claim,
                max_bytes,
                candidates,
            )),
            crate::AggregateGroupPartitionCommand::Settle {
                consumer_id,
                now_ms,
                settlements,
            } => EsResponse::AggregateGroupSettled(state.settle(
                &consumer_id,
                group_epoch,
                now_ms,
                settings,
                &settlements,
            )),
            crate::AggregateGroupPartitionCommand::Renew {
                consumer_id,
                deadline_ms,
                delivery_ids,
            } => EsResponse::AggregateGroupRenewed(state.renew(
                &consumer_id,
                group_epoch,
                deadline_ms,
                &delivery_ids,
            )),
            crate::AggregateGroupPartitionCommand::Expire { now_ms } => {
                EsResponse::AggregateGroupExpired(state.expire(now_ms, settings) as u64)
            }
        };
        let key = key::sm_aggregate_group_partition(
            self.shard_id(),
            aggregate_type,
            partition_id,
            group_name,
        );
        let bytes = crate::encode::encode(&state).map_err(|error| {
            es_core::Error::Serde(format!("AggregateGroupPartition 序列化失败: {error}"))
        })?;
        batch.ops.retain(|operation| match operation {
            ApplyOp::Put(existing, _) => *existing != key,
        });
        batch.ops.push(ApplyOp::Put(key, bytes));
        batch.aggregate_group_partitions.insert(identity, state);
        Ok(response)
    }

    fn sm_range(&self) -> es_core::Result<(Vec<u8>, Vec<u8>)> {
        let mut start = Vec::with_capacity(10);
        start.push(0x02u8); // TAG_SM
        start.extend_from_slice(&self.shard_id().to_be_bytes());
        let end = key::successor(&start)
            .ok_or_else(|| es_core::Error::Internal("状态机区前缀无后继".into()))?;
        Ok((start, end))
    }

    /// 扫出本分片状态机区的全部 kv。
    ///
    /// 独立成同步函数：surrealkv 的迭代器不是 `Send`，不能跨 await 存活，
    /// 否则整个 async fn 的 future 失去 Send，无法满足 openraft 的 trait 约束。
    fn collect_sm_entries(&self) -> es_core::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (start, end) = self.sm_range()?;
        let txn = self
            .tree()
            .begin()
            .map_err(|e| es_core::Error::Storage(format!("begin 失败: {e}")))?;
        let mut it = txn
            .range(start, end)
            .map_err(|e| es_core::Error::Storage(format!("range 失败: {e}")))?;
        let mut out = Vec::new();
        it.seek_first()
            .map_err(|e| es_core::Error::Storage(format!("seek_first 失败: {e}")))?;
        while it.valid() {
            let k = it.key().user_key().to_vec();
            let v = it
                .value()
                .map_err(|e| es_core::Error::Storage(format!("value 失败: {e}")))?
                .to_vec();
            out.push((k, v));
            it.next()
                .map_err(|e| es_core::Error::Storage(format!("next 失败: {e}")))?;
        }
        Ok(out)
    }
}

impl RaftStateMachine<TypeConfig> for EsStorage {
    type SnapshotBuilder = Self;

    /// 返回已应用日志与成员关系。
    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        let sm = self.sm_cache().read().await;
        Ok((sm.last_applied, sm.membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<EsResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let shard = self.shard_id();
        let mut batch = ApplyBatch {
            ops: Vec::new(),
            aggregate_versions: std::collections::HashMap::new(),
            aggregate_partition_positions: std::collections::HashMap::new(),
            aggregate_states: std::collections::HashMap::new(),
            aggregate_idempotency: std::collections::HashMap::new(),
            aggregate_partition_fences: std::collections::HashMap::new(),
            aggregate_catalog: None,
            aggregate_group_catalog: None,
            aggregate_group_partitions: std::collections::HashMap::new(),
            new_aggregate_events: Vec::new(),
        };

        let mut responses = Vec::new();
        let mut last_applied = None;
        let mut membership: Option<StoredMembership<u64, openraft::BasicNode>> = None;

        for entry in entries {
            last_applied = Some(entry.log_id);

            match entry.payload {
                // Blank 是 leader 上任时的空日志，仅推进 last_applied
                EntryPayload::Blank => responses.push(EsResponse::Noop),
                EntryPayload::Normal(ref req) => match req {
                    EsRequest::AggregateAppend {
                        aggregate_type,
                        partition_id,
                        partition_generation,
                        aggregate_id,
                        expected_version,
                        event,
                        hlc,
                    } => {
                        let response = self
                            .apply_aggregate_append(
                                &mut batch,
                                aggregate_type,
                                *partition_id,
                                *partition_generation,
                                aggregate_id,
                                *expected_version,
                                event,
                                *hlc,
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::PutAggregateState {
                        aggregate_type,
                        partition_id,
                        partition_generation,
                        aggregate_id,
                        expected_revision,
                        data,
                        hlc,
                    } => {
                        let response = self
                            .apply_put_aggregate_state(
                                &mut batch,
                                aggregate_type,
                                *partition_id,
                                *partition_generation,
                                aggregate_id,
                                *expected_revision,
                                data,
                                *hlc,
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::InstallAggregatePartitionFence {
                        aggregate_type,
                        partition_id,
                        generation,
                    } => {
                        let response = self
                            .apply_aggregate_partition_fence(
                                &mut batch,
                                aggregate_type,
                                *partition_id,
                                *generation,
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::CommitAggregateCatalog { command } => {
                        let response = self
                            .apply_aggregate_catalog_command(&mut batch, command.clone())
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::CommitAggregateGroupCatalog { command } => {
                        let response = self
                            .apply_aggregate_group_catalog_command(&mut batch, command.clone())
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                    EsRequest::AggregateGroupPartition {
                        aggregate_type,
                        partition_id,
                        partition_generation,
                        group_name,
                        group_epoch,
                        start_position,
                        settings,
                        command,
                    } => {
                        let response = self
                            .apply_aggregate_group_partition(
                                &mut batch,
                                aggregate_type,
                                *partition_id,
                                *partition_generation,
                                group_name,
                                *group_epoch,
                                *start_position,
                                settings,
                                command.clone(),
                            )
                            .map_err(sm_write_err)?;
                        responses.push(response);
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    membership = Some(StoredMembership::new(Some(entry.log_id), mem.clone()));
                    responses.push(EsResponse::Noop);
                }
            }
        }

        for ((aggregate_type, partition_id), next_position) in &batch.aggregate_partition_positions
        {
            let bytes = crate::encode::encode(next_position).map_err(sm_write_err)?;
            batch.ops.push(ApplyOp::Put(
                key::sm_aggregate_next_position(shard, aggregate_type, *partition_id),
                bytes,
            ));
        }

        // 已应用状态：与业务数据同事务提交，保证重启后 last_applied 与数据一致
        let mut cache = self.sm_cache().write().await;
        let new_last_applied = last_applied.or(cache.last_applied);
        let new_membership = membership
            .clone()
            .unwrap_or_else(|| cache.membership.clone());
        let applied = AppliedState {
            last_applied: new_last_applied,
            membership: new_membership.clone(),
        };
        let applied_bytes = crate::encode::encode(&applied).map_err(sm_write_err)?;
        batch
            .ops
            .push(ApplyOp::Put(key::sm_applied_state(shard), applied_bytes));

        // 单事务提交全部聚合数据与已应用状态。
        let mut txn = self.tree().begin().map_err(sm_write_err)?;
        for op in &batch.ops {
            match op {
                ApplyOp::Put(k, v) => txn.set(k.clone(), v.clone()).map_err(sm_write_err)?,
            }
        }
        txn.commit().await.map_err(sm_write_err)?;

        // 提交成功后才更新内存缓存，失败时缓存不被污染
        cache.last_applied = new_last_applied;
        cache.membership = new_membership;

        for event in batch.new_aggregate_events {
            let _ = self.aggregate_event_tx().send(event);
        }

        Ok(responses)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<<TypeConfig as RaftTypeConfig>::SnapshotData>, StorageError<u64>>
    {
        // 传输数据写入 incoming 临时文件而非内存：大快照分块到达时逐块落盘
        let store = self.snapshot_store();
        store.ensure_dirs().map_err(sm_write_err)?;
        let sf = snapshot::SnapshotFile::create_temp(&store.incoming_dir())
            .await
            .map_err(|e| sm_write_err(&e))?;
        Ok(Box::new(sf))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<<TypeConfig as RaftTypeConfig>::SnapshotData>,
    ) -> std::result::Result<(), StorageError<u64>> {
        // 关 tokio 句柄转 std 句柄：安装是同步段（解压流是 std::io::Read），
        // 且不再持有传输句柄后可对临时文件做转正 rename。
        // 注意：此处返回的 path/temp 已从 SnapshotFile 中取出（mem::take），
        // 原对象 Drop 时不会删除已转正的文件。
        let mut snapshot = snapshot;
        // 防御：shutdown 刷出 tokio File 内部缓冲（tokio 1.53 的 File 写有
        // 用户态缓冲，write_all 返回不代表落盘）。openraft Chunked 在传输
        // 完成时已调用 shutdown（snapshot_transport.rs done 分支），此处
        // 幂等兜底，防未来链路变化丢数据。
        {
            use tokio::io::AsyncWriteExt as _;
            snapshot.shutdown().await.map_err(sm_write_err)?;
        }
        let (mut file, path, is_temp) = snapshot.into_std_file().map_err(sm_read_err)?;
        // RAII 兜底：错误路径（读/解压/提交失败）删除残留的 temp 文件。
        // 成功后由转正 rename 接管（guard 置 None），否则文件常驻磁盘
        // 直到下次启动清理。
        struct TempGuard(Option<std::path::PathBuf>);
        impl Drop for TempGuard {
            fn drop(&mut self) {
                if let Some(p) = &self.0 {
                    let _ = std::fs::remove_file(p);
                }
            }
        }
        let mut temp_guard = TempGuard(if is_temp { Some(path.clone()) } else { None });

        // 头部校验：magic/version/压缩 tag 不符即报错；snapshot_id 必须与
        // openraft 请求一致（防错文件被当作目标快照安装）
        let (header, file_meta) = snapshot::read_header(&mut file).map_err(sm_read_err)?;
        if file_meta.snapshot_id != meta.snapshot_id {
            return Err(sm_read_err(std::io::Error::other(format!(
                "快照文件 snapshot_id 不一致：文件 {} vs 请求 {}",
                file_meta.snapshot_id, meta.snapshot_id
            ))));
        }

        // 定位到 payload 起点（头部与 meta 未压缩）
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(32 + header.meta_len))
            .map_err(sm_read_err)?;
        let mut reader = header.compression.reader(file).map_err(sm_read_err)?;

        let shard = self.shard_id();

        // 先清掉本分片状态机区的全部现有数据，再灌入快照内容。
        // 不清空会残留快照里已不存在的 key（例如被 purge 掉的事件）。
        let (sm_start, sm_end) = self.sm_range().map_err(sm_write_err)?;
        let old_keys = self.collect_keys(sm_start, sm_end).map_err(sm_write_err)?;

        let mut cache = self.sm_cache().write().await;

        let mut txn = self.tree().begin().map_err(sm_write_err)?;
        for k in &old_keys {
            txn.delete(k.clone()).map_err(sm_write_err)?;
        }
        // 流式解压逐条灌入（不把整个快照载入内存；txn 缓冲上限见 docs/snapshot.md）
        let read_bytes = snapshot::for_each_record(&mut reader, |k, v| {
            txn.set(k, v)
                .map_err(|e| std::io::Error::other(format!("set 失败: {e}")))?;
            Ok(())
        })
        .map_err(sm_read_err)?;
        if read_bytes != header.payload_len {
            return Err(sm_read_err(std::io::Error::other(format!(
                "快照 payload 长度不符：实读 {read_bytes} vs 声明 {}",
                header.payload_len
            ))));
        }
        // 已应用状态随快照一起写，保持原子
        let applied = AppliedState {
            last_applied: meta.last_log_id,
            membership: meta.last_membership.clone(),
        };
        txn.set(
            key::sm_applied_state(shard),
            crate::encode::encode(&applied).map_err(sm_write_err)?,
        )
        .map_err(sm_write_err)?;
        txn.commit().await.map_err(sm_write_err)?;

        cache.last_applied = meta.last_log_id;
        cache.membership = meta.last_membership.clone();

        // 转正：incoming 临时文件 → 规范名。先提交后转正：提交后转正失败
        // 只损失文件缓存（SM 数据已就位）；反过来会留下"文件在而数据不在"。
        if is_temp {
            let final_path = self.snapshot_store().final_path(meta.last_log_id);
            if let Err(e) = std::fs::rename(&path, &final_path) {
                tracing::warn!("快照文件转正失败（仅损失文件缓存，SM 数据已提交）: {e}");
            } else {
                temp_guard.0 = None; // 转正成功，guard 不再删除
            }
        } else {
            temp_guard.0 = None;
        }
        // 保留清理：装快照也计入历史保留
        let store = self.snapshot_store();
        if let Err(e) = store.retain(store.keep()) {
            tracing::warn!("快照保留清理失败: {e}");
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let store = self.snapshot_store();
        // 过滤领先于 applied 的快照：restore/崩溃残留的更新文件与状态机不一致
        let applied = self.sm_cache().read().await.last_applied;
        let Some(path) = store.latest(applied).map_err(sm_read_err)? else {
            return Ok(None);
        };
        // 读文件头取该文件自己的完整 meta（含 last_membership，供 follower 安装）
        let mut f = std::fs::File::open(&path).map_err(sm_read_err)?;
        let (_, meta) = snapshot::read_header(&mut f).map_err(sm_read_err)?;
        let sf = snapshot::SnapshotFile::open(path)
            .await
            .map_err(|e| sm_read_err(&e))?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(sf),
        }))
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for EsStorage {
    #[allow(clippy::result_large_err)]
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let shard = self.shard_id();

        // 在同一个锁作用域内取元数据与数据，避免与并发 apply 交错产生撕裂快照。
        // 迭代器在这个块内创建并销毁，不跨 await。
        let (last_applied, membership, entries) = {
            let cache = self.sm_cache().read().await;
            let entries = self.collect_sm_entries().map_err(sm_read_err)?;
            (cache.last_applied, cache.membership.clone(), entries)
        };

        // snapshot_id 需在同一分片内唯一且可比较。用 last_applied 的
        // term/index 拼接，避免依赖墙上时钟（Date::now 在确定性回放里不可用）。
        let snapshot_id = match last_applied {
            Some(l) => format!("{}-{}-{}", shard, l.leader_id.term, l.index),
            None => format!("{shard}-empty"),
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        // 写临时文件 → 原子 rename 转正：崩溃不会留下半写的正式快照文件。
        // 头部与 meta 未压缩（裸文件写入），payload 按配置压缩（docs/snapshot.md）。
        let store = self.snapshot_store();
        store.ensure_dirs().map_err(sm_write_err)?;
        let tmp = store.tmp_path();
        let header = snapshot::SnapshotHeader {
            version: snapshot::SNAP_VERSION,
            compression: store.compression(),
            shard_id: shard,
            // meta_len 必须与实际写 header 的编码一致（write_header 保留 serde_json）
            meta_len: serde_json::to_vec(&meta).map_err(sm_write_err)?.len() as u64,
            payload_len: snapshot::payload_len_for(&entries),
        };
        // 写段失败时清理 tmp（不调 finish 的 zstd Encoder Drop 不补帧尾，
        // 残留的半写文件由本处删除与启动清理兜底）
        let write_result = (|| -> std::result::Result<(), StorageError<u64>> {
            let mut f = std::fs::File::create(&tmp).map_err(sm_write_err)?;
            snapshot::write_header(&mut f, &header, &meta).map_err(sm_write_err)?;
            let mut w = store.compression().writer(f).map_err(sm_write_err)?;
            w.write_all(&(entries.len() as u64).to_le_bytes())
                .map_err(sm_write_err)?;
            for (k, v) in &entries {
                snapshot::write_record(&mut w, k, v).map_err(sm_write_err)?;
            }
            snapshot::write_end_marker(&mut w).map_err(sm_write_err)?;
            w.finish().map_err(sm_write_err)?;
            Ok(())
        })();
        if let Err(e) = write_result {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        let final_path = store.final_path(last_applied);
        std::fs::rename(&tmp, &final_path).map_err(sm_write_err)?;

        // 保留清理：删除超出 keep 的旧快照
        let removed = store.retain(store.keep()).map_err(sm_write_err)?;
        for p in &removed {
            tracing::info!("快照保留策略删除旧快照: {}", p.display());
        }

        let sf = snapshot::SnapshotFile::open(final_path)
            .await
            .map_err(|e| sm_write_err(&e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(sf),
        })
    }
}
