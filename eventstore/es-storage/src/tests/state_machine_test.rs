//! RaftStateMachine apply 语义测试。

use openraft::storage::RaftStateMachine;

use super::*;
use es_core::Hlc;

fn aggregate_aggregate_type() -> es_core::AggregateTypeId {
    es_core::AggregateTypeId::new("orders", "order").expect("合法聚合类型")
}

fn aggregate_event(event_id: uuid::Uuid, data: &[u8]) -> es_core::NewAggregateEvent {
    es_core::NewAggregateEvent {
        event_id,
        event_type: "order.changed".into(),
        data: data.to_vec(),
        metadata: b"{}".to_vec(),
    }
}

fn aggregate_fence_entry(index: u64, generation: u64) -> openraft::Entry<crate::TypeConfig> {
    request_entry(
        index,
        crate::EsRequest::InstallAggregatePartitionFence {
            aggregate_type: aggregate_aggregate_type(),
            partition_id: 7,
            generation,
        },
    )
}

fn aggregate_append_entry(
    index: u64,
    aggregate_id: &str,
    expected_version: es_core::ExpectedAggregateVersion,
    event: es_core::NewAggregateEvent,
    generation: u64,
) -> openraft::Entry<crate::TypeConfig> {
    request_entry(
        index,
        crate::EsRequest::AggregateAppend {
            aggregate_type: aggregate_aggregate_type(),
            partition_id: 7,
            partition_generation: generation,
            aggregate_id: aggregate_id.into(),
            expected_version,
            event,
            hlc: hlc(2_000 + index),
        },
    )
}

fn aggregate_state_entry(
    index: u64,
    aggregate_id: &str,
    expected_revision: es_core::ExpectedStateRevision,
    data: &[u8],
) -> openraft::Entry<crate::TypeConfig> {
    request_entry(
        index,
        crate::EsRequest::PutAggregateState {
            aggregate_type: aggregate_aggregate_type(),
            partition_id: 7,
            partition_generation: 1,
            aggregate_id: aggregate_id.into(),
            expected_revision,
            data: data.to_vec(),
            hlc: hlc(3_000 + index),
        },
    )
}

fn aggregate_group_definition(operation_id: uuid::Uuid) -> es_core::AggregateGroupDefinition {
    es_core::AggregateGroupDefinition {
        aggregate_type: aggregate_aggregate_type(),
        name: "workers".into(),
        revision: 0,
        epoch: 0,
        start: es_core::AggregateGroupStart::Beginning,
        partition_starts: (0..es_core::EVENT_PARTITION_COUNT)
            .map(|partition| (partition, 0))
            .collect(),
        settings: es_core::AggregateGroupSettings::default(),
        create_operation_id: operation_id,
        last_operation_id: operation_id,
    }
}

fn aggregate_group_candidate(
    delivery_id: uuid::Uuid,
    position: u64,
    aggregate_id: &str,
) -> es_core::AggregateDeliveryCandidate {
    es_core::AggregateDeliveryCandidate {
        delivery_id,
        partition_position: position,
        aggregate_id: aggregate_id.into(),
        aggregate_version: position,
        event_id: uuid::Uuid::new_v4(),
        payload_bytes: 1,
        replayed: false,
    }
}

fn aggregate_group_entry(
    index: u64,
    command: crate::AggregateGroupPartitionCommand,
) -> openraft::Entry<crate::TypeConfig> {
    request_entry(
        index,
        crate::EsRequest::AggregateGroupPartition {
            aggregate_type: aggregate_aggregate_type(),
            partition_id: 7,
            partition_generation: 1,
            group_name: "workers".into(),
            group_epoch: 1,
            start_position: 0,
            settings: es_core::AggregateGroupSettings::default(),
            command,
        },
    )
}

fn hlc(wall: u64) -> Hlc {
    Hlc { wall, logical: 0 }
}

fn request_entry(index: u64, request: crate::EsRequest) -> openraft::Entry<crate::TypeConfig> {
    openraft::Entry {
        log_id: log_id(1, index),
        payload: openraft::EntryPayload::Normal(request),
    }
}

#[tokio::test]
async fn aggregate_instances_have_independent_versions_and_shared_partition_positions() {
    let (mut storage, _dir) = new_storage(0);
    let responses = storage
        .apply(vec![
            aggregate_fence_entry(0, 1),
            aggregate_append_entry(
                1,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"one-zero"),
                1,
            ),
            aggregate_append_entry(
                2,
                "order-2",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"two-zero"),
                1,
            ),
            aggregate_append_entry(
                3,
                "order-1",
                es_core::ExpectedAggregateVersion::Exact(0),
                aggregate_event(uuid::Uuid::new_v4(), b"one-one"),
                1,
            ),
        ])
        .await
        .expect("应用聚合事件批次");

    assert!(matches!(
        responses[1],
        crate::EsResponse::AggregateAppendOk {
            aggregate_version: 0,
            partition_position: 0
        }
    ));
    assert!(matches!(
        responses[2],
        crate::EsResponse::AggregateAppendOk {
            aggregate_version: 0,
            partition_position: 1
        }
    ));
    assert!(matches!(
        responses[3],
        crate::EsResponse::AggregateAppendOk {
            aggregate_version: 1,
            partition_position: 2
        }
    ));

    let first: Vec<_> = storage
        .read_aggregate_partition_events(&aggregate_aggregate_type(), 7, 0, 0)
        .expect("读取类型分区")
        .into_iter()
        .filter(|event| event.aggregate_id == "order-1")
        .collect();
    assert_eq!(
        first
            .iter()
            .map(|event| event.aggregate_version)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    let partition = storage
        .read_aggregate_partition_events(&aggregate_aggregate_type(), 7, 0, 0)
        .expect("读取分区");
    assert_eq!(
        partition
            .iter()
            .map(|event| (event.aggregate_id.as_str(), event.aggregate_version))
            .collect::<Vec<_>>(),
        vec![("order-1", 0), ("order-2", 0), ("order-1", 1)]
    );
}

#[tokio::test]
async fn aggregate_occ_conflict_does_not_consume_partition_position() {
    let (mut storage, _dir) = new_storage(0);
    let responses = storage
        .apply(vec![
            aggregate_fence_entry(0, 1),
            aggregate_append_entry(
                1,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"first"),
                1,
            ),
            aggregate_append_entry(
                2,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"conflict"),
                1,
            ),
            aggregate_append_entry(
                3,
                "order-2",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"second"),
                1,
            ),
        ])
        .await
        .expect("应用 OCC 批次");
    assert!(matches!(
        responses[2],
        crate::EsResponse::AggregateOptimisticConflict {
            actual_version: Some(0)
        }
    ));
    assert!(matches!(
        responses[3],
        crate::EsResponse::AggregateAppendOk {
            aggregate_version: 0,
            partition_position: 1
        }
    ));
}

#[tokio::test]
async fn aggregate_idempotency_detects_same_batch_and_content_conflict() {
    let (mut storage, _dir) = new_storage(0);
    let event_id = uuid::Uuid::new_v4();
    let original = aggregate_event(event_id, b"original");
    let responses = storage
        .apply(vec![
            aggregate_fence_entry(0, 1),
            aggregate_append_entry(
                1,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                original.clone(),
                1,
            ),
            aggregate_append_entry(
                2,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                original,
                1,
            ),
            aggregate_append_entry(
                3,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(event_id, b"changed"),
                1,
            ),
        ])
        .await
        .expect("应用幂等批次");
    assert!(matches!(
        responses[1],
        crate::EsResponse::AggregateAppendOk {
            aggregate_version: 0,
            partition_position: 0
        }
    ));
    assert_eq!(format!("{:?}", responses[1]), format!("{:?}", responses[2]));
    assert!(matches!(
        responses[3],
        crate::EsResponse::AggregateIdempotencyConflict
    ));
    assert_eq!(
        storage
            .read_aggregate_partition_events(&aggregate_aggregate_type(), 7, 0, 0)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn aggregate_partition_fence_rejects_missing_and_stale_generation() {
    let (mut storage, _dir) = new_storage(0);
    let responses = storage
        .apply(vec![
            aggregate_append_entry(
                0,
                "order-1",
                es_core::ExpectedAggregateVersion::Any,
                aggregate_event(uuid::Uuid::new_v4(), b"missing"),
                1,
            ),
            aggregate_fence_entry(1, 2),
            aggregate_append_entry(
                2,
                "order-1",
                es_core::ExpectedAggregateVersion::Any,
                aggregate_event(uuid::Uuid::new_v4(), b"stale"),
                1,
            ),
        ])
        .await
        .expect("应用 fence 批次");
    assert!(matches!(
        responses[0],
        crate::EsResponse::AggregatePartitionFenced {
            current_generation: 0
        }
    ));
    assert!(matches!(
        responses[2],
        crate::EsResponse::AggregatePartitionFenced {
            current_generation: 2
        }
    ));
    assert!(
        storage
            .read_aggregate_meta(&aggregate_aggregate_type(), 7, "order-1")
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn aggregate_state_requires_instance_and_uses_revision_cas() {
    let (mut storage, _dir) = new_storage(0);
    let responses = storage
        .apply(vec![
            aggregate_fence_entry(0, 1),
            aggregate_state_entry(1, "missing", es_core::ExpectedStateRevision::Absent, b"{}"),
            aggregate_append_entry(
                2,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"created"),
                1,
            ),
            aggregate_state_entry(
                3,
                "order-1",
                es_core::ExpectedStateRevision::Absent,
                b"{\"balance\":100}",
            ),
            aggregate_state_entry(
                4,
                "order-1",
                es_core::ExpectedStateRevision::Absent,
                b"{\"balance\":50}",
            ),
            aggregate_state_entry(
                5,
                "order-1",
                es_core::ExpectedStateRevision::Exact(0),
                b"{\"balance\":50}",
            ),
        ])
        .await
        .expect("应用状态批次");
    assert!(matches!(responses[1], crate::EsResponse::AggregateNotFound));
    assert!(matches!(
        responses[3],
        crate::EsResponse::AggregateStateStored {
            state: es_core::AggregateStateDocument { revision: 0, .. }
        }
    ));
    assert!(matches!(
        responses[4],
        crate::EsResponse::AggregateStateConflict {
            actual_revision: Some(0)
        }
    ));
    assert!(matches!(
        responses[5],
        crate::EsResponse::AggregateStateStored {
            state: es_core::AggregateStateDocument { revision: 1, .. }
        }
    ));
    assert_eq!(
        storage
            .read_aggregate_state(&aggregate_aggregate_type(), 7, "order-1")
            .unwrap()
            .unwrap(),
        es_core::AggregateState {
            revision: 1,
            data: b"{\"balance\":50}".to_vec()
        }
    );
    assert_eq!(
        storage
            .read_aggregate_state_document(&aggregate_aggregate_type(), 7, "order-1")
            .unwrap()
            .unwrap(),
        es_core::AggregateStateDocument {
            revision: 1,
            data: b"{\"balance\":50}".to_vec(),
            modified_hlc: hlc(3_005),
        }
    );
}

#[tokio::test]
async fn aggregate_state_without_modified_metadata_reads_epoch() {
    let (storage, _dir) = new_storage(0);
    let state = es_core::AggregateState {
        revision: 4,
        data: b"legacy".to_vec(),
    };
    storage
        .set(
            &crate::key::sm_aggregate_state(0, &aggregate_aggregate_type(), 7, "order-legacy"),
            &crate::encode::encode(&state).expect("编码旧状态"),
        )
        .await
        .expect("写入旧状态值");

    assert_eq!(
        storage
            .read_aggregate_state_document(&aggregate_aggregate_type(), 7, "order-legacy")
            .expect("读取旧状态")
            .expect("旧状态存在"),
        es_core::AggregateStateDocument {
            revision: 4,
            data: b"legacy".to_vec(),
            modified_hlc: hlc(0),
        }
    );
    assert_eq!(
        storage
            .list_aggregate_partition_states(&aggregate_aggregate_type(), 7, None, 10)
            .expect("列出旧状态"),
        vec![(
            "order-legacy".to_string(),
            es_core::AggregateStateDocument {
                revision: 4,
                data: b"legacy".to_vec(),
                modified_hlc: hlc(0),
            },
        )]
    );

    storage
        .set(
            &crate::key::sm_aggregate_state_modified(
                0,
                &aggregate_aggregate_type(),
                7,
                "order-legacy",
            ),
            b"truncated-hlc",
        )
        .await
        .expect("写入损坏的状态时间");
    assert!(
        storage
            .read_aggregate_state_document(&aggregate_aggregate_type(), 7, "order-legacy")
            .is_err(),
        "单状态读取必须拒绝损坏的时间元数据"
    );
    assert!(
        storage
            .list_aggregate_partition_states(&aggregate_aggregate_type(), 7, None, 10)
            .is_err(),
        "状态列表必须拒绝损坏的时间元数据"
    );
}

#[tokio::test]
async fn aggregate_apply_rejects_invalid_and_exhausted_boundaries() {
    let (mut storage, _dir) = new_storage(0);
    let invalid_type: es_core::AggregateTypeId = serde_json::from_value(serde_json::json!({
        "business_space": "",
        "aggregate_type": "order"
    }))
    .expect("反序列化用于验证的非法类型");
    let invalid_event = |event_type: &str| es_core::NewAggregateEvent {
        event_id: uuid::Uuid::new_v4(),
        event_type: event_type.into(),
        data: Vec::new(),
        metadata: Vec::new(),
    };

    let responses = storage
        .apply(vec![
            request_entry(
                0,
                crate::EsRequest::InstallAggregatePartitionFence {
                    aggregate_type: invalid_type.clone(),
                    partition_id: 7,
                    generation: 1,
                },
            ),
            request_entry(
                1,
                crate::EsRequest::InstallAggregatePartitionFence {
                    aggregate_type: aggregate_aggregate_type(),
                    partition_id: 7,
                    generation: 0,
                },
            ),
            aggregate_fence_entry(2, 1),
            request_entry(
                3,
                crate::EsRequest::AggregateAppend {
                    aggregate_type: invalid_type.clone(),
                    partition_id: 7,
                    partition_generation: 1,
                    aggregate_id: "order-1".into(),
                    expected_version: es_core::ExpectedAggregateVersion::Any,
                    event: invalid_event("Changed"),
                    hlc: hlc(4_003),
                },
            ),
            aggregate_append_entry(
                4,
                "",
                es_core::ExpectedAggregateVersion::Any,
                invalid_event("Changed"),
                1,
            ),
            aggregate_append_entry(
                5,
                "order-1",
                es_core::ExpectedAggregateVersion::Any,
                invalid_event(""),
                1,
            ),
            request_entry(
                6,
                crate::EsRequest::PutAggregateState {
                    aggregate_type: invalid_type,
                    partition_id: 7,
                    partition_generation: 1,
                    aggregate_id: "order-1".into(),
                    expected_revision: es_core::ExpectedStateRevision::Absent,
                    data: Vec::new(),
                    hlc: hlc(4_006),
                },
            ),
            aggregate_state_entry(7, "", es_core::ExpectedStateRevision::Absent, b"{}"),
        ])
        .await
        .expect("非法边界应返回领域响应而非存储错误");
    for response in [
        &responses[0],
        &responses[1],
        &responses[3],
        &responses[4],
        &responses[5],
        &responses[6],
        &responses[7],
    ] {
        assert!(matches!(
            response,
            crate::EsResponse::AggregateInvalid { .. }
        ));
    }

    let aggregate_type = aggregate_aggregate_type();
    storage
        .set(
            &crate::key::sm_aggregate_meta(0, &aggregate_type, 7, "version-max"),
            &crate::encode::encode(&es_core::AggregateMeta {
                current_version: u64::MAX,
            })
            .expect("编码最大版本"),
        )
        .await
        .expect("写最大版本");
    let version_response = storage
        .apply(vec![aggregate_append_entry(
            8,
            "version-max",
            es_core::ExpectedAggregateVersion::Any,
            invalid_event("Changed"),
            1,
        )])
        .await
        .expect("验证聚合版本耗尽");
    assert!(matches!(
        version_response[0],
        crate::EsResponse::AggregateInvalid { .. }
    ));

    storage
        .set(
            &crate::key::sm_aggregate_next_position(0, &aggregate_type, 7),
            &crate::encode::encode(&u64::MAX).expect("编码最大分区位置"),
        )
        .await
        .expect("写最大分区位置");
    let position_response = storage
        .apply(vec![aggregate_append_entry(
            9,
            "position-max",
            es_core::ExpectedAggregateVersion::NoAggregate,
            invalid_event("Changed"),
            1,
        )])
        .await
        .expect("验证分区位置耗尽");
    assert!(matches!(
        position_response[0],
        crate::EsResponse::AggregateInvalid { .. }
    ));

    storage
        .set(
            &crate::key::sm_aggregate_meta(0, &aggregate_type, 7, "state-max"),
            &crate::encode::encode(&es_core::AggregateMeta { current_version: 0 })
                .expect("编码聚合元数据"),
        )
        .await
        .expect("写聚合元数据");
    storage
        .set(
            &crate::key::sm_aggregate_state(0, &aggregate_type, 7, "state-max"),
            &crate::encode::encode(&es_core::AggregateState {
                revision: u64::MAX,
                data: Vec::new(),
            })
            .expect("编码最大状态 revision"),
        )
        .await
        .expect("写最大状态 revision");
    let state_response = storage
        .apply(vec![aggregate_state_entry(
            10,
            "state-max",
            es_core::ExpectedStateRevision::Exact(u64::MAX),
            b"{}",
        )])
        .await
        .expect("验证状态 revision 耗尽");
    assert!(matches!(
        state_response[0],
        crate::EsResponse::AggregateInvalid { .. }
    ));
}

#[tokio::test]
async fn aggregate_group_apply_validates_epoch_settings_and_commands() {
    let (mut storage, _dir) = new_storage(0);
    storage
        .apply(vec![aggregate_fence_entry(0, 1)])
        .await
        .expect("安装 fence");
    let invalid_settings = es_core::AggregateGroupSettings {
        max_unacked_per_consumer: 0,
        max_unacked_per_group: 4096,
        ack_timeout_ms: 10_000,
        max_retries: 5,
        retry_min_ms: 100,
        retry_max_ms: 5_000,
    };
    let group_request = |index, group_name: &str, generation, epoch, settings, command| {
        request_entry(
            index,
            crate::EsRequest::AggregateGroupPartition {
                aggregate_type: aggregate_aggregate_type(),
                partition_id: 7,
                partition_generation: generation,
                group_name: group_name.into(),
                group_epoch: epoch,
                start_position: 0,
                settings,
                command,
            },
        )
    };
    let expire = || crate::AggregateGroupPartitionCommand::Expire { now_ms: 10 };
    let invalid = storage
        .apply(vec![
            group_request(1, "", 1, 1, Default::default(), expire()),
            group_request(2, "workers", 1, 1, invalid_settings, expire()),
            group_request(3, "workers", 2, 1, Default::default(), expire()),
        ])
        .await
        .expect("消费者组非法边界返回领域响应");
    assert!(matches!(
        invalid[0],
        crate::EsResponse::AggregateInvalid { .. }
    ));
    assert!(matches!(
        invalid[1],
        crate::EsResponse::AggregateInvalid { .. }
    ));
    assert!(matches!(
        invalid[2],
        crate::EsResponse::AggregatePartitionFenced { .. }
    ));

    let delivery_id = uuid::Uuid::new_v4();
    let valid = storage
        .apply(vec![
            group_request(
                4,
                "workers",
                1,
                2,
                Default::default(),
                crate::AggregateGroupPartitionCommand::Claim {
                    consumer_id: "consumer-a".into(),
                    now_ms: 10,
                    deadline_ms: 20,
                    max_claim: 1,
                    max_bytes: 10,
                    candidates: vec![aggregate_group_candidate(delivery_id, 0, "order-1")],
                },
            ),
            group_request(5, "workers", 1, 2, Default::default(), expire()),
        ])
        .await
        .expect("同批次消费者组命令");
    assert!(matches!(
        valid[0],
        crate::EsResponse::AggregateGroupClaimed(_)
    ));
    assert!(matches!(
        valid[1],
        crate::EsResponse::AggregateGroupExpired(_)
    ));

    let stale = storage
        .apply(vec![group_request(
            6,
            "workers",
            1,
            1,
            Default::default(),
            expire(),
        )])
        .await
        .expect("旧 epoch 返回 stale");
    assert!(matches!(
        stale[0],
        crate::EsResponse::AggregateGroupStaleEpoch { current_epoch: 2 }
    ));

    let reset_and_renew = storage
        .apply(vec![group_request(
            7,
            "workers",
            1,
            3,
            Default::default(),
            crate::AggregateGroupPartitionCommand::Renew {
                consumer_id: "consumer-a".into(),
                deadline_ms: 30,
                delivery_ids: vec![delivery_id],
            },
        )])
        .await
        .expect("新 epoch 重置并执行 renew");
    assert!(matches!(
        reset_and_renew[0],
        crate::EsResponse::AggregateGroupRenewed(_)
    ));
}

#[tokio::test]
async fn aggregate_catalog_same_batch_create_activate_is_persisted() {
    let (mut storage, _dir) = new_storage(0);
    let operation_id = uuid::Uuid::new_v4();
    let placements = (0..es_core::EVENT_PARTITION_COUNT)
        .map(|partition| (partition, u64::from(partition % 2)))
        .collect();
    let responses = storage
        .apply(vec![
            request_entry(
                0,
                crate::EsRequest::CommitAggregateCatalog {
                    command: es_core::AggregateCatalogCommand::Create {
                        aggregate_type: aggregate_aggregate_type(),
                        operation_id,
                        seed: [9; 16],
                        placements,
                    },
                },
            ),
            request_entry(
                1,
                crate::EsRequest::CommitAggregateCatalog {
                    command: es_core::AggregateCatalogCommand::Activate {
                        aggregate_type: aggregate_aggregate_type(),
                        operation_id,
                    },
                },
            ),
        ])
        .await
        .expect("应用 catalog 批次");
    assert!(matches!(
        responses[1],
        crate::EsResponse::AggregateCatalogApplied(es_core::AggregateCatalogApply {
            revision: 2,
            outcome: es_core::AggregateCatalogOutcome::AggregateType {
                aggregate_type: es_core::AggregateTypeDefinition {
                    status: es_core::AggregateTypeStatus::Active,
                    ..
                },
                ..
            }
        })
    ));
    let catalog = storage.read_aggregate_catalog().expect("读取 catalog");
    assert_eq!(catalog.revision, 2);
    assert_eq!(
        catalog.aggregate_types[&aggregate_aggregate_type()].status,
        es_core::AggregateTypeStatus::Active
    );
}

#[tokio::test]
async fn aggregate_group_catalog_and_partition_progress_are_raft_persisted() {
    let (mut storage, _dir) = new_storage(0);
    let operation_id = uuid::Uuid::new_v4();
    let first_id = uuid::Uuid::new_v4();
    let blocked_same_instance_id = uuid::Uuid::new_v4();
    let other_id = uuid::Uuid::new_v4();
    let responses = storage
        .apply(vec![
            aggregate_fence_entry(0, 1),
            request_entry(
                1,
                crate::EsRequest::CommitAggregateGroupCatalog {
                    command: es_core::AggregateGroupCatalogCommand::Create {
                        definition: aggregate_group_definition(operation_id),
                        partition_count: es_core::EVENT_PARTITION_COUNT,
                    },
                },
            ),
            aggregate_group_entry(
                2,
                crate::AggregateGroupPartitionCommand::Claim {
                    consumer_id: "consumer-a".into(),
                    now_ms: 10,
                    deadline_ms: 20,
                    max_claim: 8,
                    max_bytes: 1024,
                    candidates: vec![
                        aggregate_group_candidate(first_id, 0, "order-1"),
                        aggregate_group_candidate(blocked_same_instance_id, 1, "order-1"),
                        aggregate_group_candidate(other_id, 2, "order-2"),
                    ],
                },
            ),
        ])
        .await
        .expect("应用组创建与 claim");
    assert!(matches!(
        &responses[1],
        crate::EsResponse::AggregateGroupCatalogApplied(es_core::AggregateGroupCatalogApply {
            outcome: es_core::AggregateGroupCatalogOutcome::Group(definition),
            ..
        }) if definition.revision == 1 && definition.epoch == 1
    ));
    assert!(matches!(
        &responses[2],
        crate::EsResponse::AggregateGroupClaimed(deliveries)
            if deliveries.len() == 2
                && deliveries.iter().all(|delivery| delivery.delivery_id != blocked_same_instance_id)
    ));

    storage
        .apply(vec![aggregate_group_entry(
            3,
            crate::AggregateGroupPartitionCommand::Settle {
                consumer_id: "consumer-a".into(),
                now_ms: 11,
                settlements: vec![es_core::AggregateSettlement {
                    delivery_id: other_id,
                    action: es_core::AggregateSettlementAction::Ack,
                    reason: String::new(),
                }],
            },
        )])
        .await
        .expect("乱序 Ack");
    let progress = storage
        .read_aggregate_group_partition(&aggregate_aggregate_type(), 7, "workers")
        .expect("读取组分区")
        .expect("组分区已惰性创建");
    assert_eq!(progress.next_position, 0);
    assert_eq!(
        progress.resolved_gaps,
        std::collections::BTreeSet::from([2])
    );

    storage
        .apply(vec![aggregate_group_entry(
            4,
            crate::AggregateGroupPartitionCommand::Settle {
                consumer_id: "consumer-a".into(),
                now_ms: 12,
                settlements: vec![es_core::AggregateSettlement {
                    delivery_id: first_id,
                    action: es_core::AggregateSettlementAction::Ack,
                    reason: String::new(),
                }],
            },
        )])
        .await
        .expect("补齐低位 Ack");
    let progress = storage
        .read_aggregate_group_partition(&aggregate_aggregate_type(), 7, "workers")
        .unwrap()
        .unwrap();
    assert_eq!(
        progress.next_position, 1,
        "位置 1 尚未投递，checkpoint 不能越过"
    );
    assert_eq!(
        progress.resolved_gaps,
        std::collections::BTreeSet::from([2])
    );
    assert_eq!(
        storage
            .read_aggregate_group_catalog()
            .expect("读取组 catalog")
            .groups
            .len(),
        1
    );
}

#[tokio::test]
async fn aggregate_append_broadcasts_only_after_commit() {
    let (mut storage, _dir) = new_storage(0);
    let mut receiver = storage.subscribe_aggregate_events();
    storage
        .apply(vec![
            aggregate_fence_entry(0, 1),
            aggregate_append_entry(
                1,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"created"),
                1,
            ),
        ])
        .await
        .expect("追加聚合事件");
    let event = receiver.recv().await.expect("接收聚合事件");
    assert_eq!(event.aggregate_id, "order-1");
    assert_eq!(event.aggregate_version, 0);
    assert_eq!(event.partition_position, 0);
}

#[tokio::test]
async fn aggregate_data_and_idempotency_survive_storage_reopen() {
    let dir = tempfile::tempdir().expect("建临时目录");
    let path = dir.path().to_path_buf();
    let snapshots = path.join("snapshots");
    let event_id = uuid::Uuid::new_v4();
    let event = aggregate_event(event_id, b"created");
    let tree = std::sync::Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(path.clone())
            .build()
            .expect("打开 tree"),
    );
    let mut storage = crate::EsStorage::new(
        0,
        tree,
        crate::snapshot::SnapshotConfig {
            dir: snapshots.clone(),
            ..Default::default()
        },
    )
    .expect("创建存储");
    storage
        .apply(vec![
            aggregate_fence_entry(0, 1),
            aggregate_append_entry(
                1,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                event.clone(),
                1,
            ),
            aggregate_state_entry(
                2,
                "order-1",
                es_core::ExpectedStateRevision::Absent,
                b"{\"balance\":100}",
            ),
        ])
        .await
        .expect("写入聚合数据");
    storage.close().await.expect("关闭存储");
    drop(storage);

    let tree = std::sync::Arc::new(
        surrealkv::TreeBuilder::new()
            .with_path(path)
            .build()
            .expect("重开 tree"),
    );
    let mut reopened = crate::EsStorage::new(
        0,
        tree,
        crate::snapshot::SnapshotConfig {
            dir: snapshots,
            ..Default::default()
        },
    )
    .expect("重建存储");
    assert_eq!(
        reopened
            .read_aggregate_meta(&aggregate_aggregate_type(), 7, "order-1")
            .unwrap(),
        Some(es_core::AggregateMeta { current_version: 0 })
    );
    assert_eq!(
        reopened
            .read_aggregate_state(&aggregate_aggregate_type(), 7, "order-1")
            .unwrap()
            .unwrap()
            .revision,
        0
    );
    assert_eq!(
        reopened
            .read_aggregate_state_document(&aggregate_aggregate_type(), 7, "order-1")
            .unwrap()
            .unwrap()
            .modified_hlc,
        hlc(3_002)
    );

    let responses = reopened
        .apply(vec![
            aggregate_append_entry(
                3,
                "order-1",
                es_core::ExpectedAggregateVersion::NoAggregate,
                event,
                1,
            ),
            aggregate_append_entry(
                4,
                "order-2",
                es_core::ExpectedAggregateVersion::NoAggregate,
                aggregate_event(uuid::Uuid::new_v4(), b"second"),
                1,
            ),
        ])
        .await
        .expect("重开后重试并追加");
    assert!(matches!(
        responses[0],
        crate::EsResponse::AggregateAppendOk {
            aggregate_version: 0,
            partition_position: 0
        }
    ));
    assert!(matches!(
        responses[1],
        crate::EsResponse::AggregateAppendOk {
            aggregate_version: 0,
            partition_position: 1
        }
    ));
    reopened.close().await.expect("关闭重开存储");
}

#[tokio::test]
async fn snapshot_roundtrip_preserves_aggregate_state_modified_time() {
    use openraft::RaftSnapshotBuilder;

    let (mut src, _src_dir) = new_storage(0);
    src.apply(vec![
        aggregate_fence_entry(0, 1),
        aggregate_append_entry(
            1,
            "order-1",
            es_core::ExpectedAggregateVersion::NoAggregate,
            aggregate_event(uuid::Uuid::new_v4(), b"created"),
            1,
        ),
        aggregate_state_entry(
            2,
            "order-1",
            es_core::ExpectedStateRevision::Absent,
            br#"{"balance":50}"#,
        ),
    ])
    .await
    .expect("写入聚合状态");
    let snapshot = src.build_snapshot().await.expect("构建快照");

    let (mut dst, _dst_dir) = new_storage(0);
    dst.install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .expect("安装快照");
    assert_eq!(
        dst.read_aggregate_state_document(&aggregate_aggregate_type(), 7, "order-1")
            .expect("读取快照状态")
            .expect("快照包含状态"),
        es_core::AggregateStateDocument {
            revision: 0,
            data: br#"{"balance":50}"#.to_vec(),
            modified_hlc: hlc(3_002),
        }
    );
}
