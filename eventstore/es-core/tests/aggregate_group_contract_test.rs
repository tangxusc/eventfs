use es_core::{
    AggregateDeliveryCandidate, AggregateGroupDelivery, AggregateGroupPartition,
    AggregateGroupRetry, AggregateGroupSettings, AggregateInstanceLease, AggregateSettlement,
    AggregateSettlementAction, AggregateSettlementResult,
};
use uuid::Uuid;

fn candidate(position: u64, aggregate_id: &str) -> AggregateDeliveryCandidate {
    AggregateDeliveryCandidate {
        delivery_id: Uuid::new_v4(),
        partition_position: position,
        aggregate_id: aggregate_id.into(),
        aggregate_version: position,
        event_id: Uuid::new_v4(),
        payload_bytes: 1,
        replayed: false,
    }
}

fn delivery(candidate: &AggregateDeliveryCandidate, consumer_id: &str) -> AggregateGroupDelivery {
    AggregateGroupDelivery {
        delivery_id: candidate.delivery_id,
        consumer_id: consumer_id.into(),
        partition_position: candidate.partition_position,
        aggregate_id: candidate.aggregate_id.clone(),
        aggregate_version: candidate.aggregate_version,
        event_id: candidate.event_id,
        attempt: 0,
        deadline_ms: 1_000,
        group_epoch: 1,
        replayed: false,
    }
}

fn claim(
    partition: &mut AggregateGroupPartition,
    settings: &AggregateGroupSettings,
    candidates: Vec<AggregateDeliveryCandidate>,
) -> Vec<AggregateGroupDelivery> {
    partition.claim("consumer-a", 10, 100, settings, 10, 1_024, candidates)
}

#[test]
fn settings_and_claim_reject_each_invalid_boundary() {
    let valid = AggregateGroupSettings::default();
    valid.validate().unwrap();

    let mut invalid = valid.clone();
    invalid.max_unacked_per_consumer = 0;
    assert!(invalid.validate().is_err());
    invalid = valid.clone();
    invalid.max_unacked_per_group = 0;
    assert!(invalid.validate().is_err());
    invalid = valid.clone();
    invalid.max_unacked_per_consumer = invalid.max_unacked_per_group + 1;
    assert!(invalid.validate().is_err());
    invalid = valid.clone();
    invalid.ack_timeout_ms = 0;
    assert!(invalid.validate().is_err());
    invalid = valid.clone();
    invalid.retry_min_ms = 0;
    assert!(invalid.validate().is_err());
    invalid = valid.clone();
    invalid.retry_min_ms = invalid.retry_max_ms + 1;
    assert!(invalid.validate().is_err());

    let mut partition = AggregateGroupPartition::new(1, 0);
    assert!(
        partition
            .claim("", 10, 100, &valid, 1, 1, Vec::new())
            .is_empty()
    );
    assert!(
        partition
            .claim("consumer-a", 10, 10, &valid, 1, 1, Vec::new())
            .is_empty()
    );
    assert!(
        partition
            .claim("consumer-a", 10, 100, &valid, 0, 1, Vec::new())
            .is_empty()
    );
    assert!(
        partition
            .claim("consumer-a", 10, 100, &valid, 1, 0, Vec::new())
            .is_empty()
    );
    assert!(
        partition
            .claim("consumer-a", 10, 100, &invalid, 1, 1, Vec::new())
            .is_empty()
    );
}

#[test]
fn claim_filters_duplicates_retries_leases_and_old_positions() {
    let settings = AggregateGroupSettings::default();

    let old = candidate(4, "old");
    let mut partition = AggregateGroupPartition::new(1, 5);
    assert!(claim(&mut partition, &settings, vec![old]).is_empty());

    let resolved = candidate(0, "resolved");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition.resolved_gaps.insert(0);
    assert!(claim(&mut partition, &settings, vec![resolved]).is_empty());

    let duplicate_id = candidate(0, "duplicate-id");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition.deliveries.insert(
        duplicate_id.delivery_id,
        delivery(&duplicate_id, "consumer-a"),
    );
    assert!(claim(&mut partition, &settings, vec![duplicate_id]).is_empty());

    let existing = candidate(0, "existing-position");
    let duplicate_position = candidate(0, "other-instance");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition
        .deliveries
        .insert(existing.delivery_id, delivery(&existing, "consumer-a"));
    assert!(claim(&mut partition, &settings, vec![duplicate_position]).is_empty());

    let retry = candidate(0, "retry");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition.pending_retries.insert(
        0,
        AggregateGroupRetry {
            partition_position: 0,
            aggregate_id: retry.aggregate_id.clone(),
            aggregate_version: retry.aggregate_version,
            event_id: retry.event_id,
            attempt: 1,
            not_before_ms: 11,
            replayed: true,
        },
    );
    assert!(claim(&mut partition, &settings, vec![retry]).is_empty());

    let mismatched_retry = candidate(0, "retry-mismatch");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition.pending_retries.insert(
        0,
        AggregateGroupRetry {
            partition_position: 0,
            aggregate_id: mismatched_retry.aggregate_id.clone(),
            aggregate_version: 0,
            event_id: Uuid::new_v4(),
            attempt: 1,
            not_before_ms: 0,
            replayed: true,
        },
    );
    assert!(claim(&mut partition, &settings, vec![mismatched_retry]).is_empty());

    let mut replay_without_retry = candidate(0, "replay");
    replay_without_retry.replayed = true;
    let mut partition = AggregateGroupPartition::new(1, 0);
    assert!(claim(&mut partition, &settings, vec![replay_without_retry]).is_empty());

    let leased = candidate(0, "leased");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition.pending_retries.insert(
        0,
        AggregateGroupRetry {
            partition_position: 0,
            aggregate_id: leased.aggregate_id.clone(),
            aggregate_version: leased.aggregate_version,
            event_id: leased.event_id,
            attempt: 1,
            not_before_ms: 0,
            replayed: false,
        },
    );
    partition.leases.insert(
        leased.aggregate_id.clone(),
        AggregateInstanceLease {
            consumer_id: "consumer-b".into(),
            group_epoch: 1,
            deadline_ms: 100,
        },
    );
    assert!(claim(&mut partition, &settings, vec![leased]).is_empty());
}

#[test]
fn claim_enforces_instance_order_credit_and_batch_bytes() {
    let settings = AggregateGroupSettings {
        max_unacked_per_consumer: 1,
        max_unacked_per_group: 1,
        ..AggregateGroupSettings::default()
    };
    let existing = candidate(0, "existing");
    let next = candidate(1, "next");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition
        .deliveries
        .insert(existing.delivery_id, delivery(&existing, "consumer-a"));
    assert!(claim(&mut partition, &settings, vec![next]).is_empty());

    let active = candidate(0, "same-instance");
    let blocked = candidate(1, "same-instance");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition
        .deliveries
        .insert(active.delivery_id, delivery(&active, "consumer-b"));
    assert!(
        claim(
            &mut partition,
            &AggregateGroupSettings::default(),
            vec![blocked]
        )
        .is_empty()
    );

    let pending = candidate(1, "pending-instance");
    let blocked = candidate(0, "pending-instance");
    let mut partition = AggregateGroupPartition::new(1, 0);
    partition.pending_retries.insert(
        1,
        AggregateGroupRetry {
            partition_position: 1,
            aggregate_id: pending.aggregate_id,
            aggregate_version: pending.aggregate_version,
            event_id: pending.event_id,
            attempt: 1,
            not_before_ms: 100,
            replayed: false,
        },
    );
    assert!(
        claim(
            &mut partition,
            &AggregateGroupSettings::default(),
            vec![blocked]
        )
        .is_empty()
    );

    let first = candidate(0, "first");
    let mut second = candidate(1, "second");
    second.payload_bytes = 10;
    let mut partition = AggregateGroupPartition::new(1, 0);
    let claimed = partition.claim(
        "consumer-a",
        10,
        100,
        &AggregateGroupSettings::default(),
        10,
        5,
        vec![first, second],
    );
    assert_eq!(claimed.len(), 1);
}

#[test]
fn settle_and_renew_cover_stale_missing_wrong_and_lease_free_deliveries() {
    let settings = AggregateGroupSettings::default();
    let missing = Uuid::new_v4();
    let mut partition = AggregateGroupPartition::new(1, 5);
    assert_eq!(
        partition.settle(
            "consumer-a",
            2,
            10,
            &settings,
            &[AggregateSettlement {
                delivery_id: missing,
                action: AggregateSettlementAction::Ack,
                reason: String::new(),
            }],
        ),
        vec![AggregateSettlementResult::StaleLease]
    );
    assert_eq!(
        partition.settle(
            "consumer-a",
            1,
            10,
            &settings,
            &[AggregateSettlement {
                delivery_id: missing,
                action: AggregateSettlementAction::Ack,
                reason: String::new(),
            }],
        ),
        vec![AggregateSettlementResult::AlreadySettled]
    );

    let wrong = candidate(5, "wrong-consumer");
    partition
        .deliveries
        .insert(wrong.delivery_id, delivery(&wrong, "consumer-b"));
    assert_eq!(
        partition.settle(
            "consumer-a",
            1,
            10,
            &settings,
            &[AggregateSettlement {
                delivery_id: wrong.delivery_id,
                action: AggregateSettlementAction::Ack,
                reason: String::new(),
            }],
        ),
        vec![AggregateSettlementResult::WrongConsumer]
    );

    let old = candidate(4, "old-position");
    partition
        .deliveries
        .insert(old.delivery_id, delivery(&old, "consumer-a"));
    assert_eq!(
        partition.settle(
            "consumer-a",
            1,
            10,
            &settings,
            &[AggregateSettlement {
                delivery_id: old.delivery_id,
                action: AggregateSettlementAction::Ack,
                reason: String::new(),
            }],
        ),
        vec![AggregateSettlementResult::Applied]
    );

    assert_eq!(
        partition.renew("consumer-a", 2, 200, &[missing]),
        vec![AggregateSettlementResult::StaleLease]
    );
    assert_eq!(
        partition.renew("consumer-a", 1, 200, &[missing]),
        vec![AggregateSettlementResult::AlreadySettled]
    );

    let wrong = candidate(6, "renew-wrong");
    partition
        .deliveries
        .insert(wrong.delivery_id, delivery(&wrong, "consumer-b"));
    assert_eq!(
        partition.renew("consumer-a", 1, 200, &[wrong.delivery_id]),
        vec![AggregateSettlementResult::WrongConsumer]
    );

    let lease_free = candidate(7, "lease-free");
    partition
        .deliveries
        .insert(lease_free.delivery_id, delivery(&lease_free, "consumer-a"));
    assert_eq!(
        partition.renew("consumer-a", 1, 200, &[lease_free.delivery_id]),
        vec![AggregateSettlementResult::Applied]
    );

    let retry = candidate(8, "retry-to-park");
    partition
        .deliveries
        .insert(retry.delivery_id, delivery(&retry, "consumer-a"));
    let no_retries = AggregateGroupSettings {
        max_retries: 0,
        ..settings
    };
    assert_eq!(
        partition.settle(
            "consumer-a",
            1,
            10,
            &no_retries,
            &[AggregateSettlement {
                delivery_id: retry.delivery_id,
                action: AggregateSettlementAction::Retry,
                reason: "failed".into(),
            }],
        ),
        vec![AggregateSettlementResult::Applied]
    );
    assert!(partition.parked.contains_key(&retry.delivery_id));

    let retry = candidate(9, "retry-pending");
    partition
        .deliveries
        .insert(retry.delivery_id, delivery(&retry, "consumer-a"));
    assert_eq!(
        partition.settle(
            "consumer-a",
            1,
            10,
            &AggregateGroupSettings::default(),
            &[AggregateSettlement {
                delivery_id: retry.delivery_id,
                action: AggregateSettlementAction::Retry,
                reason: "transient".into(),
            }],
        ),
        vec![AggregateSettlementResult::Applied]
    );
    assert!(
        partition
            .pending_retries
            .contains_key(&retry.partition_position)
    );
}
