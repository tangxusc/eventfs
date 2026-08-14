//! `esctl persistent`：持久化订阅组管理与显式拉取/结算。

use anyhow::{Context, Result};
use es_proto::eventstore::*;

use crate::cli::*;
use crate::commands::Ctx;
use crate::output;

fn target(streams: &[String], all: bool) -> Option<PersistentSubscriptionTarget> {
    if all {
        Some(PersistentSubscriptionTarget {
            target: Some(persistent_subscription_target::Target::All(Empty {})),
        })
    } else if !streams.is_empty() {
        Some(PersistentSubscriptionTarget {
            target: Some(persistent_subscription_target::Target::Streams(
                SubscribeStreams {
                    stream_ids: streams.to_vec(),
                },
            )),
        })
    } else {
        None
    }
}

fn has_settings(settings: &PersistentSettingsArgs) -> bool {
    settings.max_unacked_per_consumer.is_some()
        || settings.max_unacked_per_group.is_some()
        || settings.ack_timeout_ms.is_some()
        || settings.max_retries.is_some()
        || settings.retry_min_ms.is_some()
        || settings.retry_max_ms.is_some()
}

fn create_settings(settings: &PersistentSettingsArgs) -> Option<PersistentSubscriptionSettings> {
    has_settings(settings).then(|| PersistentSubscriptionSettings {
        max_unacked_per_consumer: settings.max_unacked_per_consumer.unwrap_or(0),
        max_unacked_per_group: settings.max_unacked_per_group.unwrap_or(0),
        ack_timeout_ms: settings.ack_timeout_ms.unwrap_or(0),
        max_retries: settings.max_retries.unwrap_or(0),
        retry_min_ms: settings.retry_min_ms.unwrap_or(0),
        retry_max_ms: settings.retry_max_ms.unwrap_or(0),
    })
}

fn merged_settings(
    current: PersistentSubscriptionSettings,
    settings: &PersistentSettingsArgs,
) -> Option<PersistentSubscriptionSettings> {
    has_settings(settings).then(|| PersistentSubscriptionSettings {
        max_unacked_per_consumer: settings
            .max_unacked_per_consumer
            .unwrap_or(current.max_unacked_per_consumer),
        max_unacked_per_group: settings
            .max_unacked_per_group
            .unwrap_or(current.max_unacked_per_group),
        ack_timeout_ms: settings.ack_timeout_ms.unwrap_or(current.ack_timeout_ms),
        max_retries: settings.max_retries.unwrap_or(current.max_retries),
        retry_min_ms: settings.retry_min_ms.unwrap_or(current.retry_min_ms),
        retry_max_ms: settings.retry_max_ms.unwrap_or(current.retry_max_ms),
    })
}

fn target_text(target: Option<&PersistentSubscriptionTarget>) -> String {
    match target.and_then(|target| target.target.as_ref()) {
        Some(persistent_subscription_target::Target::All(_)) => "$all".into(),
        Some(persistent_subscription_target::Target::Streams(streams)) => {
            streams.stream_ids.join(",")
        }
        None => "-".into(),
    }
}

fn info_json(info: &PersistentSubscriptionInfo) -> serde_json::Value {
    serde_json::json!({
        "name": info.name,
        "revision": info.revision,
        "epoch": info.epoch,
        "target": target_text(info.target.as_ref()),
        "stream_count": info.stream_count,
        "active_delivery_count": info.active_delivery_count,
        "parked_count": info.parked_count,
        "settings": info.settings.as_ref().map(|settings| serde_json::json!({
            "max_unacked_per_consumer": settings.max_unacked_per_consumer,
            "max_unacked_per_group": settings.max_unacked_per_group,
            "ack_timeout_ms": settings.ack_timeout_ms,
            "max_retries": settings.max_retries,
            "retry_min_ms": settings.retry_min_ms,
            "retry_max_ms": settings.retry_max_ms,
        })),
    })
}

fn render_infos(format: Format, infos: &[PersistentSubscriptionInfo]) -> String {
    match format {
        Format::Json => serde_json::json!({
            "subscriptions": infos.iter().map(info_json).collect::<Vec<_>>()
        })
        .to_string(),
        Format::Table => {
            let rows = infos
                .iter()
                .map(|info| {
                    vec![
                        info.name.clone(),
                        info.revision.to_string(),
                        info.epoch.to_string(),
                        target_text(info.target.as_ref()),
                        info.active_delivery_count.to_string(),
                        info.parked_count.to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            output::render_table(
                &["NAME", "REV", "EPOCH", "TARGET", "UNACKED", "PARKED"],
                &rows,
            )
        }
        Format::Simple => infos
            .iter()
            .map(|info| {
                format!(
                    "name: {}\nrevision: {}\nepoch: {}\ntarget: {}\nunacked: {}\nparked: {}",
                    info.name,
                    info.revision,
                    info.epoch,
                    target_text(info.target.as_ref()),
                    info.active_delivery_count,
                    info.parked_count
                )
            })
            .collect::<Vec<_>>()
            .join("\n\n"),
    }
}

fn delivery_json(delivery: &PersistentDelivery) -> serde_json::Value {
    serde_json::json!({
        "delivery_id": output::event_id_text(&delivery.delivery_id),
        "group_epoch": delivery.group_epoch,
        "attempt": delivery.attempt,
        "lease_deadline_ms": delivery.lease_deadline_ms,
        "replayed": delivery.replayed,
        "event": delivery.event.as_ref().map(output::subscription_event_to_json),
    })
}

fn render_fetch(format: Format, response: &FetchPersistentSubscriptionResponse) -> String {
    match format {
        Format::Json => serde_json::json!({
            "deliveries": response.deliveries.iter().map(delivery_json).collect::<Vec<_>>(),
            "caught_up": response.caught_up,
            "throttled": response.throttled,
            "retry_after_ms": response.retry_after_ms,
        })
        .to_string(),
        Format::Table => {
            let rows = response
                .deliveries
                .iter()
                .map(|delivery| {
                    let event = delivery.event.as_ref();
                    vec![
                        output::event_id_text(&delivery.delivery_id),
                        delivery.group_epoch.to_string(),
                        event
                            .map(|event| event.stream_id.clone())
                            .unwrap_or_default(),
                        event
                            .map(|event| event.version.to_string())
                            .unwrap_or_default(),
                        delivery.attempt.to_string(),
                        delivery.replayed.to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            output::render_table(
                &["DELIVERY", "EPOCH", "STREAM", "VER", "ATTEMPT", "REPLAYED"],
                &rows,
            )
        }
        Format::Simple => {
            if response.deliveries.is_empty() {
                return format!(
                    "deliveries: 0\ncaught_up: {}\nthrottled: {}\nretry_after_ms: {}",
                    response.caught_up, response.throttled, response.retry_after_ms
                );
            }
            response
                .deliveries
                .iter()
                .map(|delivery| {
                    let event = delivery.event.as_ref();
                    format!(
                        "delivery: {} epoch={} stream={} version={} attempt={} replayed={}",
                        output::event_id_text(&delivery.delivery_id),
                        delivery.group_epoch,
                        event.map(|event| event.stream_id.as_str()).unwrap_or("-"),
                        event
                            .map(|event| event.version.to_string())
                            .unwrap_or_else(|| "-".into()),
                        delivery.attempt,
                        delivery.replayed
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
    }
}

async fn create(ctx: &Ctx, args: &PersistentCreateArgs) -> Result<()> {
    let next_versions = args
        .next_versions
        .iter()
        .map(|item| (item.stream.clone(), item.version))
        .collect();
    let request = CreatePersistentSubscriptionRequest {
        name: args.name.clone(),
        target: target(&args.stream, args.all),
        start: Some(PersistentStartSpec {
            default: if args.now {
                PersistentStartDefault::PersistentStartNow as i32
            } else {
                PersistentStartDefault::PersistentStartBeginning as i32
            },
            next_versions,
        }),
        settings: create_settings(&args.settings),
    };
    let info = ctx
        .cluster
        .with_persistent_leader(|mut client| {
            let request = request.clone();
            async move {
                client
                    .create_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await?;
    println!("{}", render_infos(ctx.format, &[info]));
    Ok(())
}

async fn update(ctx: &Ctx, args: &PersistentUpdateArgs) -> Result<()> {
    let current = ctx
        .cluster
        .with_persistent_leader(|mut client| {
            let name = args.name.clone();
            async move {
                client
                    .get_persistent_subscription(GetPersistentSubscriptionRequest { name })
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await?;
    let resets = args
        .reset
        .iter()
        .map(|reset| {
            let start = match reset.start.as_str() {
                "beginning" => persistent_stream_reset::Start::Beginning(Empty {}),
                "now" => persistent_stream_reset::Start::Now(Empty {}),
                version => persistent_stream_reset::Start::NextVersion(
                    version.parse().expect("clap 已校验 reset version"),
                ),
            };
            PersistentStreamReset {
                stream_id: reset.stream.clone(),
                start: Some(start),
            }
        })
        .collect();
    let request = UpdatePersistentSubscriptionRequest {
        name: args.name.clone(),
        expected_revision: args.expected_revision,
        target: target(&args.stream, args.all),
        settings: merged_settings(current.settings.unwrap_or_default(), &args.settings),
        resets,
    };
    let info = ctx
        .cluster
        .with_persistent_leader(|mut client| {
            let request = request.clone();
            async move {
                client
                    .update_persistent_subscription(request)
                    .await
                    .map(|response| response.into_inner())
            }
        })
        .await?;
    println!("{}", render_infos(ctx.format, &[info]));
    Ok(())
}

/// 执行持久化订阅子命令。
pub async fn run(ctx: &Ctx, action: &PersistentAction) -> Result<()> {
    match action {
        PersistentAction::Create(args) => create(ctx, args).await,
        PersistentAction::Update(args) => update(ctx, args).await,
        PersistentAction::Delete(args) => {
            let request = DeletePersistentSubscriptionRequest {
                name: args.name.clone(),
                expected_revision: args.expected_revision,
            };
            ctx.cluster
                .with_persistent_leader(|mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .delete_persistent_subscription(request)
                            .await
                            .map(|_| ())
                    }
                })
                .await?;
            println!("deleted: {}", args.name);
            Ok(())
        }
        PersistentAction::Get(args) => {
            let info = ctx
                .cluster
                .with_persistent_leader(|mut client| {
                    let name = args.name.clone();
                    async move {
                        client
                            .get_persistent_subscription(GetPersistentSubscriptionRequest { name })
                            .await
                            .map(|response| response.into_inner())
                    }
                })
                .await?;
            println!("{}", render_infos(ctx.format, &[info]));
            Ok(())
        }
        PersistentAction::List => {
            let infos = ctx
                .cluster
                .with_persistent_leader(|mut client| async move {
                    client
                        .list_persistent_subscriptions(ListPersistentSubscriptionsRequest {})
                        .await
                        .map(|response| response.into_inner().subscriptions)
                })
                .await?;
            println!("{}", render_infos(ctx.format, &infos));
            Ok(())
        }
        PersistentAction::Fetch(args) => {
            let request = FetchPersistentSubscriptionRequest {
                name: args.name.clone(),
                consumer_id: args.consumer.clone(),
                max_events: args.max_events,
                max_bytes: args.max_bytes,
                wait_ms: args.wait_ms,
            };
            let response = ctx
                .cluster
                .with_persistent_leader(|mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .fetch_persistent_subscription(request)
                            .await
                            .map(|response| response.into_inner())
                    }
                })
                .await?;
            println!("{}", render_fetch(ctx.format, &response));
            Ok(())
        }
        PersistentAction::Settle(args) => {
            let delivery_id = uuid::Uuid::parse_str(&args.delivery)
                .with_context(|| format!("delivery 不是 UUID: {}", args.delivery))?;
            let action = match args.action {
                PersistentActionArg::Ack => PersistentSettlementAction::PersistentSettlementAck,
                PersistentActionArg::Retry => PersistentSettlementAction::PersistentSettlementRetry,
                PersistentActionArg::Park => PersistentSettlementAction::PersistentSettlementPark,
                PersistentActionArg::Skip => PersistentSettlementAction::PersistentSettlementSkip,
            };
            let request = SettlePersistentSubscriptionRequest {
                name: args.name.clone(),
                consumer_id: args.consumer.clone(),
                group_epoch: args.epoch,
                settlements: vec![PersistentSettlement {
                    delivery_id: delivery_id.as_bytes().to_vec(),
                    action: action as i32,
                    reason: args.reason.clone(),
                }],
            };
            let response = ctx
                .cluster
                .with_persistent_leader(|mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .settle_persistent_subscription(request)
                            .await
                            .map(|response| response.into_inner())
                    }
                })
                .await?;
            let result = response.results.first().context("服务端未返回结算结果")?;
            let status = PersistentSettlementStatus::try_from(result.status)
                .map_err(|_| anyhow::anyhow!("服务端返回未知结算状态"))?;
            println!(
                "delivery: {}\nstatus: {}",
                args.delivery,
                status.as_str_name()
            );
            Ok(())
        }
        PersistentAction::Parked(args) => {
            let request = ListParkedPersistentSubscriptionRequest {
                name: args.name.clone(),
                offset: args.offset,
                limit: args.limit,
            };
            let response = ctx
                .cluster
                .with_persistent_leader(|mut client| {
                    let request = request.clone();
                    async move {
                        client
                            .list_parked_persistent_subscription(request)
                            .await
                            .map(|response| response.into_inner())
                    }
                })
                .await?;
            let rows = response
                .events
                .iter()
                .map(|event| {
                    serde_json::json!({
                        "parked_id": output::event_id_text(&event.parked_id),
                        "attempts": event.attempts,
                        "reason": event.reason,
                        "parked_at_ms": event.parked_at_ms,
                        "event": event.event.as_ref().map(output::subscription_event_to_json),
                    })
                })
                .collect::<Vec<_>>();
            println!(
                "{}",
                serde_json::json!({"events": rows, "next_offset": response.next_offset})
            );
            Ok(())
        }
        PersistentAction::Replay(args) => {
            let count = ctx
                .cluster
                .with_persistent_leader(|mut client| {
                    let name = args.name.clone();
                    async move {
                        client
                            .replay_parked_persistent_subscription(
                                ReplayParkedPersistentSubscriptionRequest { name },
                            )
                            .await
                            .map(|response| response.into_inner().replayed_count)
                    }
                })
                .await?;
            println!("replayed: {count}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_settings_uses_zero_as_server_default() {
        let settings = PersistentSettingsArgs {
            ack_timeout_ms: Some(25),
            ..Default::default()
        };
        let parsed = create_settings(&settings).expect("包含设置");
        assert_eq!(parsed.ack_timeout_ms, 25);
        assert_eq!(parsed.max_unacked_per_consumer, 0);
    }

    #[test]
    fn update_settings_preserves_unspecified_fields() {
        let current = PersistentSubscriptionSettings {
            max_unacked_per_consumer: 10,
            max_unacked_per_group: 20,
            ack_timeout_ms: 30,
            max_retries: 4,
            retry_min_ms: 5,
            retry_max_ms: 6,
        };
        let merged = merged_settings(
            current,
            &PersistentSettingsArgs {
                max_retries: Some(9),
                ..Default::default()
            },
        )
        .expect("包含更新");
        assert_eq!(merged.max_unacked_per_consumer, 10);
        assert_eq!(merged.max_retries, 9);
    }

    #[test]
    fn optional_target_and_settings_cover_all_forms() {
        assert!(target(&[], false).is_none());
        assert!(matches!(
            target(&[], true).unwrap().target,
            Some(persistent_subscription_target::Target::All(_))
        ));
        assert!(matches!(
            target(&["a".into(), "b".into()], false)
                .unwrap()
                .target,
            Some(persistent_subscription_target::Target::Streams(SubscribeStreams {
                stream_ids
            })) if stream_ids == ["a", "b"]
        ));
        assert!(create_settings(&PersistentSettingsArgs::default()).is_none());

        let settings = PersistentSettingsArgs {
            max_unacked_per_consumer: Some(1),
            max_unacked_per_group: Some(2),
            ack_timeout_ms: Some(3),
            max_retries: Some(4),
            retry_min_ms: Some(5),
            retry_max_ms: Some(6),
        };
        let parsed = create_settings(&settings).unwrap();
        assert_eq!(
            (
                parsed.max_unacked_per_consumer,
                parsed.max_unacked_per_group,
                parsed.ack_timeout_ms,
                parsed.max_retries,
                parsed.retry_min_ms,
                parsed.retry_max_ms,
            ),
            (1, 2, 3, 4, 5, 6)
        );

        assert!(create_settings(&PersistentSettingsArgs {
            max_unacked_per_group: Some(2),
            ..Default::default()
        })
        .is_some());
        assert!(create_settings(&PersistentSettingsArgs {
            retry_min_ms: Some(5),
            ..Default::default()
        })
        .is_some());
        assert!(create_settings(&PersistentSettingsArgs {
            retry_max_ms: Some(6),
            ..Default::default()
        })
        .is_some());
    }

    fn sample_info(target: Option<PersistentSubscriptionTarget>) -> PersistentSubscriptionInfo {
        PersistentSubscriptionInfo {
            name: "workers".into(),
            revision: 2,
            epoch: 3,
            target,
            settings: Some(PersistentSubscriptionSettings::default()),
            stream_count: 1,
            active_delivery_count: 4,
            parked_count: 5,
        }
    }

    #[test]
    fn subscription_info_renders_all_formats_and_targets() {
        let all = sample_info(target(&[], true));
        let streams = sample_info(target(&["a".into(), "b".into()], false));
        let missing = sample_info(None);

        assert!(render_infos(Format::Json, std::slice::from_ref(&all)).contains("$all"));
        assert!(render_infos(Format::Table, std::slice::from_ref(&streams)).contains("a,b"));
        assert!(render_infos(Format::Simple, &[missing]).contains("target: -"));
    }

    #[test]
    fn fetch_renders_empty_and_eventless_deliveries() {
        let empty = FetchPersistentSubscriptionResponse {
            deliveries: vec![],
            caught_up: true,
            throttled: false,
            retry_after_ms: 7,
        };
        assert!(render_fetch(Format::Simple, &empty).contains("deliveries: 0"));

        let response = FetchPersistentSubscriptionResponse {
            deliveries: vec![PersistentDelivery {
                delivery_id: vec![0; 16],
                event: None,
                attempt: 1,
                lease_deadline_ms: 2,
                group_epoch: 3,
                replayed: false,
            }],
            caught_up: false,
            throttled: false,
            retry_after_ms: 0,
        };
        assert!(render_fetch(Format::Json, &response).contains("delivery_id"));
        assert!(render_fetch(Format::Table, &response).contains("ATTEMPT"));
        assert!(render_fetch(Format::Simple, &response).contains("stream=-"));
    }
}
