//! `esctl aggregate`：聚合事件集、实例事件、状态与诊断命令。

use anyhow::{Context, Result, anyhow, bail};
use es_client::AggregateStoreClient;
use es_proto::eventstore::*;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::cli::{
    AggregateAction, AggregateAppendArgs, AggregateCreateArgs, AggregateEventSetArgs,
    AggregateFollowArgs, AggregateGroupAction, AggregateGroupCreateArgs, AggregateGroupDeleteArgs,
    AggregateGroupFetchArgs, AggregateGroupSettingsArgs, AggregateGroupSettleArgs,
    AggregateGroupSettlementActionArg, AggregateGroupUpdateArgs, AggregateStateAction,
    AggregateStateGetArgs, AggregateStateListArgs, AggregateStatePutArgs,
    ExpectedAggregateVersionArg, Format,
};
use crate::commands::Ctx;
use crate::output;

fn event_set(business_space: &str, aggregate_type: &str) -> AggregateEventSetRef {
    AggregateEventSetRef {
        business_space: business_space.into(),
        aggregate_type: aggregate_type.into(),
    }
}

async fn connect(ctx: &Ctx) -> Result<AggregateStoreClient> {
    AggregateStoreClient::connect_with_tls(
        ctx.cluster.endpoints().to_vec(),
        ctx.cluster.tls().cloned(),
    )
    .await
    .map_err(anyhow::Error::new)
}

fn load_json(inline: &Option<String>, file: &Option<std::path::PathBuf>) -> Result<Vec<u8>> {
    let bytes = match (inline, file) {
        (Some(value), None) => value.as_bytes().to_vec(),
        (None, Some(path)) => std::fs::read(path)
            .with_context(|| format!("读取 JSON 文件 {} 失败", path.display()))?,
        _ => unreachable!("clap 参数组已保证二选一"),
    };
    serde_json::from_slice::<serde_json::Value>(&bytes).context("payload 必须是合法 JSON")?;
    Ok(bytes)
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        bail!("十六进制 token 长度必须为偶数");
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|_| anyhow!("token 包含非法十六进制字符"))
        })
        .collect()
}

fn expected_version(value: &ExpectedAggregateVersionArg) -> ExpectedAggregateVersion {
    let kind = match value {
        ExpectedAggregateVersionArg::Any => expected_aggregate_version::Kind::Any(Empty {}),
        ExpectedAggregateVersionArg::NoAggregate => {
            expected_aggregate_version::Kind::NoAggregate(Empty {})
        }
        ExpectedAggregateVersionArg::AggregateExists => {
            expected_aggregate_version::Kind::AggregateExists(Empty {})
        }
        ExpectedAggregateVersionArg::Exact(version) => {
            expected_aggregate_version::Kind::Exact(*version)
        }
    };
    ExpectedAggregateVersion { kind: Some(kind) }
}

fn json_bytes(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::json!(output::event_data_text(bytes)))
}

fn event_json(event: &AggregateEvent) -> serde_json::Value {
    serde_json::json!({
        "aggregate_id": event.aggregate_id,
        "aggregate_version": event.aggregate_version,
        "event_id": output::event_id_text(&event.event_id),
        "event_type": event.event_type,
        "data": json_bytes(&event.data),
        "metadata": json_bytes(&event.metadata),
        "hlc": {
            "wall": event.hlc.as_ref().map(|hlc| hlc.wall).unwrap_or(0),
            "logical": event.hlc.as_ref().map(|hlc| hlc.logical).unwrap_or(0),
        },
    })
}

fn event_set_json(info: &AggregateEventSetInfo) -> serde_json::Value {
    let identity = info.event_set.as_ref();
    serde_json::json!({
        "business_space": identity.map(|value| value.business_space.as_str()).unwrap_or(""),
        "aggregate_type": identity.map(|value| value.aggregate_type.as_str()).unwrap_or(""),
        "partition_count": info.partition_count,
        "hash_algorithm": info.hash_algorithm,
        "status": AggregateEventSetStatus::try_from(info.status)
            .map(|status| status.as_str_name())
            .unwrap_or("UNKNOWN"),
        "catalog_revision": info.catalog_revision,
    })
}

fn operation_id(value: &Option<String>) -> Result<Uuid> {
    value
        .as_deref()
        .map(Uuid::parse_str)
        .transpose()
        .context("operation ID 必须是 UUID")
        .map(|value| value.unwrap_or_else(Uuid::new_v4))
}

fn has_group_settings(value: &AggregateGroupSettingsArgs) -> bool {
    value.max_unacked_per_consumer.is_some()
        || value.max_unacked_per_group.is_some()
        || value.ack_timeout_ms.is_some()
        || value.max_retries.is_some()
        || value.retry_min_ms.is_some()
        || value.retry_max_ms.is_some()
}

fn merged_group_settings(
    value: &AggregateGroupSettingsArgs,
    current: Option<&AggregateGroupSettings>,
) -> Option<AggregateGroupSettings> {
    if !has_group_settings(value) {
        return None;
    }
    let defaults = es_core::AggregateGroupSettings::default();
    Some(AggregateGroupSettings {
        max_unacked_per_consumer: value.max_unacked_per_consumer.unwrap_or_else(|| {
            current
                .map(|settings| settings.max_unacked_per_consumer)
                .unwrap_or(defaults.max_unacked_per_consumer)
        }),
        max_unacked_per_group: value.max_unacked_per_group.unwrap_or_else(|| {
            current
                .map(|settings| settings.max_unacked_per_group)
                .unwrap_or(defaults.max_unacked_per_group)
        }),
        ack_timeout_ms: value.ack_timeout_ms.unwrap_or_else(|| {
            current
                .map(|settings| settings.ack_timeout_ms)
                .unwrap_or(defaults.ack_timeout_ms)
        }),
        max_retries: value.max_retries.unwrap_or_else(|| {
            current
                .map(|settings| settings.max_retries)
                .unwrap_or(defaults.max_retries)
        }),
        retry_min_ms: value.retry_min_ms.unwrap_or_else(|| {
            current
                .map(|settings| settings.retry_min_ms)
                .unwrap_or(defaults.retry_min_ms)
        }),
        retry_max_ms: value.retry_max_ms.unwrap_or_else(|| {
            current
                .map(|settings| settings.retry_max_ms)
                .unwrap_or(defaults.retry_max_ms)
        }),
    })
}

fn group_json(info: &AggregateGroupInfo) -> serde_json::Value {
    let identity = info.event_set.as_ref();
    let start = info
        .start
        .as_ref()
        .and_then(|start| start.kind.as_ref())
        .map(|kind| match kind {
            aggregate_group_start::Kind::Beginning(_) => "beginning",
            aggregate_group_start::Kind::Now(_) => "now",
        })
        .unwrap_or("unknown");
    serde_json::json!({
        "business_space": identity.map(|value| value.business_space.as_str()).unwrap_or(""),
        "aggregate_type": identity.map(|value| value.aggregate_type.as_str()).unwrap_or(""),
        "name": info.name,
        "revision": info.revision,
        "epoch": info.epoch,
        "start": start,
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

fn render_groups(format: Format, groups: &[AggregateGroupInfo]) -> String {
    match format {
        Format::Json => serde_json::json!({
            "groups": groups.iter().map(group_json).collect::<Vec<_>>()
        })
        .to_string(),
        Format::Table => output::render_table(
            &["SPACE", "TYPE", "GROUP", "REVISION", "EPOCH", "START"],
            &groups
                .iter()
                .map(|group| {
                    let value = group_json(group);
                    vec![
                        value["business_space"].as_str().unwrap_or("").into(),
                        value["aggregate_type"].as_str().unwrap_or("").into(),
                        group.name.clone(),
                        group.revision.to_string(),
                        group.epoch.to_string(),
                        value["start"].as_str().unwrap_or("unknown").into(),
                    ]
                })
                .collect::<Vec<_>>(),
        )
        .trim_end()
        .to_string(),
        Format::Simple => groups
            .iter()
            .map(|group| {
                let value = group_json(group);
                format!(
                    "{}/{}\t{}\trevision={}\tepoch={}\t{}",
                    value["business_space"].as_str().unwrap_or(""),
                    value["aggregate_type"].as_str().unwrap_or(""),
                    group.name,
                    group.revision,
                    group.epoch,
                    value["start"].as_str().unwrap_or("unknown")
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

async fn create(ctx: &Ctx, args: &AggregateCreateArgs) -> Result<()> {
    let operation_id = match &args.operation_id {
        Some(value) => {
            Uuid::parse_str(value).with_context(|| format!("非法 operation ID {value:?}"))?
        }
        None => Uuid::new_v4(),
    };
    let mut client = connect(ctx).await?;
    let info = client
        .create_event_set(CreateEventSetRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            operation_id: operation_id.as_bytes().to_vec(),
        })
        .await?;
    println!("{}", render_event_sets(ctx.format, &[info]));
    Ok(())
}

fn render_event_sets(format: Format, infos: &[AggregateEventSetInfo]) -> String {
    match format {
        Format::Json => serde_json::json!({
            "event_sets": infos.iter().map(event_set_json).collect::<Vec<_>>()
        })
        .to_string(),
        Format::Table => {
            let rows = infos
                .iter()
                .map(|info| {
                    let identity = info.event_set.as_ref();
                    vec![
                        identity
                            .map(|v| v.business_space.clone())
                            .unwrap_or_default(),
                        identity
                            .map(|v| v.aggregate_type.clone())
                            .unwrap_or_default(),
                        info.partition_count.to_string(),
                        AggregateEventSetStatus::try_from(info.status)
                            .map(|value| value.as_str_name().to_string())
                            .unwrap_or_else(|_| "UNKNOWN".into()),
                        info.catalog_revision.to_string(),
                    ]
                })
                .collect::<Vec<_>>();
            output::render_table(
                &["SPACE", "TYPE", "PARTITIONS", "STATUS", "CATALOG_REV"],
                &rows,
            )
            .trim_end()
            .to_string()
        }
        Format::Simple => infos
            .iter()
            .map(|info| {
                let identity = info.event_set.as_ref();
                format!(
                    "{}/{}\t{} partitions\t{}\trevision {}",
                    identity.map(|v| v.business_space.as_str()).unwrap_or(""),
                    identity.map(|v| v.aggregate_type.as_str()).unwrap_or(""),
                    info.partition_count,
                    AggregateEventSetStatus::try_from(info.status)
                        .map(|value| value.as_str_name())
                        .unwrap_or("UNKNOWN"),
                    info.catalog_revision
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

async fn append(ctx: &Ctx, args: &AggregateAppendArgs) -> Result<()> {
    let data = load_json(&args.data, &args.data_file)?;
    let metadata = args.metadata.as_deref().unwrap_or("{}").as_bytes().to_vec();
    serde_json::from_slice::<serde_json::Value>(&metadata).context("metadata 必须是合法 JSON")?;
    let event_id = match &args.event_id {
        Some(value) => {
            Uuid::parse_str(value).with_context(|| format!("非法 event ID {value:?}"))?
        }
        None => Uuid::new_v4(),
    };
    let mut client = connect(ctx).await?;
    let response = client
        .append(AppendAggregateEventRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            aggregate_id: args.aggregate_id.clone(),
            expected_version: Some(expected_version(&args.expected_version)),
            event: Some(NewAggregateEvent {
                event_id: event_id.as_bytes().to_vec(),
                event_type: args.event_type.clone(),
                data,
                metadata,
            }),
        })
        .await?;
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "aggregate_id": args.aggregate_id,
                "aggregate_version": response.aggregate_version,
                "event_id": event_id,
            })
        ),
        _ => println!(
            "OK {}/{} {} version={} event_id={event_id}",
            args.business_space, args.aggregate_type, args.aggregate_id, response.aggregate_version
        ),
    }
    Ok(())
}

async fn follow(ctx: &Ctx, args: &AggregateFollowArgs) -> Result<()> {
    let start = if let Some(cursor) = &args.cursor {
        aggregate_read_start::Kind::Cursor(decode_hex(cursor)?)
    } else if args.now {
        aggregate_read_start::Kind::Now(Empty {})
    } else {
        aggregate_read_start::Kind::Beginning(Empty {})
    };
    let mut client = connect(ctx).await?;
    let mut stream = client
        .follow(ReadAggregateEventsRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            start: Some(AggregateReadStart { kind: Some(start) }),
        })
        .await?;
    while let Some(frame) = stream.next().await {
        let frame = frame?;
        let cursor = output::hex(&frame.cursor);
        match frame.payload {
            Some(read_aggregate_events_response::Payload::Event(event)) => match ctx.format {
                Format::Json => println!(
                    "{}",
                    serde_json::json!({"type": "event", "event": event_json(&event), "cursor": cursor})
                ),
                _ => println!(
                    "{}\t{}\t[{}]\t{}",
                    event.aggregate_id,
                    event.aggregate_version,
                    event.event_type,
                    json_bytes(&event.data)
                ),
            },
            Some(read_aggregate_events_response::Payload::CaughtUp(_)) => {
                match ctx.format {
                    Format::Json => println!(
                        "{}",
                        serde_json::json!({"type": "caught_up", "cursor": cursor})
                    ),
                    _ => println!("[已追平，进入实时推送] cursor={cursor}"),
                }
                if args.once {
                    return Ok(());
                }
            }
            Some(read_aggregate_events_response::Payload::Degraded(value)) => {
                if args.once {
                    bail!(
                        "follow 已降级，{} 个来源不可用，无法确认完全追平",
                        value.unavailable_source_count
                    );
                }
                eprintln!(
                    "[follow 已降级，{} 个来源不可用，正在重试]",
                    value.unavailable_source_count
                );
            }
            Some(read_aggregate_events_response::Payload::Recovered(_)) => {
                eprintln!("[follow 已恢复]");
            }
            None => {}
        }
    }
    Ok(())
}

async fn state_list(ctx: &Ctx, args: &AggregateStateListArgs) -> Result<()> {
    let mut client = connect(ctx).await?;
    let response = client
        .list_states(ListAggregateStatesRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            page_size: args.page_size,
            page_token: args
                .page_token
                .as_deref()
                .map(decode_hex)
                .transpose()?
                .unwrap_or_default(),
        })
        .await?;
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "states": response.states.iter().map(|state| serde_json::json!({
                    "aggregate_id": state.aggregate_id,
                    "revision": state.revision,
                })).collect::<Vec<_>>(),
                "next_page_token": output::hex(&response.next_page_token),
            })
        ),
        Format::Table => println!(
            "{}",
            output::render_table(
                &["AGGREGATE_ID", "REVISION"],
                &response
                    .states
                    .iter()
                    .map(|state| vec![state.aggregate_id.clone(), state.revision.to_string()])
                    .collect::<Vec<_>>()
            )
            .trim_end()
        ),
        Format::Simple => {
            for state in response.states {
                println!("{}\t{}", state.aggregate_id, state.revision);
            }
            if !response.next_page_token.is_empty() {
                eprintln!("next_page_token={}", output::hex(&response.next_page_token));
            }
        }
    }
    Ok(())
}

async fn state_get(ctx: &Ctx, args: &AggregateStateGetArgs) -> Result<()> {
    let mut client = connect(ctx).await?;
    let response = client
        .get_state(GetAggregateStateRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            aggregate_id: args.aggregate_id.clone(),
        })
        .await?;
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "aggregate_id": args.aggregate_id,
                "revision": response.revision,
                "data": json_bytes(&response.data),
            })
        ),
        _ => println!(
            "revision={}\n{}",
            response.revision,
            String::from_utf8_lossy(&response.data)
        ),
    }
    Ok(())
}

async fn state_put(ctx: &Ctx, args: &AggregateStatePutArgs) -> Result<()> {
    let data = load_json(&args.data, &args.data_file)?;
    let kind = if args.expected_revision == "absent" {
        expected_state_revision::Kind::Absent(Empty {})
    } else {
        expected_state_revision::Kind::Exact(
            args.expected_revision
                .parse()
                .context("expected-revision 必须是 absent 或无符号整数")?,
        )
    };
    let mut client = connect(ctx).await?;
    let response = client
        .put_state(PutAggregateStateRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            aggregate_id: args.aggregate_id.clone(),
            expected_revision: Some(ExpectedStateRevision { kind: Some(kind) }),
            data,
        })
        .await?;
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({"aggregate_id": args.aggregate_id, "revision": response.revision})
        ),
        _ => println!("OK {} revision={}", args.aggregate_id, response.revision),
    }
    Ok(())
}

async fn get(ctx: &Ctx, args: &AggregateEventSetArgs) -> Result<()> {
    let mut client = connect(ctx).await?;
    let info = client
        .get_event_set(event_set(&args.business_space, &args.aggregate_type))
        .await?;
    println!("{}", render_event_sets(ctx.format, &[info]));
    Ok(())
}

async fn partitions(ctx: &Ctx, args: &AggregateEventSetArgs) -> Result<()> {
    let mut client = connect(ctx).await?;
    let partitions = client
        .list_partitions(event_set(&args.business_space, &args.aggregate_type))
        .await?;
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({"partitions": partitions.iter().map(|partition| serde_json::json!({
                "partition_id": partition.partition_id,
                "shard_id": partition.shard_id,
                "generation": partition.generation,
                "moving": partition.moving,
                "target_shard_id": partition.target_shard_id,
            })).collect::<Vec<_>>()})
        ),
        _ => println!(
            "{}",
            output::render_table(
                &["PARTITION", "SHARD", "GENERATION", "MOVING", "TARGET"],
                &partitions
                    .iter()
                    .map(|partition| vec![
                        partition.partition_id.to_string(),
                        partition.shard_id.to_string(),
                        partition.generation.to_string(),
                        partition.moving.to_string(),
                        partition.target_shard_id.to_string(),
                    ])
                    .collect::<Vec<_>>()
            )
            .trim_end()
        ),
    }
    Ok(())
}

async fn group_create(ctx: &Ctx, args: &AggregateGroupCreateArgs) -> Result<()> {
    let start = if args.now {
        aggregate_group_start::Kind::Now(Empty {})
    } else {
        aggregate_group_start::Kind::Beginning(Empty {})
    };
    let mut client = connect(ctx).await?;
    let group = client
        .create_group(CreateAggregateGroupRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            name: args.name.clone(),
            start: Some(AggregateGroupStart { kind: Some(start) }),
            settings: merged_group_settings(&args.settings, None),
            operation_id: operation_id(&args.operation_id)?.as_bytes().to_vec(),
        })
        .await?;
    println!("{}", render_groups(ctx.format, &[group]));
    Ok(())
}

async fn group_update(ctx: &Ctx, args: &AggregateGroupUpdateArgs) -> Result<()> {
    if !args.reset_beginning && !args.reset_now && !has_group_settings(&args.settings) {
        bail!("update 至少需要一个 settings 参数或 reset 参数");
    }
    let identity = event_set(&args.business_space, &args.aggregate_type);
    let mut client = connect(ctx).await?;
    let current = if has_group_settings(&args.settings) {
        Some(
            client
                .get_group(GetAggregateGroupRequest {
                    event_set: Some(identity.clone()),
                    name: args.name.clone(),
                })
                .await?,
        )
    } else {
        None
    };
    let start = if args.reset_now {
        Some(AggregateGroupStart {
            kind: Some(aggregate_group_start::Kind::Now(Empty {})),
        })
    } else if args.reset_beginning {
        Some(AggregateGroupStart {
            kind: Some(aggregate_group_start::Kind::Beginning(Empty {})),
        })
    } else {
        None
    };
    let group = client
        .update_group(UpdateAggregateGroupRequest {
            event_set: Some(identity),
            name: args.name.clone(),
            expected_revision: args.expected_revision,
            start,
            settings: merged_group_settings(
                &args.settings,
                current.as_ref().and_then(|group| group.settings.as_ref()),
            ),
            operation_id: operation_id(&args.operation_id)?.as_bytes().to_vec(),
        })
        .await?;
    println!("{}", render_groups(ctx.format, &[group]));
    Ok(())
}

async fn group_delete(ctx: &Ctx, args: &AggregateGroupDeleteArgs) -> Result<()> {
    connect(ctx)
        .await?
        .delete_group(DeleteAggregateGroupRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            name: args.name.clone(),
            expected_revision: args.expected_revision,
            operation_id: operation_id(&args.operation_id)?.as_bytes().to_vec(),
        })
        .await?;
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({"deleted": true, "name": args.name})
        ),
        _ => println!("OK deleted {}", args.name),
    }
    Ok(())
}

async fn group_list(ctx: &Ctx, args: &AggregateEventSetArgs) -> Result<()> {
    let groups = connect(ctx)
        .await?
        .list_groups(event_set(&args.business_space, &args.aggregate_type))
        .await?;
    println!("{}", render_groups(ctx.format, &groups));
    Ok(())
}

async fn group_fetch(ctx: &Ctx, args: &AggregateGroupFetchArgs) -> Result<()> {
    let response = connect(ctx)
        .await?
        .fetch_group(FetchAggregateGroupRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            name: args.name.clone(),
            consumer_id: args.consumer.clone(),
            max_events: args.max_events,
            max_bytes: args.max_bytes,
            wait_ms: args.wait_ms,
        })
        .await?;
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({
                "deliveries": response.deliveries.iter().map(|delivery| serde_json::json!({
                    "delivery_id": output::hex(&delivery.delivery_id),
                    "event": delivery.event.as_ref().map(event_json),
                    "attempt": delivery.attempt,
                    "deadline_ms": delivery.deadline_ms,
                    "replayed": delivery.replayed,
                })).collect::<Vec<_>>(),
                "caught_up": response.caught_up,
                "throttled": response.throttled,
            })
        ),
        Format::Table => println!(
            "{}",
            output::render_table(
                &["DELIVERY", "AGGREGATE", "VERSION", "ATTEMPT", "DEADLINE_MS"],
                &response
                    .deliveries
                    .iter()
                    .map(|delivery| {
                        let event = delivery.event.as_ref();
                        vec![
                            output::hex(&delivery.delivery_id),
                            event
                                .map(|event| event.aggregate_id.clone())
                                .unwrap_or_default(),
                            event
                                .map(|event| event.aggregate_version.to_string())
                                .unwrap_or_default(),
                            delivery.attempt.to_string(),
                            delivery.deadline_ms.to_string(),
                        ]
                    })
                    .collect::<Vec<_>>()
            )
            .trim_end()
        ),
        Format::Simple => {
            for delivery in response.deliveries {
                let event = delivery.event.as_ref();
                println!(
                    "{}\t{}\t{}\tattempt={}\t{}",
                    output::hex(&delivery.delivery_id),
                    event.map(|event| event.aggregate_id.as_str()).unwrap_or(""),
                    event
                        .map(|event| event.aggregate_version.to_string())
                        .unwrap_or_default(),
                    delivery.attempt,
                    event
                        .map(|event| json_bytes(&event.data).to_string())
                        .unwrap_or_default()
                );
            }
            eprintln!(
                "caught_up={} throttled={}",
                response.caught_up, response.throttled
            );
        }
    }
    Ok(())
}

async fn group_settle(ctx: &Ctx, args: &AggregateGroupSettleArgs) -> Result<()> {
    let action = match args.action {
        AggregateGroupSettlementActionArg::Ack => {
            AggregateGroupSettlementAction::AggregateGroupSettlementAck
        }
        AggregateGroupSettlementActionArg::Retry => {
            AggregateGroupSettlementAction::AggregateGroupSettlementRetry
        }
        AggregateGroupSettlementActionArg::Park => {
            AggregateGroupSettlementAction::AggregateGroupSettlementPark
        }
        AggregateGroupSettlementActionArg::Skip => {
            AggregateGroupSettlementAction::AggregateGroupSettlementSkip
        }
    };
    let response = connect(ctx)
        .await?
        .settle_group(SettleAggregateGroupRequest {
            event_set: Some(event_set(&args.business_space, &args.aggregate_type)),
            name: args.name.clone(),
            consumer_id: args.consumer.clone(),
            settlements: vec![AggregateGroupSettlement {
                delivery_id: decode_hex(&args.delivery)?,
                action: action as i32,
                reason: args.reason.clone(),
            }],
        })
        .await?;
    let result = response
        .results
        .first()
        .ok_or_else(|| anyhow!("服务端未返回 settlement 结果"))?;
    let status = AggregateGroupSettlementStatus::try_from(result.status)
        .map(|status| status.as_str_name())
        .unwrap_or("UNKNOWN");
    match ctx.format {
        Format::Json => println!(
            "{}",
            serde_json::json!({"delivery_id": args.delivery, "status": status})
        ),
        _ => println!("{}\t{}", args.delivery, status),
    }
    Ok(())
}

/// 执行一个 `esctl aggregate` 子命令。
///
/// # 参数
/// `ctx` 提供端点、TLS 和输出格式；`action` 是 clap 已验证的命令参数。
///
/// # 返回
/// 命令成功输出后返回 `Ok(())`。
///
/// # 错误
/// JSON/token/UUID 非法、RPC 失败、OCC 或 revision 冲突时返回错误。
pub async fn run(ctx: &Ctx, action: &AggregateAction) -> Result<()> {
    match action {
        AggregateAction::Capabilities => {
            let value = connect(ctx).await?.capabilities().await?;
            println!(
                "{}",
                serde_json::json!({
                    "api_version": value.api_version,
                    "partition_count": value.partition_count,
                    "max_event_bytes": value.max_event_bytes,
                    "max_state_bytes": value.max_state_bytes,
                    "state_revision_cas": value.state_revision_cas,
                    "explicit_group_settlement": value.explicit_group_settlement,
                })
            );
            Ok(())
        }
        AggregateAction::Create(args) => create(ctx, args).await,
        AggregateAction::List => {
            let infos = connect(ctx).await?.list_event_sets().await?;
            println!("{}", render_event_sets(ctx.format, &infos));
            Ok(())
        }
        AggregateAction::Get(args) => get(ctx, args).await,
        AggregateAction::Append(args) => append(ctx, args).await,
        AggregateAction::Follow(args) => follow(ctx, args).await,
        AggregateAction::State(args) => match &args.action {
            AggregateStateAction::List(args) => state_list(ctx, args).await,
            AggregateStateAction::Get(args) => state_get(ctx, args).await,
            AggregateStateAction::Put(args) => state_put(ctx, args).await,
        },
        AggregateAction::Group(args) => match &args.action {
            AggregateGroupAction::Create(args) => group_create(ctx, args).await,
            AggregateGroupAction::Update(args) => group_update(ctx, args).await,
            AggregateGroupAction::Delete(args) => group_delete(ctx, args).await,
            AggregateGroupAction::List(args) => group_list(ctx, args).await,
            AggregateGroupAction::Fetch(args) => group_fetch(ctx, args).await,
            AggregateGroupAction::Settle(args) => group_settle(ctx, args).await,
        },
        AggregateAction::Status => {
            let value = connect(ctx).await?.status().await?;
            println!(
                "{}",
                serde_json::json!({
                    "catalog_revision": value.catalog_revision,
                    "event_set_count": value.event_set_count,
                    "creating_event_set_count": value.creating_event_set_count,
                    "active_event_set_count": value.active_event_set_count,
                })
            );
            Ok(())
        }
        AggregateAction::Partitions(args) => partitions(ctx, args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn group_info(start: Option<aggregate_group_start::Kind>) -> AggregateGroupInfo {
        AggregateGroupInfo {
            event_set: Some(event_set("orders", "order")),
            name: "workers".into(),
            revision: 2,
            epoch: 3,
            start: start.map(|kind| AggregateGroupStart { kind: Some(kind) }),
            settings: Some(AggregateGroupSettings {
                max_unacked_per_consumer: 4,
                max_unacked_per_group: 8,
                ack_timeout_ms: 10,
                max_retries: 2,
                retry_min_ms: 1,
                retry_max_ms: 5,
            }),
        }
    }

    fn event_set_info(status: i32) -> AggregateEventSetInfo {
        AggregateEventSetInfo {
            event_set: Some(event_set("orders", "order")),
            partition_count: 256,
            hash_algorithm: "xxh3-v1".into(),
            status,
            catalog_revision: 7,
        }
    }

    #[test]
    fn hex_token_round_trip_and_rejects_invalid_input() {
        let bytes = vec![0, 1, 15, 16, 255];
        assert_eq!(decode_hex(&output::hex(&bytes)).unwrap(), bytes);
        assert!(decode_hex("0").is_err());
        assert!(decode_hex("gg").is_err());
    }

    #[test]
    fn expected_aggregate_version_maps_all_variants() {
        for value in [
            ExpectedAggregateVersionArg::Any,
            ExpectedAggregateVersionArg::NoAggregate,
            ExpectedAggregateVersionArg::AggregateExists,
            ExpectedAggregateVersionArg::Exact(7),
        ] {
            assert!(expected_version(&value).kind.is_some());
        }
    }

    #[test]
    fn json_sources_ids_and_binary_fallback_are_validated() {
        let inline = Some(r#"{"value":1}"#.to_string());
        assert_eq!(load_json(&inline, &None).unwrap(), br#"{"value":1}"#);
        assert!(load_json(&Some("{".into()), &None).is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("payload.json");
        std::fs::write(&path, br#"[1,2]"#).unwrap();
        assert_eq!(load_json(&None, &Some(path)).unwrap(), br#"[1,2]"#);
        assert_eq!(json_bytes(br#"{"ok":true}"#)["ok"], true);
        assert_eq!(json_bytes(&[0xff]), serde_json::json!("hex:ff"));

        let explicit = Uuid::new_v4();
        assert_eq!(operation_id(&Some(explicit.to_string())).unwrap(), explicit);
        assert!(operation_id(&Some("bad".into())).is_err());
        assert_ne!(operation_id(&None).unwrap(), Uuid::nil());
    }

    #[test]
    fn group_settings_merge_defaults_current_and_explicit_values() {
        assert!(merged_group_settings(&AggregateGroupSettingsArgs::default(), None).is_none());

        let explicit = AggregateGroupSettingsArgs {
            max_unacked_per_consumer: Some(7),
            max_unacked_per_group: Some(9),
            ack_timeout_ms: Some(11),
            max_retries: Some(3),
            retry_min_ms: Some(2),
            retry_max_ms: Some(6),
        };
        let merged = merged_group_settings(&explicit, None).unwrap();
        assert_eq!(
            (
                merged.max_unacked_per_consumer,
                merged.max_unacked_per_group,
                merged.ack_timeout_ms,
                merged.max_retries,
                merged.retry_min_ms,
                merged.retry_max_ms,
            ),
            (7, 9, 11, 3, 2, 6)
        );

        let current = AggregateGroupSettings {
            max_unacked_per_consumer: 20,
            max_unacked_per_group: 30,
            ack_timeout_ms: 40,
            max_retries: 5,
            retry_min_ms: 6,
            retry_max_ms: 70,
        };
        for partial in [
            AggregateGroupSettingsArgs {
                max_unacked_per_consumer: Some(1),
                ..Default::default()
            },
            AggregateGroupSettingsArgs {
                max_unacked_per_group: Some(2),
                ..Default::default()
            },
            AggregateGroupSettingsArgs {
                ack_timeout_ms: Some(3),
                ..Default::default()
            },
            AggregateGroupSettingsArgs {
                max_retries: Some(4),
                ..Default::default()
            },
            AggregateGroupSettingsArgs {
                retry_min_ms: Some(5),
                ..Default::default()
            },
            AggregateGroupSettingsArgs {
                retry_max_ms: Some(6),
                ..Default::default()
            },
        ] {
            let merged = merged_group_settings(&partial, Some(&current)).unwrap();
            assert!(merged.max_unacked_per_consumer == 1 || merged.max_unacked_per_consumer == 20);
            assert!(merged.max_unacked_per_group == 2 || merged.max_unacked_per_group == 30);
        }
    }

    #[test]
    fn event_set_and_group_rendering_cover_all_formats_and_missing_fields() {
        let active = event_set_info(AggregateEventSetStatus::AggregateEventSetActive as i32);
        for format in [Format::Json, Format::Table, Format::Simple] {
            let rendered = render_event_sets(format, std::slice::from_ref(&active));
            assert!(rendered.contains("orders"));
            assert!(rendered.contains("order"));
        }
        let mut unknown = event_set_info(i32::MAX);
        unknown.event_set = None;
        assert!(render_event_sets(Format::Simple, &[unknown.clone()]).contains("UNKNOWN"));
        assert!(render_event_sets(Format::Table, &[unknown]).contains("UNKNOWN"));

        let beginning = group_info(Some(aggregate_group_start::Kind::Beginning(Empty {})));
        let now = group_info(Some(aggregate_group_start::Kind::Now(Empty {})));
        for format in [Format::Json, Format::Table, Format::Simple] {
            let rendered = render_groups(format, &[beginning.clone(), now.clone()]);
            assert!(rendered.contains("workers"));
            assert!(rendered.contains("beginning"));
            assert!(rendered.contains("now"));
        }
        let mut missing = group_info(None);
        missing.event_set = None;
        missing.settings = None;
        assert_eq!(group_json(&missing)["start"], "unknown");
        assert!(group_json(&missing)["settings"].is_null());
    }
}
