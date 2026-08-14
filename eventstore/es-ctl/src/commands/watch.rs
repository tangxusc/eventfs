//! `esctl watch`：订阅流事件（先追平历史，追平后实时推送）。

use anyhow::{Result, anyhow, bail};
use es_proto::eventstore::*;

use crate::cli::{Format, WatchArgs};
use crate::commands::Ctx;
use crate::output;

/// 单条订阅消息渲染（流式输出，事件、状态交替出现）。
fn render_message(format: Format, payload: subscribe_response::Payload) -> String {
    match payload {
        subscribe_response::Payload::Event(ev) => match format {
            Format::Json => {
                serde_json::json!({ "type": "event", "event": output::subscription_event_to_json(&ev) })
                    .to_string()
            }
            _ => output::subscription_event_simple_line(&ev),
        },
        subscribe_response::Payload::CaughtUp(_) => match format {
            Format::Json => r#"{"type":"caught_up"}"#.into(),
            Format::Table => "---- caught up, entering live mode ----".into(),
            Format::Simple => "[已追平，进入实时推送]".into(),
        },
        subscribe_response::Payload::Degraded(_) => match format {
            Format::Json => r#"{"type":"degraded"}"#.into(),
            Format::Table => "---- subscription degraded ----".into(),
            Format::Simple => "[订阅已降级，部分内部来源不可用]".into(),
        },
    }
}

pub async fn run(ctx: &Ctx, args: &WatchArgs) -> Result<()> {
    let target = match (args.stream.is_empty(), args.all) {
        (false, false) => subscribe_request::Target::Streams(SubscribeStreams {
            stream_ids: args.stream.clone(),
        }),
        (true, true) => subscribe_request::Target::All(Empty {}),
        _ => bail!("watch 需要指定 <STREAM> 或 --all"),
    };

    let req = SubscribeRequest {
        target: Some(target),
    };

    // 订阅是长连接，选一个端点直连（失败即退出，不做端点间重试）
    let endpoint = ctx.cluster.pick_endpoint();
    let mut client = ctx.cluster.event_client(&endpoint).await?;
    let mut stream = client
        .subscribe(req)
        .await
        .map_err(|e| anyhow!("订阅失败（端点 {endpoint}）：{}", e.message()))?
        .into_inner();

    let mut caught_up = false;
    let mut degraded = false;
    while let Some(resp) = stream.message().await? {
        match resp.payload {
            Some(payload) => {
                let is_caught_up = matches!(payload, subscribe_response::Payload::CaughtUp(_));
                let is_degraded = matches!(payload, subscribe_response::Payload::Degraded(_));
                println!("{}", render_message(ctx.format, payload));
                degraded |= is_degraded;
                if is_degraded && args.once {
                    return Err(anyhow!("订阅已降级，无法确认所有 stream 均已追平"));
                }
                if is_caught_up {
                    caught_up = true;
                    if args.once {
                        return Ok(());
                    }
                }
            }
            None => {} // 空消息跳过
        }
    }

    // 流正常结束但未追平：服务端关闭（如订阅者落后被 Lagged 踢出）。
    // --once 的退出码 0 语义前提是「已追平」，未追平必须非零退出，
    // 否则依赖退出码判断历史已消费完的脚本会静默缺失数据。
    if args.once && (!caught_up || degraded) {
        return Err(anyhow!(
            "订阅流在收到 caught_up 信号前关闭（可能因落后被服务端断开）"
        ));
    }
    if !caught_up {
        eprintln!("订阅流已关闭（可能因落后被服务端断开）");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev() -> SubscriptionEvent {
        SubscriptionEvent {
            stream_id: "s".into(),
            version: 1,
            event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
            event_type: "T".into(),
            data: b"d".to_vec(),
            metadata: Vec::new(),
            hlc: None,
        }
    }

    #[test]
    fn caught_up_message_three_formats() {
        let p = subscribe_response::Payload::CaughtUp(Empty {});
        assert_eq!(
            render_message(Format::Simple, p.clone()),
            "[已追平，进入实时推送]"
        );
        assert_eq!(
            render_message(Format::Table, p.clone()),
            "---- caught up, entering live mode ----"
        );
        assert_eq!(render_message(Format::Json, p), r#"{"type":"caught_up"}"#);
    }

    #[test]
    fn event_message_json_typed() {
        let p = subscribe_response::Payload::Event(ev());
        let json: serde_json::Value =
            serde_json::from_str(&render_message(Format::Json, p)).expect("合法 JSON");
        assert_eq!(json["type"], "event");
        assert_eq!(json["event"]["stream_id"], "s");
    }

    #[test]
    fn degraded_message_three_formats() {
        let p = subscribe_response::Payload::Degraded(Empty {});
        assert_eq!(
            render_message(Format::Simple, p.clone()),
            "[订阅已降级，部分内部来源不可用]"
        );
        assert_eq!(
            render_message(Format::Table, p.clone()),
            "---- subscription degraded ----"
        );
        assert_eq!(render_message(Format::Json, p), r#"{"type":"degraded"}"#);
    }
}
