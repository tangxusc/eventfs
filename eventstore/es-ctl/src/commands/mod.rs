//! 命令分发上下文与公共工具（事件收集、渲染、续读游标透传）。

use anyhow::Result;
use es_proto::eventstore::{Event, ReadEventsResponse, ShardPosition};

use crate::cli::{Format, GlobalArgs};
use crate::client::ClusterClient;
use crate::output;
use crate::shards::{ShardScope, resolve_shard_scope};

pub mod append;
pub mod create_stream;
pub mod init;
pub mod migrate;
pub mod member;
pub mod meta;
pub mod read;
pub mod route;
pub mod snapshot;
pub mod status;
pub mod watch;

/// 命令执行上下文：连接、全局参数、分片范围（惰性探测并缓存）。
pub struct Ctx {
    pub cluster: ClusterClient,
    pub global: GlobalArgs,
    pub format: Format,
    shard_scope: tokio::sync::Mutex<Option<ShardScope>>,
}

impl Ctx {
    pub fn new(cluster: ClusterClient, global: GlobalArgs) -> Self {
        let format = global.write_out;
        Self {
            cluster,
            global,
            format,
            shard_scope: tokio::sync::Mutex::new(None),
        }
    }

    /// 分片范围：首次调用时探测并缓存（探测失败回退默认值并告警）。
    pub async fn shards(&self) -> Result<ShardScope> {
        let mut guard = self.shard_scope.lock().await;
        if let Some(scope) = guard.as_ref() {
            return Ok(scope.clone());
        }
        let scope = resolve_shard_scope(&self.cluster, &self.global).await?;
        *guard = Some(scope.clone());
        Ok(scope)
    }
}

/// 把单向流响应收集为事件列表（读流结束即返回）。
pub async fn collect_events(
    mut stream: tonic::Streaming<ReadEventsResponse>,
) -> Result<Vec<Event>> {
    let mut events = Vec::new();
    while let Some(resp) = stream.message().await? {
        events.extend(resp.events);
    }
    Ok(events)
}

/// 收集 ReadAll 单页响应：事件 + 服务端返回的续读游标（每分片下一页起点）。
///
/// 游标必须由服务端驱动：本页被归并丢弃的分片也会推进游标，
/// 客户端翻页时把 next_positions 原样透传为 from_positions，
/// 缺失分片的事件不会在续读中永久消失。
pub async fn collect_page(
    mut stream: tonic::Streaming<ReadEventsResponse>,
) -> Result<(Vec<Event>, Vec<ShardPosition>)> {
    let mut events = Vec::new();
    let mut next_positions = Vec::new();
    while let Some(resp) = stream.message().await? {
        events.extend(resp.events);
        if !resp.next_positions.is_empty() {
            next_positions = resp.next_positions;
        }
    }
    Ok((events, next_positions))
}

/// 事件渲染文本（read/readall 共用；事件已全部收集），调用方负责 println。
pub fn render_events(format: Format, events: &[Event]) -> String {
    match format {
        Format::Simple => events
            .iter()
            .map(|ev| output::event_simple_line(ev))
            .collect::<Vec<_>>()
            .join("\n"),
        Format::Table => {
            let rows: Vec<Vec<String>> = events
                .iter()
                .map(|ev| {
                    let hlc = ev
                        .hlc
                        .as_ref()
                        .map(|h| output::hlc_to_rfc3339(h.wall))
                        .unwrap_or_else(|| "-".into());
                    vec![
                        ev.stream_id.clone(),
                        ev.version.to_string(),
                        ev.event_type.clone(),
                        hlc,
                        ev.position.to_string(),
                        ev.shard_id.to_string(),
                        output::event_data_text(&ev.data),
                    ]
                })
                .collect();
            output::render_table(
                &["STREAM", "VER", "TYPE", "HLC", "POS", "SHARD", "DATA"],
                &rows,
            )
        }
        Format::Json => {
            let events: Vec<serde_json::Value> = events.iter().map(output::event_to_json).collect();
            serde_json::json!({ "events": events }).to_string()
        }
    }
}

/// 续读游标渲染为 `shard:pos,...` 字符串（simple 模式提示用）
pub fn from_positions_text(positions: &[(u64, u64)]) -> String {
    positions
        .iter()
        .map(|(s, p)| format!("{s}:{p}"))
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(shard: u64, pos: u64) -> Event {
        Event {
            stream_id: "s".into(),
            version: pos,
            event_id: Vec::new(),
            event_type: "T".into(),
            data: Vec::new(),
            metadata: Vec::new(),
            hlc: None,
            position: pos,
            shard_id: shard,
        }
    }

    #[test]
    fn cursor_text_format() {
        assert_eq!(from_positions_text(&[(3, 10), (5, 3)]), "3:10,5:3");
        assert_eq!(from_positions_text(&[]), "");
    }

    #[test]
    fn events_table_rendering() {
        let ev = event(1, 2);
        let table = render_events(Format::Table, &[ev]);
        assert!(table.contains("STREAM"), "应有表头");
        assert!(table.contains("T"), "应有事件类型");
        let json = render_events(Format::Json, &[]);
        assert_eq!(json, r#"{"events":[]}"#);
        let empty_simple = render_events(Format::Simple, &[]);
        assert_eq!(empty_simple, "");
    }
}
