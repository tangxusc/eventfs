//! 输出渲染：simple / table / json 三种格式。
//!
//! 事件行格式（read/readall/watch 共用，simple 模式）：
//! `{version}\t{RFC3339}\t[{event_type}]\t{data}`，data 非 UTF-8 时输出 `hex:..`。

use chrono::{DateTime, Utc};
use serde_json::json;
use uuid::Uuid;

use es_proto::eventstore::Event;

/// 字节数组转小写十六进制
pub fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// HLC wall 毫秒转 RFC3339；转换失败时退回原始数字
pub fn hlc_to_rfc3339(wall_ms: u64) -> String {
    match DateTime::<Utc>::from_timestamp_millis(wall_ms as i64) {
        Some(t) => t.to_rfc3339(),
        None => wall_ms.to_string(),
    }
}

/// 事件数据文本化：合法 UTF-8 原样输出，否则 hex:..（含前 32 字节截断）
pub fn event_data_text(data: &[u8]) -> String {
    match std::str::from_utf8(data) {
        Ok(s) => s.to_string(),
        Err(_) => {
            let n = data.len().min(32);
            format!("hex:{}", hex(&data[..n]))
        }
    }
}

/// event_id 字节转 UUID 字符串；非法时退回 hex
pub fn event_id_text(event_id: &[u8]) -> String {
    match Uuid::from_slice(event_id) {
        Ok(u) => u.to_string(),
        Err(_) => hex(event_id),
    }
}

/// 事件数据 JSON 化：合法 UTF-8 存字符串，否则存 "hex:.."
fn event_data_json(data: &[u8]) -> serde_json::Value {
    match std::str::from_utf8(data) {
        Ok(s) => json!(s),
        Err(_) => json!(format!("hex:{}", hex(data))),
    }
}

/// 单个事件转 JSON 对象
pub fn event_to_json(ev: &Event) -> serde_json::Value {
    json!({
        "stream_id": ev.stream_id,
        "version": ev.version,
        "event_id": event_id_text(&ev.event_id),
        "event_type": ev.event_type,
        "data": event_data_json(&ev.data),
        "metadata": event_data_json(&ev.metadata),
        "hlc": { "wall": ev.hlc.as_ref().map(|h| h.wall).unwrap_or(0),
                 "logical": ev.hlc.as_ref().map(|h| h.logical).unwrap_or(0) },
        "position": ev.position,
        "shard_id": ev.shard_id,
    })
}

/// simple 模式事件行
pub fn event_simple_line(ev: &Event) -> String {
    let hlc = ev
        .hlc
        .as_ref()
        .map(|h| hlc_to_rfc3339(h.wall))
        .unwrap_or_else(|| "-".into());
    format!(
        "{}\t{}\t[{}]\t{}",
        ev.version,
        hlc,
        ev.event_type,
        event_data_text(&ev.data)
    )
}

/// 渲染简单对齐表格：每列按内容最宽对齐，列间两个空格
pub fn render_table(header: &[&str], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = header
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let w = h.chars().count();
            rows.iter()
                .map(|r| r.get(i).map(|c| c.chars().count()).unwrap_or(0))
                .max()
                .unwrap_or(0)
                .max(w)
        })
        .collect();

    let mut out = String::new();
    let pad = |s: &str, w: usize| {
        let mut line = s.to_string();
        let pad_len = w.saturating_sub(line.chars().count());
        line.push_str(&" ".repeat(pad_len));
        line
    };

    let head: Vec<String> = header
        .iter()
        .zip(&widths)
        .map(|(h, w)| pad(h, *w))
        .collect();
    out.push_str(head.join("  ").trim_end());
    out.push('\n');

    for row in rows {
        let cells: Vec<String> = widths
            .iter()
            .enumerate()
            .map(|(i, w)| pad(row.get(i).map(String::as_str).unwrap_or(""), *w))
            .collect();
        // 行尾不保留填充空格
        out.push_str(cells.join("  ").trim_end());
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(v: u64, t: &str, data: &[u8]) -> Event {
        Event {
            stream_id: "s/1".into(),
            version: v,
            event_id: Uuid::new_v4().as_bytes().to_vec(),
            event_type: t.into(),
            data: data.to_vec(),
            metadata: Vec::new(),
            hlc: Some(es_proto::eventstore::Hlc {
                wall: 1_700_000_000_123,
                logical: 1,
            }),
            position: v,
            shard_id: 0,
        }
    }

    #[test]
    fn hex_输出小写16进制() {
        assert_eq!(hex(&[0x0f, 0xAB, 0x01]), "0fab01");
        assert_eq!(hex(&[]), "");
    }

    #[test]
    fn utf8数据原样输出() {
        assert_eq!(event_data_text(b"hello"), "hello");
    }

    #[test]
    fn 二进制数据hex输出() {
        assert_eq!(event_data_text(&[0xff, 0x00, 0x1a]), "hex:ff001a");
    }

    #[test]
    fn hlc毫秒转rfc3339() {
        assert_eq!(
            hlc_to_rfc3339(1_700_000_000_123),
            "2023-11-14T22:13:20.123+00:00"
        );
    }

    #[test]
    fn 事件simple行格式() {
        let line = event_simple_line(&event(3, "OrderPlaced", b"abc"));
        let parts: Vec<&str> = line.split('\t').collect();
        assert_eq!(parts[0], "3");
        assert_eq!(parts[2], "[OrderPlaced]");
        assert_eq!(parts[3], "abc");
    }

    #[test]
    fn 事件json结构() {
        let v = event_to_json(&event(1, "T", b"d"));
        assert_eq!(v["stream_id"], "s/1");
        assert_eq!(v["version"], 1);
        assert_eq!(v["data"], "d");
        assert_eq!(v["hlc"]["wall"], 1_700_000_000_123u64);
    }

    #[test]
    fn 表格对齐() {
        let table = render_table(
            &["A", "BB"],
            &[
                vec!["1".into(), "long".into()],
                vec!["222".into(), "x".into()],
            ],
        );
        let lines: Vec<&str> = table.lines().collect();
        assert_eq!(lines[0], "A    BB");
        assert_eq!(lines[1], "1    long");
        assert_eq!(lines[2], "222  x");
    }

    #[test]
    fn 表格空行() {
        let table = render_table(&["A", "B"], &[]);
        assert_eq!(table, "A  B\n");
    }
}
