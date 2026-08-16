//! CLI 公共输出渲染。

use uuid::Uuid;

/// 把字节编码为小写十六进制。
pub fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

/// 把事件数据转成可读文本；非 UTF-8 数据以最多 32 字节的十六进制表示。
pub fn event_data_text(data: &[u8]) -> String {
    std::str::from_utf8(data)
        .map(str::to_owned)
        .unwrap_or_else(|_| {
            let length = data.len().min(32);
            format!("hex:{}", hex(&data[..length]))
        })
}

/// 把事件 ID 转成 UUID；非法 UUID 字节退回十六进制。
pub fn event_id_text(event_id: &[u8]) -> String {
    Uuid::from_slice(event_id)
        .map(|uuid| uuid.to_string())
        .unwrap_or_else(|_| hex(event_id))
}

/// 渲染简单对齐表格。
///
/// `header` 定义列名，`rows` 中缺失的单元格按空字符串处理；返回文本以换行结尾。
pub fn render_table(header: &[&str], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = header
        .iter()
        .enumerate()
        .map(|(index, title)| {
            rows.iter()
                .filter_map(|row| row.get(index))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or_default()
                .max(title.chars().count())
        })
        .collect();
    let render = |cells: Vec<String>| {
        cells
            .iter()
            .enumerate()
            .map(|(index, cell)| {
                let mut value = cell.clone();
                value.push_str(&" ".repeat(widths[index].saturating_sub(cell.chars().count())));
                value
            })
            .collect::<Vec<_>>()
            .join("  ")
            .trim_end()
            .to_string()
    };
    let mut output = render(header.iter().map(|value| (*value).to_string()).collect());
    output.push('\n');
    for row in rows {
        let cells = (0..header.len())
            .map(|index| row.get(index).cloned().unwrap_or_default())
            .collect();
        output.push_str(&render(cells));
        output.push('\n');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binary_data_uses_hex() {
        assert_eq!(event_data_text(&[0xff, 0, 0x1a]), "hex:ff001a");
    }

    #[test]
    fn table_columns_are_aligned() {
        let table = render_table(
            &["A", "BB"],
            &[
                vec!["1".into(), "long".into()],
                vec!["222".into(), "x".into()],
            ],
        );
        assert_eq!(
            table.lines().collect::<Vec<_>>(),
            ["A    BB", "1    long", "222  x"]
        );
    }
}
