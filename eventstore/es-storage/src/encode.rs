//! 存储值二进制编码：统一使用 bincode 的 `standard()` 配置（小端 +
//! **变长整数** varint，不是定长——同一 u64 的编码长度随值大小变化，
//! 离线工具/手写解析须按 varint 处理，见 es-raft/network.rs:400）。
//!
//! 存储格式是**内部格式**，与网络 proto（raft.proto / eventstore.proto）
//! 和客户端 API 完全解耦，可独立演进。2026-08 从 serde_json 迁移：
//! JSON 序列化 Vec<u8> 时 base64 膨胀 33% 且慢数倍，bincode 直接复制
//! 字节。3 节点 TLS 压测：写入吞吐 4–6×（8–11 → 35–55 MB/s）、读取
//! 3.5–4×、100KB 单条 p99 从 71ms 降到 4.2ms（见 docs/benchmarks.md）。
//!
//! 注意：离线工具（reshard、snapshot 恢复）读写同一存储格式，任何编码
//! 变更必须同步。快照文件头 meta 故意保留 JSON（几百字节、与数据格式
//! 解耦，跨版本读取更稳）。
//!
//! 错误处理：本模块返回 bincode 原始错误（不包装成 `es_core::Error`），
//! 由调用方在边界处带上下文包装（如「Event 反序列化失败」），避免
//! Serde 嵌套 Serde 的双层错误信息。

use bincode::config::standard;
// DecodeError 的 Custom 变体是 non_exhaustive 不能直接构造，经其实现
// 的 serde::de::Error::custom 关联函数构造（匿名导入避免 trait 名冲突）
use serde::de::Error as _;

/// 编码为 bincode 字节。
///
/// # 参数
/// - `v`: 待序列化的值
///
/// # 返回
/// 编码后的字节；序列化失败时返回 [`bincode::error::EncodeError`]。
pub fn encode<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(v, standard())
}

/// 解码 bincode 字节。
///
/// 严格模式：要求 `bytes` 恰好是单个值的完整编码，存在尾随字节即报错。
/// 静默丢弃尾字节会吞掉损坏数据——旧 JSON 遗留字节（varint 头字节被
/// 接受、剩余字节被忽略）会解码出垃圾值而不是响亮失败。开发期无旧
/// 数据迁移，宁可报错也不静默接受。
///
/// # 参数
/// - `bytes`: 单个值完整编码的字节
///
/// # 返回
/// 解码出的值；解码失败或存在尾随字节时返回
/// [`bincode::error::DecodeError`]。
pub fn decode<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, bincode::error::DecodeError> {
    let (v, consumed) = bincode::serde::decode_from_slice(bytes, standard())?;
    if consumed != bytes.len() {
        return Err(bincode::error::DecodeError::custom(format!(
            "值编码后仍有 {} 字节尾随数据（严格模式拒绝）",
            bytes.len() - consumed
        )));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_single_values() {
        for x in [0u64, 1, 49, 123, u64::MAX] {
            let bytes = encode(&x).unwrap();
            assert_eq!(decode::<u64>(&bytes).unwrap(), x, "u64 roundtrip {x}");
        }
        let s = "你好".to_string();
        let bytes = encode(&s).unwrap();
        assert_eq!(decode::<String>(&bytes).unwrap(), s);
    }

    #[test]
    fn trailing_bytes_rejected() {
        // 合法编码后追加垃圾字节：必须报错，不能静默丢弃
        let mut junk = encode(&42u64).unwrap();
        junk.push(0x00);
        assert!(decode::<u64>(&junk).is_err(), "尾随字节应报错");

        // 旧 JSON 遗留字节：varint 头字节可解码（'1'=49）、剩余被忽略
        // 的静默损坏路径，严格模式下一律报错
        assert!(decode::<u64>(b"12345678").is_err(), "JSON 遗留字节应报错");
        assert!(decode::<u64>(b"{}").is_err());
    }

    #[test]
    fn truncated_rejected() {
        let bytes = encode(&"hello".to_string()).unwrap();
        assert!(
            decode::<String>(&bytes[..bytes.len() - 1]).is_err(),
            "截断编码应报错"
        );
    }
}
