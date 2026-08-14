//! Key 编码：将逻辑键编码为 surrealkv 的字节键。
//!
//! 核心约束：
//! 1. 整数必须固定宽度大端编码（字节序 = 数值序）
//! 2. stream_id 前必须加长度前缀，避免前缀包含时范围扫描串流
//! 3. 不同命名空间用首字节 tag 隔离

use std::io;

/// Raft 日志区 tag
const TAG_RAFT: u8 = 0x01;
/// 状态机区 tag
const TAG_SM: u8 = 0x02;
/// 快照区 tag
const TAG_SNAPSHOT: u8 = 0x03;

/// Raft 日志子类别
const RAFT_LOG_ENTRY: u8 = 0x01;
const RAFT_VOTE: u8 = 0x02;
const RAFT_LAST_PURGED: u8 = 0x03;
const RAFT_COMMITTED: u8 = 0x04;

/// 状态机子类别
const SM_EVENT: u8 = 0x01;
const SM_STREAM_META: u8 = 0x02;
const SM_POSITION_PTR: u8 = 0x03;
const SM_APPLIED_STATE: u8 = 0x04;
const SM_IDEMPOTENCY: u8 = 0x05;
const SM_NEXT_POSITION: u8 = 0x06;
const SM_OWNERSHIP_CATALOG: u8 = 0x07;
const SM_OWNERSHIP_FENCE: u8 = 0x08;
const SM_PERSISTENT_GROUP: u8 = 0x09;

/// 快照子类别
const SNAPSHOT_CURRENT: u8 = 0x01;

/// 编码 u64 为 8 字节大端
#[inline]
fn encode_u64_be(v: u64) -> [u8; 8] {
    v.to_be_bytes()
}

/// 解码 8 字节大端为 u64
#[inline]
fn decode_u64_be(b: &[u8]) -> Result<u64, io::Error> {
    if b.len() != 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("期望 8 字节，实际 {} 字节", b.len()),
        ));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(b);
    Ok(u64::from_be_bytes(arr))
}

/// 计算字节序后继（用于 range 上界）。
///
/// 从末字节向前找第一个不等于 0xFF 的字节，加一并截断其后所有字节。
/// 若全为 0xFF 则返回 None（表示上界无穷，扫到末尾）。
pub fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    for i in (0..prefix.len()).rev() {
        if prefix[i] != 0xFF {
            let mut s = prefix[..=i].to_vec();
            s[i] += 1;
            return Some(s);
        }
    }
    None // 全 0xFF，无后继
}

/// Raft 日志 entry key: [0x01][shard:BE8][0x01][index:BE8]
pub fn raft_log_entry(shard_id: u64, index: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(18);
    k.push(TAG_RAFT);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(RAFT_LOG_ENTRY);
    k.extend_from_slice(&encode_u64_be(index));
    k
}

/// Raft vote key: [0x01][shard:BE8][0x02]
pub fn raft_vote(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_RAFT);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(RAFT_VOTE);
    k
}

/// Raft last_purged_log_id key
pub fn raft_last_purged(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_RAFT);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(RAFT_LAST_PURGED);
    k
}

/// Raft committed log id key
pub fn raft_committed(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_RAFT);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(RAFT_COMMITTED);
    k
}

/// 状态机 event key: [0x02][shard:BE8][0x01][slen:BE8][stream][version:BE8]
pub fn sm_event(shard_id: u64, stream_id: &str, version: u64) -> Vec<u8> {
    let stream_bytes = stream_id.as_bytes();
    let slen = stream_bytes.len() as u64;
    let mut k = Vec::with_capacity(26 + stream_bytes.len());
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_EVENT);
    k.extend_from_slice(&encode_u64_be(slen));
    k.extend_from_slice(stream_bytes);
    k.extend_from_slice(&encode_u64_be(version));
    k
}

/// 状态机 stream_meta key: [0x02][shard:BE8][0x02][slen:BE8][stream]
pub fn sm_stream_meta(shard_id: u64, stream_id: &str) -> Vec<u8> {
    let stream_bytes = stream_id.as_bytes();
    let slen = stream_bytes.len() as u64;
    let mut k = Vec::with_capacity(18 + stream_bytes.len());
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_STREAM_META);
    k.extend_from_slice(&encode_u64_be(slen));
    k.extend_from_slice(stream_bytes);
    k
}

/// 状态机 position 指针 key: [0x02][shard:BE8][0x03][position:BE8]
pub fn sm_position_ptr(shard_id: u64, position: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(18);
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_POSITION_PTR);
    k.extend_from_slice(&encode_u64_be(position));
    k
}

/// 状态机 applied_state key: [0x02][shard:BE8][0x04]
pub fn sm_applied_state(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_APPLIED_STATE);
    k
}

/// 状态机 idempotency key: [0x02][shard:BE8][0x05][event_id:16B]
pub fn sm_idempotency(shard_id: u64, event_id: &uuid::Uuid) -> Vec<u8> {
    let mut k = Vec::with_capacity(26);
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_IDEMPOTENCY);
    k.extend_from_slice(event_id.as_bytes());
    k
}

/// 状态机 next_position 计数器 key: [0x02][shard:BE8][0x06]
pub fn sm_next_position(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_NEXT_POSITION);
    k
}

/// 控制 Shard 的 Stream 归属权威状态 key。
pub fn sm_ownership_catalog(shard_id: u64) -> Vec<u8> {
    sm_sub_prefix(shard_id, SM_OWNERSHIP_CATALOG)
}

/// 数据 Shard 的 Stream 归属代次 fencing key。
pub fn sm_ownership_fence(shard_id: u64, stream_id: &str) -> Vec<u8> {
    let stream_bytes = stream_id.as_bytes();
    let mut key = sm_sub_prefix(shard_id, SM_OWNERSHIP_FENCE);
    key.extend_from_slice(&(stream_bytes.len() as u64).to_be_bytes());
    key.extend_from_slice(stream_bytes);
    key
}

/// 持久化订阅组 key：`[SM][shard][group-tag][len:BE8][group]`。
pub fn sm_persistent_group(shard_id: u64, group: &str) -> Vec<u8> {
    let bytes = group.as_bytes();
    let mut key = sm_sub_prefix(shard_id, SM_PERSISTENT_GROUP);
    key.extend_from_slice(&encode_u64_be(bytes.len() as u64));
    key.extend_from_slice(bytes);
    key
}

/// 持久化订阅组扫描前缀。
pub fn sm_persistent_group_prefix(shard_id: u64) -> Vec<u8> {
    sm_sub_prefix(shard_id, SM_PERSISTENT_GROUP)
}

/// 从持久化订阅组 key 解出组名。
pub fn decode_persistent_group_key(key: &[u8]) -> Option<String> {
    const HEAD: usize = 10;
    if key.len() < HEAD + 8 {
        return None;
    }
    let len = decode_u64_be(&key[HEAD..HEAD + 8]).ok()? as usize;
    let start = HEAD + 8;
    if key.len() != start + len {
        return None;
    }
    String::from_utf8(key[start..].to_vec()).ok()
}

/// 快照 key: [0x03][shard:BE8][0x01]
pub fn snapshot_current(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_SNAPSHOT);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SNAPSHOT_CURRENT);
    k
}

/// 构造某 stream 的事件扫描前缀: [0x02][shard:BE8][0x01][slen:BE8][stream]
pub fn sm_event_prefix(shard_id: u64, stream_id: &str) -> Vec<u8> {
    let stream_bytes = stream_id.as_bytes();
    let slen = stream_bytes.len() as u64;
    let mut k = Vec::with_capacity(18 + stream_bytes.len());
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_EVENT);
    k.extend_from_slice(&encode_u64_be(slen));
    k.extend_from_slice(stream_bytes);
    k
}

/// Raft 日志区扫描前缀: [0x01][shard:BE8][0x01]
pub fn raft_log_prefix(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_RAFT);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(RAFT_LOG_ENTRY);
    k
}

/// Raft 日志区的排他上界（前缀的字节序后继）。
///
/// 前缀末字节为 `RAFT_LOG_ENTRY`(0x01)，恒不为 0xFF，故后继必然存在，
/// 结果正是 vote key。因 `range` 左闭右开，vote key 本身被排除，
/// 扫描结果恰为该分片全部日志条目。
pub fn raft_log_upper(shard_id: u64) -> Vec<u8> {
    successor(&raft_log_prefix(shard_id)).expect("日志前缀末字节非 0xFF，后继必然存在")
}

/// 构造「包含 k 自身」的排他上界：在 k 末尾追加一个 0x00。
///
/// 用于反向扫描的上界。**不能用 `successor(k)`**：k 末尾是定宽 BE 整数，
/// 当该整数为 `u64::MAX` 时字节全为 0xFF，`successor` 会向前进位到
/// stream / shard 部分，越界到别的 key 段。
///
/// 追加 0x00 得到的键严格大于 k（同前缀但更长），又小于任何
/// 「同前缀且整数更大」的键（差异在定宽整数的首个不同字节上决出），
/// 因此恰好是包含 k 的最小排他上界。
pub fn upper_including(k: &[u8]) -> Vec<u8> {
    let mut u = Vec::with_capacity(k.len() + 1);
    u.extend_from_slice(k);
    u.push(0);
    u
}

/// 某分片状态机区某子类别的扫描前缀
fn sm_sub_prefix(shard_id: u64, sub: u8) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(sub);
    k
}

/// StreamMeta 区前缀，用于枚举分片内全部流
pub fn sm_stream_meta_prefix(shard_id: u64) -> Vec<u8> {
    sm_sub_prefix(shard_id, SM_STREAM_META)
}

/// position 指针区前缀，用于按提交序扫描
pub fn sm_position_prefix(shard_id: u64) -> Vec<u8> {
    sm_sub_prefix(shard_id, SM_POSITION_PTR)
}

/// 幂等索引区前缀，用于枚举全部幂等记录
pub fn sm_idempotency_prefix(shard_id: u64) -> Vec<u8> {
    sm_sub_prefix(shard_id, SM_IDEMPOTENCY)
}

/// 从 StreamMeta key 中解出 stream_id。
///
/// 布局 `[TAG][shard:BE8][SUB][slen:BE8][stream]`，据 slen 截取 stream 段。
pub fn decode_stream_meta_key(k: &[u8]) -> Option<String> {
    const HEAD: usize = 10; // TAG(1) + shard(8) + 子类别(1)
    if k.len() < HEAD + 8 {
        return None;
    }
    let slen = decode_u64_be(&k[HEAD..HEAD + 8]).ok()? as usize;
    let start = HEAD + 8;
    if k.len() != start + slen {
        return None;
    }
    String::from_utf8(k[start..].to_vec()).ok()
}

/// 从日志 key 中解出 index。用于反向迭代取 last_log_id 时的校验。
pub fn decode_log_index(key: &[u8]) -> Option<u64> {
    let prefix_len = 10; // TAG(1) + shard(8) + 子类别(1)
    if key.len() != prefix_len + 8 {
        return None;
    }
    decode_u64_be(&key[prefix_len..]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_encode_roundtrip() {
        for v in [0, 1, 42, u64::MAX / 2, u64::MAX - 1, u64::MAX] {
            let enc = encode_u64_be(v);
            let dec = decode_u64_be(&enc).unwrap();
            assert_eq!(dec, v, "{v} 往返失败");
        }
    }

    #[test]
    fn byte_order_matches_numeric_order() {
        // 这是设计文档 9 节明确要求的测试：大端编码的字节序必须等于数值序
        let nums = [0u64, 1, 10, 100, 255, 256, 1000, 65535, 65536, u64::MAX];
        let mut encoded: Vec<_> = nums.iter().map(|&n| encode_u64_be(n)).collect();
        encoded.sort(); // 按字节序排序
        for i in 0..encoded.len() - 1 {
            let a = decode_u64_be(&encoded[i]).unwrap();
            let b = decode_u64_be(&encoded[i + 1]).unwrap();
            assert!(a < b, "字节序排序后数值序 {a} 应 < {b}");
        }
    }

    #[test]
    fn random_indices_byte_order_consistent() {
        use proptest::prelude::*;
        proptest!(|(indices in prop::collection::vec(0u64..10000, 10..50))| {
            let mut by_num = indices.clone();
            by_num.sort(); // 数值序
            let mut by_bytes: Vec<_> = indices.iter().map(|&i| encode_u64_be(i)).collect();
            by_bytes.sort(); // 字节序
            let recovered: Vec<_> = by_bytes.iter().map(|b| decode_u64_be(b).unwrap()).collect();
            assert_eq!(recovered, by_num, "字节序与数值序不一致");
        });
    }

    #[test]
    fn successor_normal() {
        assert_eq!(successor(&[0x01, 0x02]), Some(vec![0x01, 0x03]));
        assert_eq!(successor(&[0x01, 0xFF]), Some(vec![0x02]));
        assert_eq!(successor(&[0xFE, 0xFF, 0xFF]), Some(vec![0xFF]));
    }

    #[test]
    fn successor_all_ff_returns_none() {
        assert_eq!(successor(&[0xFF, 0xFF, 0xFF]), None);
    }

    #[test]
    fn raft_log_entry_ordered_by_index() {
        let k0 = raft_log_entry(1, 0);
        let k1 = raft_log_entry(1, 1);
        let k100 = raft_log_entry(1, 100);
        assert!(k0 < k1);
        assert!(k1 < k100);
    }

    #[test]
    fn sm_event_ordered_by_version() {
        let e0 = sm_event(1, "test", 0);
        let e1 = sm_event(1, "test", 1);
        let e100 = sm_event(1, "test", 100);
        assert!(e0 < e1);
        assert!(e1 < e100);
    }

    #[test]
    fn length_prefix_isolates_streams() {
        // stream "a" 与 "ab" 前缀包含,长度前缀必须将它们隔离
        let _a0 = sm_event(1, "a", 0);
        let a1 = sm_event(1, "a", 1);
        let ab0 = sm_event(1, "ab", 0);
        // "a" 的所有版本应小于 "ab" 的任何版本（因 slen 不同）
        assert!(a1 < ab0, "长度前缀隔离失败");
    }

    #[test]
    fn sm_event_prefix_covers_all_versions() {
        let prefix = sm_event_prefix(1, "test");
        let e0 = sm_event(1, "test", 0);
        let e100 = sm_event(1, "test", 100);
        assert!(e0.starts_with(&prefix));
        assert!(e100.starts_with(&prefix));
        // 其它 stream 不应匹配
        let other = sm_event(1, "other", 0);
        assert!(!other.starts_with(&prefix));
    }

    #[test]
    fn shards_isolated() {
        let s1 = sm_event(1, "test", 0);
        let s2 = sm_event(2, "test", 0);
        assert_ne!(s1, s2);
        assert!(s1 < s2); // shard_id 在前,自然有序
    }

    #[test]
    fn log_upper_excludes_vote_key() {
        let upper = raft_log_upper(7);
        // 上界即 vote key，因 range 左闭右开，vote 本身不会被扫进来
        assert_eq!(upper, raft_vote(7));
        // 任意 index 的日志 key 都严格小于上界
        for idx in [0u64, 1, 1000, u64::MAX] {
            assert!(raft_log_entry(7, idx) < upper, "index={idx} 应 < 上界");
        }
    }

    #[test]
    fn log_prefix_covers_all_indices() {
        let prefix = raft_log_prefix(3);
        for idx in [0u64, 1, u64::MAX - 1, u64::MAX] {
            assert!(raft_log_entry(3, idx).starts_with(&prefix));
        }
        // 其它分片不匹配
        assert!(!raft_log_entry(4, 0).starts_with(&prefix));
    }

    #[test]
    fn upper_including_max_boundary() {
        // version = MAX 时 successor 会向前进位越界，upper_including 不会
        let at_max = sm_event(3, "s", u64::MAX);
        let upper = upper_including(&at_max);
        assert!(upper > at_max, "上界须严格大于自身，才能把自身含进区间");

        // 该上界不能吞掉别的 stream：它仍在本 stream 的前缀段内
        let prefix = sm_event_prefix(3, "s");
        assert!(upper.starts_with(&prefix));

        // 对比 successor：它会进位改掉 stream 字节，越界到别处
        let succ = successor(&at_max).expect("非全 FF");
        assert!(
            !succ.starts_with(&prefix),
            "successor 在 MAX 处确实会越出本 stream 段，这正是不能用它的原因"
        );
    }

    #[test]
    fn upper_including_no_version_overflow() {
        let k5 = sm_event(1, "s", 5);
        let k6 = sm_event(1, "s", 6);
        let upper = upper_including(&k5);
        assert!(upper > k5, "应含 version 5");
        assert!(upper < k6, "不应含 version 6");
    }

    #[test]
    fn stream_meta_key_decode_roundtrip() {
        for name in ["", "a", "订单-123", "a\u{0}b"] {
            let k = sm_stream_meta(7, name);
            assert_eq!(decode_stream_meta_key(&k).as_deref(), Some(name));
        }
        // 长度不符应返回 None，避免把别的 key 误判成 StreamMeta
        assert_eq!(decode_stream_meta_key(&sm_event(7, "a", 0)), None);
    }

    #[test]
    fn sub_prefixes_disjoint() {
        let meta = sm_stream_meta_prefix(2);
        let pos = sm_position_prefix(2);
        let idem = sm_idempotency_prefix(2);
        // 三个子区段各自独立，扫一个不会扫出另一个
        assert!(sm_stream_meta(2, "x").starts_with(&meta));
        assert!(!sm_stream_meta(2, "x").starts_with(&pos));
        assert!(sm_position_ptr(2, 0).starts_with(&pos));
        assert!(!sm_position_ptr(2, 0).starts_with(&idem));
    }

    #[test]
    fn log_index_decode_roundtrip() {
        for idx in [0u64, 1, 42, u64::MAX] {
            let k = raft_log_entry(9, idx);
            assert_eq!(decode_log_index(&k), Some(idx));
        }
        // 长度不符返回 None，避免把 vote key 误认成日志
        assert_eq!(decode_log_index(&raft_vote(9)), None);
    }

    #[test]
    fn decode_u64_be_wrong_len_errors() {
        for bad in [&[][..], &[0x01][..], &[0u8; 7][..], &[0u8; 9][..]] {
            let err = decode_u64_be(bad).expect_err("长度 != 8 应报错");
            assert!(err.to_string().contains("期望 8 字节"), "{err}");
        }
    }

    #[test]
    fn decode_stream_meta_key_short_returns_none() {
        // HEAD(10) + slen(8) = 18 字节以下的 key 直接拒绝
        assert_eq!(decode_stream_meta_key(&[]), None);
        assert_eq!(decode_stream_meta_key(&[0u8; 10]), None);
        assert_eq!(decode_stream_meta_key(&[0u8; 17]), None);
    }
}
