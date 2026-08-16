//! Key 编码：将逻辑键编码为 surrealkv 的字节键。
//!
//! 核心约束：
//! 1. 整数必须固定宽度大端编码（字节序 = 数值序）
//! 2. 可变标识符必须加长度前缀，避免前缀包含时范围扫描串数据
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
const SM_APPLIED_STATE: u8 = 0x04;
const SM_AGGREGATE_EVENT: u8 = 0x0A;
const SM_AGGREGATE_META: u8 = 0x0B;
const SM_AGGREGATE_PARTITION_INDEX: u8 = 0x0C;
const SM_AGGREGATE_NEXT_POSITION: u8 = 0x0D;
const SM_AGGREGATE_STATE: u8 = 0x0E;
const SM_AGGREGATE_IDEMPOTENCY: u8 = 0x0F;
const SM_AGGREGATE_PARTITION_FENCE: u8 = 0x10;
const SM_AGGREGATE_CATALOG: u8 = 0x11;
const SM_AGGREGATE_GROUP_CATALOG: u8 = 0x12;
const SM_AGGREGATE_GROUP_PARTITION: u8 = 0x13;
const SM_AGGREGATE_STATE_MODIFIED: u8 = 0x14;

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

/// 状态机 applied_state key: [0x02][shard:BE8][0x04]
pub fn sm_applied_state(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_SM);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SM_APPLIED_STATE);
    k
}

fn append_len_prefixed(key: &mut Vec<u8>, bytes: &[u8]) {
    key.extend_from_slice(&encode_u64_be(bytes.len() as u64));
    key.extend_from_slice(bytes);
}

fn aggregate_partition_prefix(
    shard_id: u64,
    sub: u8,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
) -> Vec<u8> {
    let aggregate_type = aggregate_type.canonical_name();
    let mut key = sm_sub_prefix(shard_id, sub);
    append_len_prefixed(&mut key, aggregate_type.as_bytes());
    key.extend_from_slice(&partition_id.to_be_bytes());
    key
}

fn aggregate_instance_prefix(
    shard_id: u64,
    sub: u8,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    aggregate_id: &str,
) -> Vec<u8> {
    let mut key = aggregate_partition_prefix(shard_id, sub, aggregate_type, partition_id);
    append_len_prefixed(&mut key, aggregate_id.as_bytes());
    key
}

/// 聚合事件 key：聚合类型、虚拟分区、实例 ID 和聚合版本共同定位。
pub fn sm_aggregate_event(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    aggregate_id: &str,
    aggregate_version: u64,
) -> Vec<u8> {
    let mut key = sm_aggregate_event_prefix(shard_id, aggregate_type, partition_id, aggregate_id);
    key.extend_from_slice(&encode_u64_be(aggregate_version));
    key
}

/// 单聚合实例的事件扫描前缀。
pub fn sm_aggregate_event_prefix(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    aggregate_id: &str,
) -> Vec<u8> {
    aggregate_instance_prefix(
        shard_id,
        SM_AGGREGATE_EVENT,
        aggregate_type,
        partition_id,
        aggregate_id,
    )
}

/// 判断 key 是否为指定 Shard 的聚合事件本体。
///
/// 快照恢复用它统计恢复事件数，避免在调用方复制状态机 tag 布局。
pub(crate) fn is_aggregate_event_key(shard_id: u64, key: &[u8]) -> bool {
    key.len() >= 10
        && key[0] == TAG_SM
        && key[1..9] == shard_id.to_be_bytes()
        && key[9] == SM_AGGREGATE_EVENT
}

/// 单聚合实例的当前版本元数据 key。
pub fn sm_aggregate_meta(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    aggregate_id: &str,
) -> Vec<u8> {
    aggregate_instance_prefix(
        shard_id,
        SM_AGGREGATE_META,
        aggregate_type,
        partition_id,
        aggregate_id,
    )
}

/// 分区内提交位置到聚合事件定位符的索引 key。
pub fn sm_aggregate_partition_index(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    partition_position: u64,
) -> Vec<u8> {
    let mut key = sm_aggregate_partition_index_prefix(shard_id, aggregate_type, partition_id);
    key.extend_from_slice(&encode_u64_be(partition_position));
    key
}

/// 单虚拟事件分区的提交位置索引前缀。
pub fn sm_aggregate_partition_index_prefix(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
) -> Vec<u8> {
    aggregate_partition_prefix(
        shard_id,
        SM_AGGREGATE_PARTITION_INDEX,
        aggregate_type,
        partition_id,
    )
}

/// 单虚拟事件分区的下一个提交位置计数器 key。
pub fn sm_aggregate_next_position(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
) -> Vec<u8> {
    aggregate_partition_prefix(
        shard_id,
        SM_AGGREGATE_NEXT_POSITION,
        aggregate_type,
        partition_id,
    )
}

/// 聚合实例业务状态文档 key。
pub fn sm_aggregate_state(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    aggregate_id: &str,
) -> Vec<u8> {
    let mut key =
        aggregate_partition_prefix(shard_id, SM_AGGREGATE_STATE, aggregate_type, partition_id);
    // aggregate_id 受公共 ASCII 规则约束且 key 到此结束，可直接编码以保留词典序。
    key.extend_from_slice(aggregate_id.as_bytes());
    key
}

/// 聚合实例业务状态最后提交 HLC key。
///
/// 与状态内容使用不同命名空间，避免改变已持久化 `AggregateState` 的 bincode 格式。
pub fn sm_aggregate_state_modified(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    aggregate_id: &str,
) -> Vec<u8> {
    let mut key = aggregate_partition_prefix(
        shard_id,
        SM_AGGREGATE_STATE_MODIFIED,
        aggregate_type,
        partition_id,
    );
    key.extend_from_slice(aggregate_id.as_bytes());
    key
}

/// 单虚拟事件分区的业务状态扫描前缀。
pub fn sm_aggregate_state_prefix(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
) -> Vec<u8> {
    aggregate_partition_prefix(shard_id, SM_AGGREGATE_STATE, aggregate_type, partition_id)
}

/// 从业务状态 key 解出聚合实例 ID。
pub fn decode_aggregate_state_key(key: &[u8]) -> Option<String> {
    const HEAD: usize = 10;
    if key.len() < HEAD + 8 + 2 + 1 || key.get(9) != Some(&SM_AGGREGATE_STATE) {
        return None;
    }
    let aggregate_type_len = decode_u64_be(&key[HEAD..HEAD + 8]).ok()? as usize;
    let aggregate_start = HEAD
        .checked_add(8)?
        .checked_add(aggregate_type_len)?
        .checked_add(2)?;
    if aggregate_start >= key.len() {
        return None;
    }
    String::from_utf8(key[aggregate_start..].to_vec()).ok()
}

/// 聚合事件的幂等索引 key；作用域限制在聚合类型和虚拟分区内。
pub fn sm_aggregate_idempotency(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    event_id: &uuid::Uuid,
) -> Vec<u8> {
    let mut key = aggregate_partition_prefix(
        shard_id,
        SM_AGGREGATE_IDEMPOTENCY,
        aggregate_type,
        partition_id,
    );
    key.extend_from_slice(event_id.as_bytes());
    key
}

/// 数据 Shard 上虚拟事件分区的 generation fence key。
pub fn sm_aggregate_partition_fence(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
) -> Vec<u8> {
    aggregate_partition_prefix(
        shard_id,
        SM_AGGREGATE_PARTITION_FENCE,
        aggregate_type,
        partition_id,
    )
}

/// 控制 Shard 上唯一的聚合类型 catalog key。
pub fn sm_aggregate_catalog(shard_id: u64) -> Vec<u8> {
    sm_sub_prefix(shard_id, SM_AGGREGATE_CATALOG)
}

/// 控制 Shard 上唯一的聚合消费者组 catalog key。
pub fn sm_aggregate_group_catalog(shard_id: u64) -> Vec<u8> {
    sm_sub_prefix(shard_id, SM_AGGREGATE_GROUP_CATALOG)
}

/// 数据 Shard 上单个事件分区的消费者组进度 key。
pub fn sm_aggregate_group_partition(
    shard_id: u64,
    aggregate_type: &es_core::AggregateTypeId,
    partition_id: u16,
    group_name: &str,
) -> Vec<u8> {
    let mut key = aggregate_partition_prefix(
        shard_id,
        SM_AGGREGATE_GROUP_PARTITION,
        aggregate_type,
        partition_id,
    );
    append_len_prefixed(&mut key, group_name.as_bytes());
    key
}

/// 快照 key: [0x03][shard:BE8][0x01]
pub fn snapshot_current(shard_id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(10);
    k.push(TAG_SNAPSHOT);
    k.extend_from_slice(&encode_u64_be(shard_id));
    k.push(SNAPSHOT_CURRENT);
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
/// 业务前缀 / shard 部分，越界到别的 key 段。
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
    fn aggregate_keys_are_partitioned_and_ordered() {
        let aggregate_type = es_core::AggregateTypeId::new("orders", "order").unwrap();
        let prefix = sm_aggregate_event_prefix(2, &aggregate_type, 7, "order-1");
        let zero = sm_aggregate_event(2, &aggregate_type, 7, "order-1", 0);
        let one = sm_aggregate_event(2, &aggregate_type, 7, "order-1", 1);
        assert!(zero.starts_with(&prefix));
        assert!(zero < one);
        assert!(!sm_aggregate_event(2, &aggregate_type, 8, "order-1", 0).starts_with(&prefix));
        assert!(!sm_aggregate_event(2, &aggregate_type, 7, "order-10", 0).starts_with(&prefix));
    }

    #[test]
    fn aggregate_event_key_detection_rejects_every_other_namespace() {
        let aggregate_type = es_core::AggregateTypeId::new("orders", "order").unwrap();
        let event = sm_aggregate_event(2, &aggregate_type, 7, "order-1", 0);
        assert!(is_aggregate_event_key(2, &event));
        assert!(!is_aggregate_event_key(2, &[]));
        assert!(!is_aggregate_event_key(2, &event[..9]));
        assert!(!is_aggregate_event_key(2, &raft_log_entry(2, 0)));
        assert!(!is_aggregate_event_key(3, &event));
        assert!(!is_aggregate_event_key(
            2,
            &sm_aggregate_state(2, &aggregate_type, 7, "order-1")
        ));
    }

    #[test]
    fn aggregate_position_keys_keep_numeric_order() {
        let aggregate_type = es_core::AggregateTypeId::new("orders", "order").unwrap();
        let prefix = sm_aggregate_partition_index_prefix(4, &aggregate_type, 255);
        let low = sm_aggregate_partition_index(4, &aggregate_type, 255, 255);
        let high = sm_aggregate_partition_index(4, &aggregate_type, 255, 256);
        assert!(low.starts_with(&prefix));
        assert!(low < high);
    }

    #[test]
    fn aggregate_state_keys_preserve_identifier_order_and_decode() {
        let aggregate_type = es_core::AggregateTypeId::new("orders", "order").unwrap();
        let short = sm_aggregate_state(1, &aggregate_type, 7, "a");
        let long = sm_aggregate_state(1, &aggregate_type, 7, "aa");
        let next = sm_aggregate_state(1, &aggregate_type, 7, "b");
        assert!(short < long && long < next);
        assert_eq!(decode_aggregate_state_key(&long).as_deref(), Some("aa"));
        assert_eq!(decode_aggregate_state_key(&raft_vote(1)), None);
        assert_ne!(
            sm_aggregate_state(1, &aggregate_type, 7, "a"),
            sm_aggregate_state_modified(1, &aggregate_type, 7, "a")
        );
    }

    #[test]
    fn aggregate_namespaces_are_disjoint() {
        let aggregate_type = es_core::AggregateTypeId::new("orders", "order").unwrap();
        let keys = [
            sm_aggregate_event(1, &aggregate_type, 0, "order-1", 0),
            sm_aggregate_meta(1, &aggregate_type, 0, "order-1"),
            sm_aggregate_partition_index(1, &aggregate_type, 0, 0),
            sm_aggregate_next_position(1, &aggregate_type, 0),
            sm_aggregate_state(1, &aggregate_type, 0, "order-1"),
            sm_aggregate_idempotency(1, &aggregate_type, 0, &uuid::Uuid::nil()),
            sm_aggregate_partition_fence(1, &aggregate_type, 0),
            sm_aggregate_catalog(1),
        ];
        let mut unique = std::collections::BTreeSet::new();
        for key in &keys {
            assert!(unique.insert(key[9]), "聚合状态机 tag 必须互不相同");
        }
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
}
