//! 分片路由：按 stream_id 确定分片归属。
//!
//! 选用 xxh3 而非 `DefaultHasher`:后者跨 Rust 版本不保证稳定，
//! 用于持久化路由时会在版本升级后导致数据错位。xxh3 算法固定且高速。

/// 计算 stream 所属的分片 ID。
///
/// shard_count 在启动时确定，运行期不可变。变更需数据重分布，本期不实现。
#[inline]
pub fn route(stream_id: &str, shard_count: u64) -> u64 {
    xxhash_rust::xxh3::xxh3_64(stream_id.as_bytes()) % shard_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_stream_same_shard() {
        let s = "test-stream";
        let shard = route(s, 16);
        for _ in 0..10 {
            assert_eq!(route(s, 16), shard);
        }
    }

    #[test]
    fn different_streams_spread_across_shards() {
        let shards: std::collections::HashSet<_> = (0..100)
            .map(|i| route(&format!("stream-{i}"), 16))
            .collect();
        // 100 个流按 16 分片分布，至少触及 10 个分片（概率性，极低概率失败但可重跑）
        assert!(shards.len() >= 10, "实际分布 {} 个分片，疑似坍缩", shards.len());
    }

    #[test]
    fn empty_stream_routable() {
        let _s = route("", 16);
    }

    /// 用 proptest 验证分片 ID 始终在合法范围内
    #[test]
    fn shard_id_in_valid_range() {
        use proptest::prelude::*;
        proptest!(|(s in ".*", c in 1u64..=256)| {
            let shard = route(&s, c);
            assert!(shard < c, "shard={shard} 应 < {c}");
        });
    }
}
