//! 流路由表：stream → shard 映射 + per-shard 流计数。
//!
//! 路由表是「显式分配」架构的核心元数据：`esctl create stream` 与隐式建流
//! 都由服务端在此分配 shard（大致最少流），stream 的归属记录在这里，
//! 不再由 `hash(stream_id) % shard_count` 推导。
//!
//! 持久化形态为专门的 JSON 文件（`{data_dir}/routes.json`），本模块是纯
//! 数据结构与分配逻辑（serde 可序列化，es-server 与 es-ctl 共用）。

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// 流路由表（快照）。
///
/// - `version`：集群级单调递增版本号，每次变更 +1，是跨节点仲裁的原子点
///   （接收方只采纳版本更高的表）。
/// - `streams`：stream → shard 映射。写路径权威；未知流（不在表中）由
///   服务端分配后写入。
/// - `shard_stream_counts`：per-shard 流计数，仅供「大致最少流」分配；
///   允许漂移（删除/迁移不精确扣减），由 RecountStreams 校准。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RouteTable {
    /// 表版本号（每次变更 +1）
    pub version: u64,
    /// stream → shard 映射
    pub streams: BTreeMap<String, u64>,
    /// shard → 承载的流数（分配用，允许不精确）
    pub shard_stream_counts: BTreeMap<u64, u64>,
}

impl RouteTable {
    /// 新建空表（version 0）
    pub fn new() -> Self {
        Self::default()
    }

    /// 查询 stream 归属 shard
    pub fn lookup(&self, stream: &str) -> Option<u64> {
        self.streams.get(stream).copied()
    }

    /// 记录 stream → shard（版本 +1）。
    ///
    /// 返回 `true` 表示新增记录，`false` 表示 stream 已存在（未变更，
    /// 调用方应直接使用已有归属）。已存在时不覆盖、不 bump 版本——
    /// 双检查后调用保证不重复分配。
    pub fn insert(&mut self, stream: &str, shard: u64) -> bool {
        if self.streams.contains_key(stream) {
            return false;
        }
        self.streams.insert(stream.to_string(), shard);
        *self.shard_stream_counts.entry(shard).or_insert(0) += 1;
        self.version += 1;
        true
    }

    /// 移除 stream（迁移清尾/孤儿清理用），版本 +1。返回被移除的 shard。
    pub fn remove(&mut self, stream: &str) -> Option<u64> {
        let shard = self.streams.remove(stream)?;
        if let Some(c) = self.shard_stream_counts.get_mut(&shard) {
            *c = c.saturating_sub(1);
        }
        self.version += 1;
        Some(shard)
    }

    /// 分配 stream 到「大致最少流」的 shard。
    ///
    /// - 从 `shard_set`（放置表全集）中选计数最小的；并列取最小 shard_id。
    /// - 不在 `shard_set` 的 shard（如配置已移除）不参与分配。
    /// - 计数允许漂移，「大致最少」即可（需求确认：流持续生产，不要求精确）。
    /// - `shard_set` 为空返回 `None`。
    pub fn allocate(&self, stream: &str, shard_set: &BTreeSet<u64>) -> Option<u64> {
        if self.streams.contains_key(stream) {
            return self.lookup(stream);
        }
        shard_set
            .iter()
            .copied()
            .min_by_key(|&s| (self.shard_stream_counts.get(&s).copied().unwrap_or(0), s))
    }

    /// 全表重建计数（RecountStreams 用）：把 streams 逐条重新计数。
    /// 版本 +1——recount 结果要经整表广播让集群收敛，同版本会被
    /// 接收方以「版本不高于本地」忽略，校准就只对本节点生效。
    pub fn recount(&mut self) {
        let mut counts: BTreeMap<u64, u64> = BTreeMap::new();
        for &s in self.streams.values() {
            *counts.entry(s).or_insert(0) += 1;
        }
        self.shard_stream_counts = counts;
        self.version += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> RouteTable {
        RouteTable {
            version: 7,
            streams: BTreeMap::from([
                ("a".to_string(), 1),
                ("b".to_string(), 1),
                ("c".to_string(), 2),
            ]),
            shard_stream_counts: BTreeMap::from([(1, 2), (2, 1)]),
        }
    }

    fn shard_set(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn lookup_returns_owner_shard() {
        let t = table();
        assert_eq!(t.lookup("a"), Some(1));
        assert_eq!(t.lookup("unknown"), None);
    }

    #[test]
    fn allocate_picks_least_loaded() {
        let t = table();
        // shard 2 计数最小(1) → 选 2
        assert_eq!(t.allocate("new", &shard_set(&[1, 2])), Some(2));
        // 不含计数最小 shard 的集合：只能从集合内选最少
        assert_eq!(t.allocate("new", &shard_set(&[1])), Some(1));
    }

    #[test]
    fn allocate_tie_breaks_by_smallest_id() {
        let t = RouteTable::new(); // 全 0
        assert_eq!(t.allocate("new", &shard_set(&[3, 1, 2])), Some(1));
    }

    #[test]
    fn allocate_existing_stream_returns_owner() {
        let t = table();
        // 已归属的流不参与分配，直接返回现有归属
        assert_eq!(t.allocate("a", &shard_set(&[2])), Some(1));
    }

    #[test]
    fn allocate_empty_set_none() {
        assert_eq!(table().allocate("new", &BTreeSet::new()), None);
    }

    #[test]
    fn allocate_skips_shards_not_in_set() {
        let t = table();
        // 计数最小的是 shard 2(count=1)，但集合里没有它 → 集合内 shard 3(count=0) 最小
        assert_eq!(t.allocate("new", &shard_set(&[1, 3])), Some(3));
        // 空集合内的 shard 不参与分配
        assert_eq!(t.allocate("new", &shard_set(&[1])), Some(1));
    }

    #[test]
    fn insert_new_and_existing() {
        let mut t = table();
        let v = t.version;
        assert!(t.insert("new", 2), "新流应插入");
        assert_eq!(t.version, v + 1);
        assert_eq!(t.lookup("new"), Some(2));
        assert_eq!(t.shard_stream_counts[&2], 2);

        // 已存在：不覆盖、不 bump
        let v2 = t.version;
        assert!(!t.insert("a", 2), "已存在的流不应重复插入");
        assert_eq!(t.version, v2);
        assert_eq!(t.lookup("a"), Some(1));
    }

    #[test]
    fn remove_decrements_count() {
        let mut t = table();
        let v = t.version;
        assert_eq!(t.remove("a"), Some(1));
        assert_eq!(t.version, v + 1);
        assert_eq!(t.shard_stream_counts[&1], 1);
        assert_eq!(t.remove("unknown"), None, "不存在的流无操作");
        assert_eq!(t.version, v + 1, "无操作不 bump 版本");
    }

    #[test]
    fn recount_rebuilds_counts() {
        let mut t = table();
        // 手工制造漂移：计数与 streams 不符
        t.shard_stream_counts = BTreeMap::from([(1, 99), (2, 99)]);
        t.recount();
        assert_eq!(t.shard_stream_counts, BTreeMap::from([(1, 2), (2, 1)]));
        assert_eq!(t.version, 8, "recount 应 bump 版本（使广播可被 peers 采纳）");
    }

    #[test]
    fn serde_roundtrip() {
        let t = table();
        let json = serde_json::to_string(&t).expect("序列化");
        let back: RouteTable = serde_json::from_str(&json).expect("反序列化");
        assert_eq!(back, t);
    }
}

#[cfg(test)]
mod fuzz {
    use super::*;
    use proptest::prelude::*;

    /// 分配不变量：分配的 shard 必在集合内；计数单调不变量在 insert 后成立
    proptest! {
        #[test]
        fn allocate_always_in_set(
            stream in "[a-z]{0,20}",
            shards in prop::collection::vec(0u64..8, 1..4),
            counts in prop::collection::vec(0u64..100, 0..8),
        ) {
            let set: BTreeSet<u64> = shards.into_iter().collect();
            let mut t = RouteTable::new();
            // 随机初始计数（含不在集合内的 shard 计数，验证跳过逻辑）
            for (i, c) in counts.into_iter().enumerate() {
                t.shard_stream_counts.insert(i as u64, c);
            }
            let chosen = t.allocate(&stream, &set);
            prop_assert!(chosen.is_some(), "非空集合必有分配");
            prop_assert!(set.contains(&chosen.unwrap()), "分配结果必在集合内");
        }
    }

    /// 插入-分配交替：分配出的 shard 计数在 insert 后必为所选 shard 的最小值之一
    proptest! {
        #[test]
        fn insert_then_allocate_consistent(
            streams in prop::collection::vec("[a-z]{1,10}", 1..10),
        ) {
            let set: BTreeSet<u64> = (0u64..3).collect();
            let mut t = RouteTable::new();
            for s in &streams {
                let chosen = t.allocate(s, &set).unwrap();
                t.insert(s, chosen);
            }
            // 全部流已插入（重复流名 insert 幂等）：总计数 = 唯一流数
            let total: u64 = t.shard_stream_counts.values().sum();
            let unique: std::collections::BTreeSet<&String> = streams.iter().collect();
            prop_assert_eq!(total, unique.len() as u64);
            // 计数与 streams 映射一致（recount 幂等）
            let mut expect: BTreeMap<u64, u64> = BTreeMap::new();
            for &shard in t.streams.values() {
                *expect.entry(shard).or_insert(0) += 1;
            }
            prop_assert_eq!(t.shard_stream_counts, expect);
        }
    }
}
