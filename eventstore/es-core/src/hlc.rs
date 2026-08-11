//! 混合逻辑时钟（Hybrid Logical Clock）。
//!
//! 单节点内严格单调,HLC 由 leader 在写入前分配并随事件落盘,
//! 使日后加跨分片近似全序时无需数据迁移。

use serde::{Deserialize, Serialize};

/// 混合逻辑时钟:物理时间 + 同毫秒内逻辑计数
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Hlc {
    /// 物理时钟,毫秒
    pub wall: u64,
    /// 同毫秒内逻辑计数,若与上次 wall 相同则递增,否则归零
    pub logical: u32,
}

impl Hlc {
    /// 根据当前物理时间与上一个 HLC 生成下一个 HLC。
    /// 保证结果严格大于 prev。
    pub fn next(prev: Option<Self>, now_ms: u64) -> Self {
        match prev {
            None => Self {
                wall: now_ms,
                logical: 0,
            },
            Some(p) => {
                if now_ms > p.wall {
                    Self {
                        wall: now_ms,
                        logical: 0,
                    }
                } else {
                    // 取 max(now_ms, p.wall) 并递增 logical
                    Self {
                        wall: p.wall,
                        logical: p.logical.wrapping_add(1), // logical 溢出时环绕,极端场景
                    }
                }
            }
        }
    }

    /// 工厂方法：当前物理时间，无前驱
    ///
    /// 生产用法：leader 在 Raft 提交前调用，分配 HLC 并随请求下发。
    /// 测试用法：快速构造一个合法 HLC。
    pub fn now() -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self::next(None, ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_predecessor_returns_physical_clock() {
        let h = Hlc::next(None, 1000);
        assert_eq!(h.wall, 1000);
        assert_eq!(h.logical, 0);
    }

    #[test]
    fn logical_resets_when_wall_advances() {
        let prev = Hlc {
            wall: 1000,
            logical: 5,
        };
        let h = Hlc::next(Some(prev), 1001);
        assert_eq!(h.wall, 1001);
        assert_eq!(h.logical, 0);
    }

    #[test]
    fn logical_increments_when_wall_stays() {
        let prev = Hlc {
            wall: 1000,
            logical: 5,
        };
        let h = Hlc::next(Some(prev), 1000);
        assert_eq!(h.wall, 1000);
        assert_eq!(h.logical, 6);
    }

    #[test]
    fn wall_holds_logical_increments_on_rewind() {
        let prev = Hlc {
            wall: 1000,
            logical: 10,
        };
        let h = Hlc::next(Some(prev), 999);
        assert_eq!(h.wall, 1000); // 维持前驱 wall
        assert_eq!(h.logical, 11);
    }

    #[test]
    fn monotonic_strictly_increasing() {
        let mut prev = None;
        let mut last = Hlc {
            wall: 0,
            logical: 0,
        };
        for t in [1000, 1000, 1001, 1000, 1002] {
            let h = Hlc::next(prev, t);
            assert!(h > last, "{h:?} 应严格大于 {last:?}");
            last = h;
            prev = Some(h);
        }
    }
}
