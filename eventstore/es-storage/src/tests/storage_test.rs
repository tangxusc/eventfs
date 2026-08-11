//! storage.rs 辅助方法测试：range 边界映射、空区间、错误转换。

use crate::key;
use crate::storage::read_logs_err;
use crate::tests::new_storage;

#[tokio::test]
async fn log_range_keys_excluded_end_le_start_none() {
    let (st, _d) = new_storage(0);
    // Excluded(end) <= start：5..5
    assert!(st.log_range_keys(&(5..5)).is_none());
    // Included(end) < start：5..=4
    assert!(st.log_range_keys(&(5..=4)).is_none());
}

#[tokio::test]
async fn log_range_keys_excluded_start_max_none() {
    let (st, _d) = new_storage(0);
    // Excluded(u64::MAX) 起点：checked_add 溢出 → 区间空
    let range: (std::ops::Bound<u64>, std::ops::Bound<u64>) =
        (std::ops::Bound::Excluded(u64::MAX), std::ops::Bound::Unbounded);
    assert!(st.log_range_keys(&range).is_none());
}

#[tokio::test]
async fn log_range_keys_included_end_max_uses_upper() {
    let (st, _d) = new_storage(0);
    // Included(u64::MAX)：+1 溢出，用 raft_log_upper 避免漏最后一条
    let (start, end) = st.log_range_keys(&(..=u64::MAX)).unwrap();
    assert_eq!(start, key::raft_log_entry(0, 0));
    assert_eq!(end, key::raft_log_upper(0));
}

#[tokio::test]
async fn log_range_keys_unbounded_bounds() {
    let (st, _d) = new_storage(0);
    let (start, end) = st.log_range_keys(&(..)).unwrap();
    assert_eq!(start, key::raft_log_entry(0, 0));
    // 左闭右开语义：上界 = 日志区后继（恰好是 vote key），排除日志区之后的一切
    assert_eq!(end, key::raft_log_upper(0));
    assert_eq!(end, key::raft_vote(0), "日志区上界应恰好是 vote key");
    assert!(key::raft_log_entry(0, u64::MAX) < end, "最大日志 key 应小于上界");
}

#[tokio::test]
async fn collect_keys_empty_or_reversed() {
    let (st, _d) = new_storage(0);
    assert!(st.collect_keys(vec![1], vec![1]).unwrap().is_empty());
    assert!(st.collect_keys(vec![2], vec![1]).unwrap().is_empty());
}

#[tokio::test]
async fn read_log_entries_empty_range() {
    let (st, _d) = new_storage(0);
    assert!(st.read_log_entries(&(5..5)).unwrap().is_empty());
    assert!(st.read_log_entries(&(5..=4)).unwrap().is_empty());
}

#[tokio::test]
async fn read_logs_err_wraps_io_error() {
    let err = es_core::Error::Internal("boom".into());
    let io = read_logs_err(err);
    assert!(io.to_string().contains("boom"), "{io}");
}
