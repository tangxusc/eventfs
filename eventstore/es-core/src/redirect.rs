//! leader 重定向协议解析与重试策略。
//!
//! 服务端(es-server/src/service.rs `client_write_to_status`)在写请求打到
//! 非 leader 节点时返回 `Unavailable`,message 中编码 leader 提示:
//! - `"not leader; leader_id=3 leader_addr=http://127.0.0.1:50052"` → 可重定向
//! - `"not leader; leader unknown, retry later"` → 选举中,稍后重试
//!
//! 本模块提供解析函数与预算制重试队列,供 es-client(SDK)与 es-ctl(CLI)
//! 共用;纯 std,不引入 tonic/tokio 依赖。

use std::collections::{HashSet, VecDeque};

/// 解析服务端返回的 leader 重定向提示。
///
/// 格式(见模块文档):`"not leader; leader_id={id} leader_addr={addr}"`。
/// `leader_addr=` 为空(openraft 未填充 leader 节点地址)时视为无提示。
///
/// # 返回
/// - `Some(addr)`:重定向地址,如 `"http://127.0.0.1:50052"`
/// - `None`:无重定向地址(选举中 `"leader unknown"`、空地址或其它文本)
pub fn parse_leader_hint(msg: &str) -> Option<String> {
    let rest = msg.strip_prefix("not leader;")?.trim();
    let rest = rest.strip_prefix("leader_id=")?;
    let (_id, addr) = rest.split_once(" leader_addr=")?;
    let addr = addr.trim();
    if addr.is_empty() {
        None
    } else {
        Some(addr.to_string())
    }
}

/// leader 重定向重试计划:预算制队列。
///
/// 与 es-ctl 旧实现(`ClusterClient::with_leader`)逐行等价,抽为独立策略对象
/// 供 es-client / es-ctl 共用。重定向地址可能不在初始端点列表,故预算为
/// `端点数 × 2 + 2`。
///
/// 语义约定:
/// - [`Self::next`] 每取一个目标(含被去重跳过的)消耗 1 份预算
/// - [`Self::redirect_to`] 重定向地址插队到队首;**即使已尝试过也允许重试**
///   (集群可能正处于选举/故障恢复中,重定向提示本身说明状态已变化;
///   预算有界兜底,不会无限循环)
/// - [`Self::retry_later`] 目标先移出已试集合再入队尾,否则重入队后会被
///   去重挡下、重试永远不会发生(es-ctl 记录过的死代码陷阱);
///   下次轮到它时 [`Self::needs_backoff`] 返回 true,调用方应退避后再试
pub struct LeaderRetryPlan {
    /// 剩余尝试预算
    budget: usize,
    /// 待尝试队列:front 优先
    queue: VecDeque<String>,
    /// 已尝试过的目标
    tried: HashSet<String>,
    /// 最近一次 [`Self::retry_later`] 的目标:下次轮到它时需要退避
    backoff_target: Option<String>,
}

impl LeaderRetryPlan {
    /// 创建重试计划。
    ///
    /// # 参数
    /// - `endpoints`:初始尝试目标列表,顺序即尝试顺序
    ///
    /// 预算 = `端点数 × 2 + 2`。
    pub fn new(endpoints: impl IntoIterator<Item = String>) -> Self {
        let queue: VecDeque<String> = endpoints.into_iter().collect();
        let budget = queue.len() * 2 + 2;
        Self {
            budget,
            queue,
            tried: HashSet::new(),
            backoff_target: None,
        }
    }

    /// 取下一个未尝试过的目标。
    ///
    /// 每调用一次消耗 1 份预算;队列耗尽(无目标)或预算耗尽时返回 `None`。
    /// 目标可能因重定向/重入队出现多次,已尝试过的会被跳过(同样消耗预算)。
    /// 取出后可用 [`Self::needs_backoff`] 判断本次是否需退避再发请求。
    pub fn next(&mut self) -> Option<String> {
        loop {
            if self.budget == 0 {
                return None;
            }
            self.budget -= 1;
            let target = self.queue.pop_front()?;
            if self.tried.insert(target.clone()) {
                return Some(target);
            }
        }
    }

    /// 重定向地址插队到队首,优先尝试。
    ///
    /// 已尝试过的目标同样入队并允许重试(集群状态可能已变化),
    /// 由预算有界兜底。
    pub fn redirect_to(&mut self, addr: String) {
        self.tried.remove(&addr);
        self.queue.push_front(addr);
    }

    /// 目标稍后重试:移出已试集合后入队尾。
    ///
    /// 用于选举中(`leader unknown`)等暂时性失败;队尾顺序保证先试完
    /// 队列里其它端点,避免死等。下次轮到该目标时 [`Self::needs_backoff`]
    /// 返回 true。
    pub fn retry_later(&mut self, target: String) {
        self.tried.remove(&target);
        self.backoff_target = Some(target.clone());
        self.queue.push_back(target);
    }

    /// 本次取出的目标是否刚被 [`Self::retry_later`] 重入队(应退避后再发请求)。
    ///
    /// 仅在最近一次重入队的目标上返回 true:队列里其它节点先被取到时
    /// 无需退避,避免无谓延迟("先把其它节点试完")。
    pub fn needs_backoff(&self, target: &str) -> bool {
        self.backoff_target.as_deref() == Some(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_leader_hint_with_addr() {
        assert_eq!(
            parse_leader_hint("not leader; leader_id=3 leader_addr=http://127.0.0.1:50052"),
            Some("http://127.0.0.1:50052".to_string())
        );
    }

    #[test]
    fn parse_leader_hint_bare_addr() {
        assert_eq!(
            parse_leader_hint("not leader; leader_id=1 leader_addr=127.0.0.1:50051"),
            Some("127.0.0.1:50051".to_string())
        );
    }

    #[test]
    fn parse_leader_hint_electing_returns_none() {
        assert_eq!(
            parse_leader_hint("not leader; leader unknown, retry later"),
            None
        );
    }

    #[test]
    fn parse_leader_hint_noise_returns_none() {
        assert_eq!(parse_leader_hint("internal error: boom"), None);
        assert_eq!(parse_leader_hint(""), None);
        assert_eq!(
            parse_leader_hint("not leader; leader_id=3"),
            None,
            "缺地址段"
        );
        assert_eq!(
            parse_leader_hint("not leader; leader_id= leader_addr="),
            None,
            "空地址"
        );
    }

    #[test]
    fn plan_follows_initial_order_then_exhausts() {
        let mut plan = LeaderRetryPlan::new(["a".into(), "b".into(), "c".into()]);
        assert_eq!(plan.next(), Some("a".to_string()));
        assert_eq!(plan.next(), Some("b".to_string()));
        assert_eq!(plan.next(), Some("c".to_string()));
        // 队列耗尽,返回 None
        assert_eq!(plan.next(), None);
    }

    #[test]
    fn plan_redirect_gets_priority() {
        let mut plan = LeaderRetryPlan::new(["a".into(), "b".into()]);
        assert_eq!(plan.next(), Some("a".to_string()));
        // 从 a 收到重定向地址 c,应优先于 b 尝试
        plan.redirect_to("c".into());
        assert_eq!(plan.next(), Some("c".to_string()));
        assert_eq!(plan.next(), Some("b".to_string()));
    }

    #[test]
    fn plan_retry_later_reenqueues_passed() {
        // es-ctl 记录过的死代码陷阱:重入队前不 remove tried,则重试永远不会发生
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        assert_eq!(plan.next(), Some("a".to_string()));
        plan.retry_later("a".into());
        assert_eq!(
            plan.next(),
            Some("a".to_string()),
            "retry_later 后应能再次尝试同一目标"
        );
    }

    #[test]
    fn plan_redirect_back_to_tried_retries() {
        // 重定向回已试过的 a:允许重试(集群状态可能已变化),预算有界兜底
        let mut plan = LeaderRetryPlan::new(["a".into(), "b".into()]);
        assert_eq!(plan.next(), Some("a".to_string()));
        plan.redirect_to("a".into());
        assert_eq!(
            plan.next(),
            Some("a".to_string()),
            "重定向到已试目标应允许重试"
        );
    }

    #[test]
    fn plan_needs_backoff_only_for_requeued() {
        // 重入队的目标再次轮到时才需要退避;队列里其它节点无需退避
        let mut plan = LeaderRetryPlan::new(["a".into(), "b".into()]);
        assert_eq!(plan.next(), Some("a".to_string()));
        assert!(!plan.needs_backoff("a"), "首次尝试无需退避");
        plan.retry_later("a".into());

        assert_eq!(plan.next(), Some("b".to_string()));
        assert!(!plan.needs_backoff("b"), "其它节点无需退避");

        assert_eq!(plan.next(), Some("a".to_string()));
        assert!(plan.needs_backoff("a"), "重入队目标再次轮到应退避");
    }

    #[test]
    fn plan_budget_exhausted_returns_none() {
        // 单端点预算 = 1×2+2 = 4:retry_later 循环 4 次后预算耗尽
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        for _ in 0..4 {
            let target = plan.next().expect("预算内应能取到目标");
            plan.retry_later(target);
        }
        assert_eq!(plan.next(), None, "预算耗尽后不再尝试");
    }
}
