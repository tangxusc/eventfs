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
/// - [`Self::next`] 出队优先于预算检查:队列里每取一个目标,**有效尝试**
///   消耗 1 份预算,被去重跳过的(队列中重复目标)不消耗
/// - 预算耗尽后仅 **重定向目标** 可继续尝试,受 [`Self::redirect_tail`]
///   计数兜底——最后预算槽位上收到的 leader 地址不应被丢弃(已知地址
///   必须被联系),同时保持尝试有界(不会无限循环)
/// - [`Self::redirect_to`] 重定向地址插队到队首;**即使已尝试过也允许重试**
///   (集群可能正处于选举/故障恢复中,重定向提示本身说明状态已变化)
/// - [`Self::retry_later`] 目标先移出已试集合再入队尾,否则重入队后会被
///   去重挡下、重试永远不会发生(es-ctl 记录过的死代码陷阱);
///   下次轮到它时 [`Self::needs_backoff`] 返回 true,调用方应退避后再试
pub struct LeaderRetryPlan {
    /// 剩余尝试预算:每次有效尝试消耗 1
    budget: usize,
    /// 预算耗尽后重定向目标仍可尝试的次数(初始 = 预算)。
    /// 最后槽位收到的 leader 提示不应被丢弃;集群抖动(A↔B 互指)
    /// 时仍有界,耗尽后报 NotLeader 是正确退路
    redirect_tail: usize,
    /// 待尝试队列:front 优先
    queue: VecDeque<QueueItem>,
    /// 已尝试过的目标
    tried: HashSet<String>,
    /// 最近一次 [`Self::retry_later`] 的目标:下次轮到它时需要退避
    backoff_target: Option<String>,
}

/// 队列元素来源:重定向目标在预算耗尽后仍允许尝试;其余(初始端点、
/// retry_later 重入队)预算耗尽即止。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum RetryKind {
    /// 来自 leader 重定向提示的地址(有明确 leader,值得追着试)
    Redirect,
    /// 初始端点或 retry_later 重入队的目标(选举中,预算耗尽即放弃)
    Normal,
}

/// 待尝试目标及其来源标记
struct QueueItem {
    addr: String,
    kind: RetryKind,
}

impl LeaderRetryPlan {
    /// 创建重试计划。
    ///
    /// # 参数
    /// - `endpoints`:初始尝试目标列表,顺序即尝试顺序
    ///
    /// 预算 = `端点数 × 2 + 2`。
    pub fn new(endpoints: impl IntoIterator<Item = String>) -> Self {
        let queue: VecDeque<QueueItem> = endpoints
            .into_iter()
            .map(|addr| QueueItem {
                addr,
                kind: RetryKind::Normal,
            })
            .collect();
        let budget = queue.len() * 2 + 2;
        Self {
            budget,
            redirect_tail: budget,
            queue,
            tried: HashSet::new(),
            backoff_target: None,
        }
    }

    /// 重定向地址插队到队首,优先尝试。
    ///
    /// 已尝试过的目标同样入队并允许重试(集群状态可能已变化);
    /// 预算耗尽后仍可尝试,由 [`Self::redirect_tail`] 兜底有界。
    pub fn redirect_to(&mut self, addr: String) {
        self.tried.remove(&addr);
        self.queue.push_front(QueueItem {
            addr,
            kind: RetryKind::Redirect,
        });
    }

    /// 目标稍后重试:移出已试集合后入队尾。
    ///
    /// 用于选举中(`leader unknown`)等暂时性失败;队尾顺序保证先试完
    /// 队列里其它端点,避免死等。下次轮到该目标时 [`Self::needs_backoff`]
    /// 返回 true。该目标受预算限制:预算耗尽即不再尝试。
    pub fn retry_later(&mut self, target: String) {
        self.tried.remove(&target);
        self.backoff_target = Some(target.clone());
        self.queue.push_back(QueueItem {
            addr: target,
            kind: RetryKind::Normal,
        });
    }

    /// 本次取出的目标是否刚被 [`Self::retry_later`] 重入队(应退避后再发请求)。
    ///
    /// 仅在最近一次重入队的目标上返回 true:队列里其它节点先被取到时
    /// 无需退避,避免无谓延迟("先把其它节点试完")。
    pub fn needs_backoff(&self, target: &str) -> bool {
        self.backoff_target.as_deref() == Some(target)
    }
}

impl Iterator for LeaderRetryPlan {
    type Item = String;

    /// 取下一个待尝试的目标。
    ///
    /// 出队优先于预算检查。重复目标不消耗预算；预算耗尽后只允许有界地继续
    /// 尝试 leader 重定向目标。取出后可用 [`LeaderRetryPlan::needs_backoff`]
    /// 判断本次是否需退避再发请求。
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // 最后预算槽位收到的重定向地址必须仍有机会被尝试。
            let item = self.queue.pop_front()?;
            if !self.tried.insert(item.addr.clone()) {
                continue;
            }
            if self.budget == 0 {
                if item.kind == RetryKind::Redirect && self.redirect_tail > 0 {
                    self.redirect_tail -= 1;
                    return Some(item.addr);
                }
                continue;
            }
            self.budget -= 1;
            return Some(item.addr);
        }
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

    #[test]
    fn plan_last_slot_redirect_not_dropped() {
        // 单端点预算 4:前 3 次选举中无提示(循环 retry_later),第 4 次
        // (最后预算槽位)收到带 leader_addr 的重定向 → 地址不应被预算丢弃
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        for _ in 0..3 {
            let target = plan.next().expect("预算内应能取到目标");
            plan.retry_later(target);
        }
        assert_eq!(plan.next(), Some("a".to_string()), "第 4 次(预算 1→0)");
        plan.redirect_to("a".into()); // 最后槽位收到重定向地址
        assert_eq!(
            plan.next(),
            Some("a".to_string()),
            "预算耗尽后重定向地址仍应被尝试"
        );
    }

    #[test]
    fn plan_budget_zero_skips_normal_targets() {
        // 预算耗尽后 retry_later 重入队的选举中目标不再尝试(有界性保持)
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        for _ in 0..4 {
            let target = plan.next().expect("预算内应能取到目标");
            plan.retry_later(target);
        }
        plan.retry_later("a".into());
        assert_eq!(plan.next(), None, "预算耗尽后选举中目标终止");
    }

    #[test]
    fn plan_dedup_skip_does_not_consume_budget() {
        // 队列中重复目标被去重跳过时不消耗预算
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        assert_eq!(plan.next(), Some("a".to_string()), "预算 4→3");
        plan.redirect_to("a".into());
        plan.redirect_to("a".into()); // 队列 [a, a]
        assert_eq!(plan.next(), Some("a".to_string()), "有效尝试 3→2");
        assert_eq!(plan.next(), None, "重复目标去重跳过,队列耗尽");
        assert_eq!(plan.budget, 2, "去重跳过不应消耗预算");
    }

    #[test]
    fn plan_redirect_tail_bounds_post_budget_attempts() {
        // 预算耗尽后重定向目标可尝试的次数受 redirect_tail(= 初始预算)限制
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        for _ in 0..4 {
            let target = plan.next().expect("预算内应能取到目标");
            plan.retry_later(target);
        }
        assert_eq!(plan.next(), None, "预算耗尽");
        let tail = plan.redirect_tail;
        assert_eq!(tail, 4, "redirect_tail 初始 = 预算");
        for _ in 0..tail {
            plan.redirect_to("a".into());
            assert_eq!(plan.next(), Some("a".to_string()), "tail 内重定向目标可试");
        }
        assert_eq!(plan.next(), None, "tail 耗尽后队列为空");
    }

    #[test]
    fn plan_redirect_after_tail_exhausted_returns_none() {
        // tail 耗尽后即使再收到重定向也返回 None(报 NotLeader 是正确退路)
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        for _ in 0..4 {
            let target = plan.next().expect("预算内应能取到目标");
            plan.retry_later(target);
        }
        for _ in 0..plan.redirect_tail {
            plan.redirect_to("a".into());
            assert!(plan.next().is_some(), "tail 内重定向目标可试");
        }
        plan.redirect_to("a".into());
        assert_eq!(plan.next(), None, "tail 耗尽后重定向目标也终止");
    }

    #[test]
    fn plan_mixed_budget_zero_redirect_then_normal() {
        // 预算 0 后重定向目标仍被尝试,其余目标终止(不无限重试)
        let mut plan = LeaderRetryPlan::new(["a".into()]);
        for _ in 0..4 {
            let target = plan.next().expect("预算内应能取到目标");
            plan.retry_later(target);
        }
        plan.redirect_to("a".into()); // 最后槽位重定向
        assert_eq!(
            plan.next(),
            Some("a".to_string()),
            "重定向目标在预算 0 后仍可尝试"
        );
        assert_eq!(plan.next(), None, "预算 0 后其它目标终止");
    }
}
