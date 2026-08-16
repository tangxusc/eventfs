//! PayloadTooLarge 拆小重试机制测试。
//!
//! 进程内集群,在网络层注入「超过 N 条的 AppendEntries 批量被拒」行为,
//! 验证 openraft 0.9.25 收到 PayloadTooLarge 后按 hint 拆小立即重试、
//! 最终收敛——这是 es-raft/network.rs 发送前拦截所依赖的机制。
//!
//! 注意:此测试的网络层直调 Raft 句柄,不走 gRPC/network.rs;
//! network.rs 本身的改动由 network_limit_test.rs 直测。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, NetworkError, PayloadTooLarge, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft};
use tokio::sync::RwLock;

use es_core::{AggregateEvent, AggregateTypeId, ExpectedAggregateVersion, Hlc, NewAggregateEvent};
use es_storage::{EsRequest, EsResponse, EsStorage, TypeConfig};

/// 带「批量条数上限」注入的测试网络。
#[derive(Clone, Default)]
struct SplittingNet {
    inner: Arc<RwLock<SplittingInner>>,
}

#[derive(Default)]
struct SplittingInner {
    nodes: HashMap<u64, Raft<TypeConfig>>,
    /// 被切断的有向链路集合 (from, to)
    cut: HashSet<(u64, u64)>,
    /// 有向链路 (from, to) 的单次 AppendEntries 条数上限;
    /// entries > max 时被拒并计数,0 表示拒绝一切非心跳批量(含单条)。
    /// 不在表中的链路不受限(集群组建阶段必须畅通)。
    limits: HashMap<(u64, u64), usize>,
    /// 被拒次数(证明拆分确实发生)
    rejected: u64,
}

impl SplittingNet {
    async fn register(&self, id: u64, raft: Raft<TypeConfig>) {
        self.inner.write().await.nodes.insert(id, raft);
    }

    /// 双向切断两个节点之间的链路
    async fn partition(&self, a: u64, b: u64) {
        let mut g = self.inner.write().await;
        g.cut.insert((a, b));
        g.cut.insert((b, a));
    }

    /// 把某节点与其它所有节点双向隔离
    async fn isolate(&self, target: u64, all: &[u64]) {
        for &o in all {
            if o != target {
                self.partition(target, o).await;
            }
        }
    }

    /// 恢复全部链路
    async fn heal(&self) {
        let mut g = self.inner.write().await;
        g.cut.clear();
    }

    /// 设置有向链路的单次 AppendEntries 条数上限;None 恢复不受限
    async fn set_max_entries(&self, from: u64, to: u64, max: Option<usize>) {
        let mut g = self.inner.write().await;
        match max {
            Some(m) => {
                g.limits.insert((from, to), m);
            }
            None => {
                g.limits.remove(&(from, to));
            }
        }
    }

    async fn entries_limit(&self, from: u64, to: u64) -> Option<usize> {
        self.inner.read().await.limits.get(&(from, to)).copied()
    }

    async fn count_rejected(&self) {
        let mut g = self.inner.write().await;
        g.rejected += 1;
    }

    async fn rejected_count(&self) -> u64 {
        self.inner.read().await.rejected
    }

    async fn is_cut(&self, from: u64, to: u64) -> bool {
        self.inner.read().await.cut.contains(&(from, to))
    }

    async fn raft_of(&self, id: u64) -> Option<Raft<TypeConfig>> {
        // 先克隆再释放锁:后续 await 不能持锁,否则与其它节点的
        // 网络调用相互等待形成死锁
        self.inner.read().await.nodes.get(&id).cloned()
    }
}

/// 某个节点视角的网络工厂
#[derive(Clone)]
struct SplittingNodeNet {
    from: u64,
    net: SplittingNet,
}

impl RaftNetworkFactory<TypeConfig> for SplittingNodeNet {
    type Network = SplittingLink;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        SplittingLink {
            from: self.from,
            to: target,
            net: self.net.clone(),
        }
    }
}

/// 一条有向链路:超限批量直接返回 PayloadTooLarge
struct SplittingLink {
    from: u64,
    to: u64,
    net: SplittingNet,
}

impl RaftNetwork<TypeConfig> for SplittingLink {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        // 注入:该链路配置了条数上限且批量超限时拒绝(模拟对端 8MB 消息上限
        // 拒收)。与 network.rs 一致:单条超限返回 Unreachable(hint=1 会让
        // openraft 无退避地无限重试单条,烧 CPU);多条返回 PayloadTooLarge
        // 按 hint 拆小,hint 取当前条数的一半(二分收缩)。
        let limit = self.net.entries_limit(self.from, self.to).await;
        if !req.entries.is_empty()
            && let Some(max) = limit
            && (max == 0 || req.entries.len() > max)
        {
            self.net.count_rejected().await;
            if req.entries.len() <= 1 {
                return Err(RPCError::Unreachable(openraft::error::Unreachable::new(
                    &std::io::Error::other("单条 AppendEntries 超过消息上限"),
                )));
            }
            let hint = ((req.entries.len() as u64) / 2).max(1);
            return Err(RPCError::PayloadTooLarge(
                PayloadTooLarge::new_entries_hint(hint),
            ));
        }

        if self.net.is_cut(self.from, self.to).await {
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("链路 {}→{} 已被切断", self.from, self.to)),
            )));
        }
        let raft = self.net.raft_of(self.to).await.ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::other("节点未注册")))
        })?;
        raft.append_entries(req)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.to, e)))
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        if self.net.is_cut(self.from, self.to).await {
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("链路 {}→{} 已被切断", self.from, self.to)),
            )));
        }
        let raft = self.net.raft_of(self.to).await.ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::other("节点未注册")))
        })?;
        raft.install_snapshot(req)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.to, e)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _o: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.net.is_cut(self.from, self.to).await {
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other(format!("链路 {}→{} 已被切断", self.from, self.to)),
            )));
        }
        let raft = self.net.raft_of(self.to).await.ok_or_else(|| {
            RPCError::Network(NetworkError::new(&std::io::Error::other("节点未注册")))
        })?;
        raft.vote(req)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.to, e)))
    }
}

/// 进程内 3 节点集群
struct Cluster {
    net: SplittingNet,
    rafts: BTreeMap<u64, Raft<TypeConfig>>,
    stores: BTreeMap<u64, EsStorage>,
    ids: Vec<u64>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Cluster {
    /// 建 3 节点集群并组建为一个 Raft group（分片 0）
    async fn start() -> Self {
        let ids = vec![1u64, 2, 3];
        let net = SplittingNet::default();
        let mut rafts = BTreeMap::new();
        let mut stores = BTreeMap::new();
        let mut dirs = Vec::new();

        for &id in &ids {
            let dir = tempfile::tempdir().expect("临时目录");
            let tree = Arc::new(
                surrealkv::TreeBuilder::new()
                    .with_path(dir.path().to_path_buf())
                    .build()
                    .expect("开 tree"),
            );
            let store = EsStorage::new(
                0,
                tree,
                es_storage::snapshot::SnapshotConfig {
                    dir: dir.path().join("snapshots"),
                    ..Default::default()
                },
            )
            .expect("建存储");
            store.restore_applied_state().await.expect("恢复状态");

            let cfg = Arc::new(
                Config {
                    cluster_name: "splitting-test".into(),
                    heartbeat_interval: 100,
                    election_timeout_min: 200,
                    election_timeout_max: 400,
                    snapshot_policy: openraft::SnapshotPolicy::Never,
                    max_in_snapshot_log_to_keep: 1000,
                    ..Default::default()
                }
                .validate()
                .expect("校验配置"),
            );

            let raft = Raft::new(
                id,
                cfg,
                SplittingNodeNet {
                    from: id,
                    net: net.clone(),
                },
                store.clone(),
                store.clone(),
            )
            .await
            .expect("建 Raft");

            net.register(id, raft.clone()).await;
            rafts.insert(id, raft);
            stores.insert(id, store);
            dirs.push(dir);
        }

        let c = Self {
            net,
            rafts,
            stores,
            ids,
            _dirs: dirs,
        };

        // 单成员自举后逐个加入,避免空节点同时竞选导致活锁
        let members: BTreeMap<u64, BasicNode> =
            [(1u64, BasicNode::default())].into_iter().collect();
        c.rafts[&1].initialize(members).await.expect("initialize");
        c.wait_leader(Duration::from_secs(5)).await;

        for &id in &[2u64, 3] {
            c.rafts[&1]
                .add_learner(id, BasicNode::default(), true)
                .await
                .expect("add_learner");
        }
        let voters: BTreeSet<u64> = c.ids.iter().copied().collect();
        c.rafts[&1]
            .change_membership(voters, false)
            .await
            .expect("change_membership");

        c
    }

    fn is_leader(&self, id: u64) -> bool {
        self.rafts[&id].metrics().borrow().state.is_leader()
    }

    fn last_applied(&self, id: u64) -> u64 {
        self.rafts[&id]
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0)
    }

    /// 等到出现 leader，返回其 id
    async fn wait_leader(&self, timeout: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for &id in &self.ids {
                if self.is_leader(id) {
                    return id;
                }
            }
            if tokio::time::Instant::now() > deadline {
                panic!("等待 leader 超时");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 经指定节点写入一条事件
    async fn write(
        &self,
        node: u64,
        aggregate_id: &str,
        data: &[u8],
    ) -> Result<EsResponse, String> {
        let req = EsRequest::AggregateAppend {
            aggregate_type: test_aggregate_type(),
            partition_id: 0,
            partition_generation: 0,
            aggregate_id: aggregate_id.to_string(),
            expected_version: ExpectedAggregateVersion::Any,
            event: NewAggregateEvent {
                event_id: uuid::Uuid::new_v4(),
                event_type: "E".into(),
                data: data.to_vec(),
                metadata: vec![],
            },
            hlc: Hlc::now(),
        };
        self.rafts[&node]
            .client_write(req)
            .await
            .map(|r| r.data)
            .map_err(|e| e.to_string())
    }

    /// 读取某节点本地状态机里的聚合实例事件。
    fn read(&self, node: u64, aggregate_id: &str) -> Vec<AggregateEvent> {
        self.stores[&node]
            .read_aggregate_partition_events(&test_aggregate_type(), 0, 0, 0)
            .expect("读取聚合事件")
            .into_iter()
            .filter(|event| event.aggregate_id == aggregate_id)
            .collect()
    }

    async fn shutdown(self) {
        for (_, r) in self.rafts {
            let _ = r.shutdown().await;
        }
        for (_, s) in self.stores {
            let _ = s.close().await;
        }
    }
}

fn test_aggregate_type() -> AggregateTypeId {
    AggregateTypeId::new("tests", "payload-shrink").expect("合法 AggregateType")
}

/// 超限批量被拆小重试后收敛:落后节点最终追平,数据一致。
///
/// 场景:隔离 follower 2 期间 leader 写 30 条 → 恢复链路 → leader 一次性
/// 推 30 条被拒(上限 5 条)→ openraft 按 hint 拆小 → 收敛。
#[tokio::test]
async fn oversized_batch_splits_and_converges() {
    let c = Cluster::start().await;
    // 集群组建期间不注入限制,等它就绪
    let leader = c.wait_leader(Duration::from_secs(5)).await;
    let follower = [1u64, 2, 3]
        .into_iter()
        .find(|&id| id != leader)
        .expect("存在 follower");

    // 隔离 follower,让日志在 leader 侧积压
    c.net.isolate(follower, &c.ids).await;
    for i in 0..30 {
        c.write(leader, "s", format!("event-{i}").as_bytes())
            .await
            .expect("写入");
    }
    // 等 leader 侧全部落盘(多数派 = leader + 另一个 follower)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while c.last_applied(leader) < 30 + 4 {
        if tokio::time::Instant::now() > deadline {
            panic!("leader 未在期限内提交全部日志");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 先注入限制再恢复链路：heal 后 leader 对被隔离 follower 的首个批量
    // （30 条整批）必然被拒；若先 heal 后设置，两者之间有个微秒级窗口——
    // 恰好落在窗口内的重试会整批通过，rejected 计数保持 0 导致断言误报。
    c.net.set_max_entries(leader, follower, Some(5)).await;
    c.net.heal().await;

    // follower 追赶:数据最终全部到达
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while c.read(follower, "s").len() < 30 {
        if tokio::time::Instant::now() > deadline {
            panic!(
                "follower{follower} 未在期限内追平,已到 {} 条,被拒 {} 次",
                c.read(follower, "s").len(),
                c.net.rejected_count().await
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 拆分确实发生过
    let rejected = c.net.rejected_count().await;
    assert!(rejected > 0, "应发生至少一次批量拒绝(拆分),实际 0 次");

    // 三节点数据逐字节一致
    for &id in &c.ids {
        let got: Vec<Vec<u8>> = c.read(id, "s").iter().map(|e| e.data.clone()).collect();
        let want: Vec<Vec<u8>> = (0..30).map(|i| format!("event-{i}").into_bytes()).collect();
        assert_eq!(got, want, "node{id} 数据不一致");
    }

    c.shutdown().await;
}

/// 单条批量被拒(上限 0 = 一切非心跳批量都拒)时 openraft 不 panic,
/// 集群仍可用:leader 保持提交能力(多数派 = 其它两个节点)。
///
/// network.rs 对单条超限返回 Unreachable 而非 hint=1(避免死循环),
/// 本测试验证 openraft 对 Unreachable 的退避路径不崩溃。
#[tokio::test]
async fn single_oversized_entry_returns_unreachable_no_panic() {
    let c = Cluster::start().await;
    let leader = c.wait_leader(Duration::from_secs(5)).await;

    // 只拒绝 leader→follower 方向的非心跳批量(含单条):该方向复制停滞,
    // 但 leader 与另一 follower 的复制正常,集群仍可提交
    let follower = [1u64, 2, 3]
        .into_iter()
        .find(|&id| id != leader)
        .expect("存在 follower");
    c.net.set_max_entries(leader, follower, Some(0)).await;

    // 写一条事件:leader 提交需要多数派,其它两个 follower 正常接收,
    // 因此写入应成功(复制停滞不影响提交)
    c.write(leader, "s", b"e1").await.expect("写入应成功");

    // 等复制尝试至少发生一次(被拒 ≥ 1),证明拒绝路径被触发且未 panic
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while c.net.rejected_count().await == 0 {
        if tokio::time::Instant::now() > deadline {
            panic!("复制拒绝未发生");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 再写一条:openraft 未因 Unreachable 崩溃,集群仍可提交
    c.write(leader, "s", b"e2").await.expect("写入仍应成功");

    c.shutdown().await;
}
