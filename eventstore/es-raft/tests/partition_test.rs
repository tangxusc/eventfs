//! 网络分区测试。
//!
//! 用进程内集群而非多进程：分区需要按「有向链路」精确控制 RPC 通断
//! （A→B 断但 B→A 通），多进程下 TCP 代理只能按节点粒度屏蔽，
//! 且 gRPC 客户端源端口随机，无法按来源过滤。
//! 进程内自建网络层可直接维护一张链路矩阵，确定且无需真实网络。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft};
use tokio::sync::RwLock;

use es_core::{ExpectedVersion, Hlc, NewEvent};
use es_storage::{EsRequest, EsResponse, EsStorage, TypeConfig};

/// 进程内测试网络：持有各节点的 Raft 句柄，并维护一张有向链路通断矩阵。
#[derive(Clone, Default)]
struct TestNet {
    inner: Arc<RwLock<TestNetInner>>,
}

#[derive(Default)]
struct TestNetInner {
    /// node_id -> Raft 实例。Raft::new 之后才能填，故用后置注册。
    nodes: HashMap<u64, Raft<TypeConfig>>,
    /// 被切断的有向链路集合 (from, to)
    cut: HashSet<(u64, u64)>,
    /// 有向链路延迟（单位:毫秒）
    delay: HashMap<(u64, u64), u64>,
}

impl TestNet {
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
        g.delay.clear();
    }

    /// 设置有向链路延迟（毫秒）。0 表示清除延迟。
    async fn set_delay(&self, from: u64, to: u64, ms: u64) {
        let mut g = self.inner.write().await;
        if ms == 0 {
            g.delay.remove(&(from, to));
        } else {
            g.delay.insert((from, to), ms);
        }
    }

    async fn is_cut(&self, from: u64, to: u64) -> bool {
        self.inner.read().await.cut.contains(&(from, to))
    }

    async fn get_delay(&self, from: u64, to: u64) -> u64 {
        self.inner
            .read()
            .await
            .delay
            .get(&(from, to))
            .copied()
            .unwrap_or(0)
    }

    async fn raft_of(&self, id: u64) -> Option<Raft<TypeConfig>> {
        // 先克隆再释放锁：后续 await 不能持锁，否则与其它节点的
        // 网络调用相互等待形成死锁
        self.inner.read().await.nodes.get(&id).cloned()
    }
}

/// 某个节点视角的网络工厂
#[derive(Clone)]
struct NodeNet {
    from: u64,
    net: TestNet,
}

impl RaftNetworkFactory<TypeConfig> for NodeNet {
    type Network = Link;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        Link {
            from: self.from,
            to: target,
            net: self.net.clone(),
        }
    }
}

/// 一条有向链路
struct Link {
    from: u64,
    to: u64,
    net: TestNet,
}

fn unreachable<E: std::error::Error>(from: u64, to: u64) -> RPCError<u64, BasicNode, E> {
    RPCError::Network(NetworkError::new(&std::io::Error::other(format!(
        "链路 {from}→{to} 已被切断"
    ))))
}

impl Link {
    /// 检查链路是否可用，施加延迟，并取出目标节点的 Raft 句柄
    async fn target<E: std::error::Error>(
        &self,
    ) -> Result<Raft<TypeConfig>, RPCError<u64, BasicNode, E>> {
        if self.net.is_cut(self.from, self.to).await {
            return Err(unreachable(self.from, self.to));
        }
        // 人为延迟：在请求发出前睡眠，模拟慢网络或慢节点
        let ms = self.net.get_delay(self.from, self.to).await;
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        self.net
            .raft_of(self.to)
            .await
            .ok_or_else(|| unreachable(self.from, self.to))
    }
}

impl RaftNetwork<TypeConfig> for Link {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self.target().await?;
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
        let raft = self.target().await?;
        raft.install_snapshot(req)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.to, e)))
    }

    async fn vote(
        &mut self,
        req: VoteRequest<u64>,
        _o: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let raft = self.target().await?;
        raft.vote(req)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.to, e)))
    }
}

/// 进程内 3 节点集群
struct Cluster {
    net: TestNet,
    rafts: BTreeMap<u64, Raft<TypeConfig>>,
    stores: BTreeMap<u64, EsStorage>,
    ids: Vec<u64>,
    _dirs: Vec<tempfile::TempDir>,
}

/// 时序配置。默认用快速超时让分区测试不至于等太久；
/// 延迟测试需要更长的超时以容纳人为延迟。
struct Timing {
    heartbeat_ms: u64,
    election_min_ms: u64,
    election_max_ms: u64,
    /// 距上次快照多少条日志后自动触发快照。None 表示不自动快照。
    snapshot_after_logs: Option<u64>,
    /// 快照后保留多少条日志(超出的会被 purge,落后节点只能靠快照追赶)
    keep_logs_after_snapshot: u64,
    /// 快照分块大小(字节),默认 3MiB 与 openraft 一致
    snapshot_max_chunk_size: u64,
}

impl Default for Timing {
    fn default() -> Self {
        Self {
            heartbeat_ms: 100,
            election_min_ms: 200,
            election_max_ms: 400,
            snapshot_after_logs: None,
            keep_logs_after_snapshot: 1000,
            snapshot_max_chunk_size: 3 * 1024 * 1024,
        }
    }
}

impl Cluster {
    /// 建 3 节点集群并组建为一个 Raft group（分片 0）
    async fn start() -> Self {
        Self::start_with_timing(Timing::default()).await
    }

    /// 建 3 节点集群，指定时序参数
    async fn start_with_timing(timing: Timing) -> Self {
        let ids = vec![1u64, 2, 3];
        let net = TestNet::default();
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
                    cluster_name: "partition-test".into(),
                    heartbeat_interval: timing.heartbeat_ms,
                    election_timeout_min: timing.election_min_ms,
                    election_timeout_max: timing.election_max_ms,
                    // 快照策略:达到阈值自动建快照,并 purge 掉多余日志。
                    // 这是快照的核心用途——让落后太多的节点不必重放全部日志。
                    snapshot_policy: match timing.snapshot_after_logs {
                        Some(n) => openraft::SnapshotPolicy::LogsSinceLast(n),
                        None => openraft::SnapshotPolicy::Never,
                    },
                    max_in_snapshot_log_to_keep: timing.keep_logs_after_snapshot,
                    snapshot_max_chunk_size: timing.snapshot_max_chunk_size,
                    ..Default::default()
                }
                .validate()
                .expect("校验配置"),
            );

            let raft = Raft::new(
                id,
                cfg,
                NodeNet {
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

        // 单成员自举后逐个加入，理由同多进程测试：避免空节点同时竞选导致活锁
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

    fn term(&self, id: u64) -> u64 {
        self.rafts[&id].metrics().borrow().current_term
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
        self.wait_leader_among(&self.ids.clone(), timeout).await
    }

    /// 在给定候选中等待 leader
    async fn wait_leader_among(&self, cands: &[u64], timeout: Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            for &id in cands {
                if self.is_leader(id) {
                    return id;
                }
            }
            if tokio::time::Instant::now() > deadline {
                let states: Vec<String> = cands
                    .iter()
                    .map(|&i| format!("node{i}={:?}", self.rafts[&i].metrics().borrow().state))
                    .collect();
                panic!("等待 leader 超时；当前状态 {states:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 等某节点不再是 leader
    async fn wait_step_down(&self, id: u64, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.is_leader(id) {
            if tokio::time::Instant::now() > deadline {
                panic!("node{id} 未在 {timeout:?} 内退位");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 等某节点追平到指定 applied
    async fn wait_applied(&self, id: u64, want: u64, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.last_applied(id) < want {
            if tokio::time::Instant::now() > deadline {
                panic!(
                    "node{id} 未在 {timeout:?} 内追平到 {want}，当前 {}",
                    self.last_applied(id)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// 经指定节点写入一条事件
    async fn write(&self, node: u64, stream: &str, data: &[u8]) -> Result<EsResponse, String> {
        let req = EsRequest::Append {
            stream_id: stream.to_string(),
            expected_version: ExpectedVersion::Any,
            events: vec![NewEvent {
                event_id: uuid::Uuid::new_v4(),
                event_type: "E".into(),
                data: data.to_vec(),
                metadata: vec![],
            }],
            hlc: Hlc::now(),
        };
        self.rafts[&node]
            .client_write(req)
            .await
            .map(|r| r.data)
            .map_err(|e| e.to_string())
    }

    /// 读某节点本地状态机里的流
    fn read(&self, node: u64, stream: &str) -> Vec<es_core::Event> {
        self.stores[&node]
            .read_stream_events(stream, 0, 0)
            .expect("读流")
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

/// 隔离 leader 后：多数派选出更高 term 的新 leader，少数派无法提交。
///
/// 注意被隔离的 leader **不会立即退位**。经典 Raft 中 leader 只在见到更高 term
/// 时才退位，而被隔离时它收不到任何消息，因此在自己视角里仍是 leader。
/// openraft 0.9 未实现基于租约的主动退位（实测 100ms 心跳下 50 个周期仍不退位）。
/// 真正的安全性保证是「少数派无法提交日志」，本测试断言的是这一点。
#[tokio::test]
async fn isolated_leader_majority_elects_minority_cannot_commit() {
    let c = Cluster::start().await;
    let old = c.wait_leader(Duration::from_secs(5)).await;
    let term_before = c.term(old);
    eprintln!("✓ 初始 leader = node{old}, term={term_before}");

    c.write(old, "p", b"a").await.expect("写入");
    let applied = c.last_applied(old);
    let others: Vec<u64> = c.ids.iter().copied().filter(|i| *i != old).collect();
    for &o in &others {
        c.wait_applied(o, applied, Duration::from_secs(5)).await;
    }

    eprintln!("→ 隔离 node{old}");
    c.net.isolate(old, &c.ids).await;

    // 剩余 2 节点在 3 成员集群中构成多数派，心跳超时后应选出新 leader
    let new = c.wait_leader_among(&others, Duration::from_secs(10)).await;
    assert_ne!(new, old, "新 leader 不应是被隔离的节点");
    assert!(
        c.term(new) > term_before,
        "新 leader 的 term 应更高: {} vs {}",
        c.term(new),
        term_before
    );
    eprintln!("✓ 新 leader = node{new}, term={}", c.term(new));

    // 关键安全性：被隔离的少数派联系不上多数派，写入不可能提交
    let err = tokio::time::timeout(Duration::from_secs(5), c.write(old, "p", b"minority")).await;
    match err {
        // 立即报错，或一直挂到超时，都说明没提交成功；
        // 唯一不可接受的是返回 Ok
        Ok(Err(e)) => eprintln!("✓ 少数派写入被拒: {e}"),
        Err(_) => eprintln!("✓ 少数派写入无法完成（超时未提交）"),
        Ok(Ok(r)) => panic!("少数派竟然提交成功了，违反 Raft 安全性: {r:?}"),
    }

    // 多数派可以继续提交
    c.write(new, "p", b"b").await.expect("多数派写入应成功");
    eprintln!("✓ 多数派写入成功");

    c.shutdown().await;
}

#[tokio::test]
async fn healed_partition_old_leader_catches_up() {
    let c = Cluster::start().await;
    let old = c.wait_leader(Duration::from_secs(5)).await;
    let others: Vec<u64> = c.ids.iter().copied().filter(|i| *i != old).collect();

    c.write(old, "h", b"before").await.expect("写入");
    let applied = c.last_applied(old);
    for &o in &others {
        c.wait_applied(o, applied, Duration::from_secs(5)).await;
    }

    // 隔离旧 leader，多数派选出新 leader 并继续写入。
    // 旧 leader 此时仍自认为 leader（见上一个测试的说明），但提交不了任何东西。
    c.net.isolate(old, &c.ids).await;
    let new = c.wait_leader_among(&others, Duration::from_secs(10)).await;

    for d in [b"during-1".as_ref(), b"during-2".as_ref()] {
        c.write(new, "h", d).await.expect("多数派写入");
    }
    let applied_new = c.last_applied(new);
    eprintln!("✓ 分区期间多数派写入 2 条，applied={applied_new}");

    // 此时被隔离的旧 leader 落后
    assert!(
        c.last_applied(old) < applied_new,
        "隔离期间旧 leader 应落后"
    );

    // 恢复网络，旧 leader 应作为 follower 追平
    eprintln!("→ 恢复网络");
    c.net.heal().await;

    // 恢复后旧 leader 会收到更高 term 的消息，此时才退位——这是它得知
    // 自己已被取代的唯一途径，所以要等而不能立即断言
    c.wait_step_down(old, Duration::from_secs(15)).await;
    c.wait_applied(old, applied_new, Duration::from_secs(15))
        .await;
    eprintln!("✓ node{old} 已退位并追平");

    // 三个节点数据一致
    let want: Vec<Vec<u8>> = vec![
        b"before".to_vec(),
        b"during-1".to_vec(),
        b"during-2".to_vec(),
    ];
    for &id in &c.ids {
        let got: Vec<Vec<u8>> = c.read(id, "h").into_iter().map(|e| e.data).collect();
        assert_eq!(got, want, "node{id} 数据应与多数派一致");
    }
    eprintln!("✓ 三节点数据收敛一致");

    c.shutdown().await;
}

#[tokio::test]
async fn one_way_link_cut_cluster_available() {
    let c = Cluster::start().await;
    let leader = c.wait_leader(Duration::from_secs(5)).await;
    let others: Vec<u64> = c.ids.iter().copied().filter(|i| *i != leader).collect();

    // 只切断 leader → 某个 follower 的单向链路。
    // 另一个 follower 仍可达，leader 仍握有多数派（自己 + 1），写入应继续成功。
    let cut_to = others[0];
    let ok_node = others[1];
    c.net.partition(leader, cut_to).await;
    // partition 是双向的，这里只保留一个方向以模拟单向故障
    c.net.inner.write().await.cut.remove(&(cut_to, leader));
    eprintln!("→ 切断 node{leader}→node{cut_to} 单向链路");

    c.write(leader, "u", b"x")
        .await
        .expect("多数派仍在，写入应成功");
    let applied = c.last_applied(leader);
    c.wait_applied(ok_node, applied, Duration::from_secs(5))
        .await;
    eprintln!("✓ 写入成功并复制到 node{ok_node}");

    // 可达的那个 follower 数据正确
    let got: Vec<Vec<u8>> = c.read(ok_node, "u").into_iter().map(|e| e.data).collect();
    assert_eq!(got, vec![b"x".to_vec()]);

    // 恢复后被切断的 follower 也应追平
    c.net.heal().await;
    c.wait_applied(cut_to, applied, Duration::from_secs(15))
        .await;
    let got: Vec<Vec<u8>> = c.read(cut_to, "u").into_iter().map(|e| e.data).collect();
    assert_eq!(got, vec![b"x".to_vec()], "恢复后应追平");
    eprintln!("✓ node{cut_to} 恢复后追平");

    c.shutdown().await;
}

#[tokio::test]
async fn one_slow_follower_fast_path_used() {
    // 800ms 延迟超过默认超时,用 1500ms 选举超时
    let c = Cluster::start_with_timing(Timing {
        heartbeat_ms: 200,
        election_min_ms: 1200,
        election_max_ms: 1500,
        ..Default::default()
    })
    .await;
    let leader = c.wait_leader(Duration::from_secs(5)).await;
    let others: Vec<u64> = c.ids.iter().copied().filter(|i| *i != leader).collect();
    let slow = others[0];
    let fast = others[1];

    c.write(leader, "asym", b"base").await.expect("基线");

    // leader→slow 慢(800ms),leader→fast 快(0ms)
    c.net.set_delay(leader, slow, 800).await;
    eprintln!("→ leader→node{slow} 延迟 800ms, leader→node{fast} 无延迟");

    let t0 = tokio::time::Instant::now();
    c.write(leader, "asym", b"x").await.expect("写入应走快路径");
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_millis(400),
        "提交应 < 400ms（走快 follower），实际 {:?}",
        elapsed
    );
    eprintln!("✓ 写入耗时 {:?}，多数派不含慢 follower", elapsed);

    // 快 follower 应立即有数据
    let applied = c.last_applied(leader);
    c.wait_applied(fast, applied, Duration::from_millis(500))
        .await;
    assert_eq!(c.read(fast, "asym").len(), 2);

    // 慢 follower 最终会追上
    c.net.set_delay(leader, slow, 0).await;
    c.wait_applied(slow, applied, Duration::from_secs(10)).await;
    assert_eq!(c.read(slow, "asym").len(), 2);
    eprintln!("✓ 移除延迟后慢 follower 已追平");

    c.shutdown().await;
}

#[tokio::test]
async fn lagging_node_snapshot_catchup() {
    // 快照策略:每 5 条日志建一次快照,快照后只留 2 条日志。
    // 这样被隔离的节点恢复后,它缺的日志大部分已被 purge,
    // leader 只能给它发快照——这正是快照存在的意义。
    let c = Cluster::start_with_timing(Timing {
        heartbeat_ms: 100,
        election_min_ms: 300,
        election_max_ms: 600,
        snapshot_after_logs: Some(5),
        keep_logs_after_snapshot: 2,
        snapshot_max_chunk_size: 3 * 1024 * 1024,
    })
    .await;

    let leader = c.wait_leader(Duration::from_secs(5)).await;
    let others: Vec<u64> = c.ids.iter().copied().filter(|i| *i != leader).collect();
    let laggard = others[0];
    let healthy = others[1];

    // 基线:先写一条,确认三节点都同步
    c.write(leader, "snap", b"base").await.expect("基线写入");
    let base_applied = c.last_applied(leader);
    for &o in &others {
        c.wait_applied(o, base_applied, Duration::from_secs(5))
            .await;
    }
    eprintln!("✓ 基线已同步,applied={base_applied}");

    // 隔离 laggard,让它错过后续所有写入
    c.net.isolate(laggard, &c.ids).await;
    eprintln!("→ 隔离 node{laggard}");

    // 大量写入,足以触发多次快照与日志清理。
    // leader 与 healthy 构成多数派,写入可正常提交。
    for i in 0..30 {
        c.write(leader, "snap", format!("v{i}").as_bytes())
            .await
            .unwrap_or_else(|e| panic!("第 {i} 次写入失败: {e}"));
    }
    let applied_after = c.last_applied(leader);
    c.wait_applied(healthy, applied_after, Duration::from_secs(10))
        .await;
    eprintln!("✓ 隔离期间写入 30 条,leader applied={applied_after}");

    // 确认 leader 真的建了快照并清了日志
    let snap_exists = c.stores[&leader]
        .read_stream_events("snap", 0, 0)
        .expect("读流")
        .len();
    assert_eq!(snap_exists, 31, "leader 应有 31 条事件(1 基线 + 30)");

    // 恢复网络。laggard 缺的日志已被 purge,只能靠快照追赶。
    eprintln!("→ 恢复网络,node{laggard} 应通过快照追赶");
    c.net.heal().await;

    // 给足时间完成快照传输与安装
    c.wait_applied(laggard, applied_after, Duration::from_secs(30))
        .await;
    eprintln!("✓ node{laggard} 已追平 applied={applied_after}");

    // 数据必须完整:31 条事件,顺序正确
    let events = c.read(laggard, "snap");
    assert_eq!(events.len(), 31, "追赶后事件数须一致");
    assert_eq!(events[0].data, b"base");
    assert_eq!(events[30].data, b"v29");

    // 版本连续无空洞——快照安装不能丢事件
    let versions: Vec<u64> = events.iter().map(|e| e.version).collect();
    let expected: Vec<u64> = (0..31).collect();
    assert_eq!(versions, expected, "版本须连续,快照不能丢事件");

    // 三节点数据一致
    for &id in &c.ids {
        let evs = c.read(id, "snap");
        assert_eq!(evs.len(), 31, "node{id} 事件数应一致");
        assert_eq!(evs[30].data, b"v29", "node{id} 最后一条应一致");
    }
    eprintln!("✓ 三节点数据收敛一致");

    c.shutdown().await;
}

#[tokio::test]
async fn logs_purged_after_snapshot_data_intact() {
    let c = Cluster::start_with_timing(Timing {
        heartbeat_ms: 100,
        election_min_ms: 300,
        election_max_ms: 600,
        snapshot_after_logs: Some(5),
        keep_logs_after_snapshot: 2,
        snapshot_max_chunk_size: 3 * 1024 * 1024,
    })
    .await;

    let leader = c.wait_leader(Duration::from_secs(5)).await;

    // 写足够多数据触发多轮快照
    for i in 0..20 {
        c.write(leader, "purge", format!("e{i}").as_bytes())
            .await
            .expect("写入");
    }

    let applied = c.last_applied(leader);
    for &id in &c.ids {
        c.wait_applied(id, applied, Duration::from_secs(10)).await;
    }

    // 数据必须完整,尽管日志已被清理
    for &id in &c.ids {
        let events = c.read(id, "purge");
        assert_eq!(events.len(), 20, "node{id} 应有 20 条事件");
        let versions: Vec<u64> = events.iter().map(|e| e.version).collect();
        let expected: Vec<u64> = (0..20).collect();
        assert_eq!(versions, expected, "node{id} 版本须连续");
    }
    eprintln!("✓ 快照+日志清理后,三节点数据均完整");

    // 新写入应能在快照之上继续
    c.write(leader, "purge", b"after-snapshot")
        .await
        .expect("快照后写入");
    let final_applied = c.last_applied(leader);
    for &id in &c.ids {
        c.wait_applied(id, final_applied, Duration::from_secs(10))
            .await;
    }
    let events = c.read(leader, "purge");
    assert_eq!(events.len(), 21, "快照后应能继续追加");
    assert_eq!(events[20].data, b"after-snapshot");
    eprintln!("✓ 快照后可继续写入");

    c.shutdown().await;
}

#[tokio::test]
async fn multi_chunk_snapshot_transfer() {
    // 64KiB 小块 + 不可压缩大数据：快照必然跨多个块传输，
    // 验证 Chunked 从文件流式读块、接收端 seek/write、end 判定全链路。
    let c = Cluster::start_with_timing(Timing {
        heartbeat_ms: 100,
        election_min_ms: 300,
        election_max_ms: 600,
        snapshot_after_logs: Some(3),
        keep_logs_after_snapshot: 1,
        snapshot_max_chunk_size: 64 * 1024,
    })
    .await;

    let leader = c.wait_leader(Duration::from_secs(5)).await;
    let others: Vec<u64> = c.ids.iter().copied().filter(|i| *i != leader).collect();
    let laggard = others[0];
    let healthy = others[1];

    // 基线
    c.write(leader, "big", b"base").await.expect("基线写入");
    let base_applied = c.last_applied(leader);
    for &o in &others {
        c.wait_applied(o, base_applied, Duration::from_secs(5))
            .await;
    }
    eprintln!("✓ 基线已同步,applied={base_applied}");

    // 隔离 laggard
    c.net.isolate(laggard, &c.ids).await;
    eprintln!("→ 隔离 node{laggard}");

    // 写 24 条大事件：每条约 8KB 不可压缩数据（uuid hex 拼接，压缩无法减小），
    // 总计约 192KB 快照 payload，按 64KiB 分块必然跨 3+ 块
    for i in 0..24 {
        let data: Vec<u8> = (0..512)
            .map(|_| uuid::Uuid::new_v4().to_string())
            .collect::<Vec<_>>()
            .join("")
            .into_bytes();
        c.write(leader, "big", &data)
            .await
            .unwrap_or_else(|e| panic!("第 {i} 次写入失败: {e}"));
    }
    let applied_after = c.last_applied(leader);
    c.wait_applied(healthy, applied_after, Duration::from_secs(10))
        .await;

    // 恢复网络，laggard 只能靠分块快照追赶
    eprintln!("→ 恢复网络,node{laggard} 应通过多块快照追赶");
    c.net.heal().await;
    c.wait_applied(laggard, applied_after, Duration::from_secs(30))
        .await;
    eprintln!("✓ node{laggard} 已追平 applied={applied_after}");

    // 数据完整：25 条（1 基线 + 24），且每条数据未被压缩/解压破坏
    let events = c.read(laggard, "big");
    assert_eq!(events.len(), 25, "追赶后事件数须一致");
    let versions: Vec<u64> = events.iter().map(|e| e.version).collect();
    assert_eq!(versions, (0..25).collect::<Vec<u64>>(), "版本须连续");
    assert_eq!(events[0].data, b"base");
    // 大事件数据逐字节一致（分块传输 + 压缩解压不得损坏数据）
    assert_eq!(events[24].data.len(), 512 * 36, "大事件数据长度须保持");
    for id in &c.ids {
        assert_eq!(c.read(*id, "big").len(), 25, "node{id} 事件数应一致");
    }
    eprintln!("✓ 多块快照传输后三节点数据一致");

    c.shutdown().await;
}
