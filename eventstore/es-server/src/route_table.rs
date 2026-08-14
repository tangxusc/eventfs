//! 路由表管理器：内存态 + `{data_dir}/routes.json` 落盘 + 跨节点广播同步。
//!
//! 路由表是「显式分配」架构的核心：stream → shard 归属由服务端在创建流
//! （或隐式建流）时分配并记录。本管理器保证：
//! - 单节点内更新原子（写锁内读-改-写，双检查防重复分配）
//! - 落盘原子（temp + rename，防半写）
//! - 跨节点收敛（整表广播 + 版本号仲裁，接收方只采纳更高版本）
//! - 本地文件热更新（watcher 触发 reload，运维手工改文件同样生效）

use std::collections::BTreeSet;
use std::path::PathBuf;

use es_core::route::RouteTable;
use es_proto::eventstore::migration_client::MigrationClient;
use es_proto::eventstore::{GetRouteTableRequest, PushRouteTableRequest};
use es_proto::tls::TlsClientConfig;
use tokio::sync::RwLock;

use crate::config::Config;

/// 路由表文件持久化路径：`{data_dir}/routes.json`
pub fn routes_path(data_dir: &std::path::Path) -> PathBuf {
    data_dir.join("routes.json")
}

/// 路由表管理器。
pub struct RouteTableManager {
    /// 内存态（查询快路径）
    mem: RwLock<RouteTable>,
    /// 落盘路径
    path: PathBuf,
    /// 更新串行化锁（含落盘与广播都在锁内，保证变更有序）
    update_mutex: tokio::sync::Mutex<()>,
    /// 分配范围：放置表全部分片（可随配置热更新变化）
    shard_set: RwLock<BTreeSet<u64>>,
    /// 广播目标：peer (node_id → 已 normalize 地址)
    peers: Vec<(u64, String)>,
    /// 广播用的 TLS 信任策略
    tls: Option<TlsClientConfig>,
    /// 本节点 ID（广播跳过自己）
    self_id: u64,
}

impl RouteTableManager {
    /// 创建管理器（不加载文件，加载见 [`RouteTableManager::load`]）。
    pub fn new(config: &Config, path: PathBuf) -> Result<Self, String> {
        let shard_set: BTreeSet<u64> = config
            .placement
            .nodes
            .iter()
            .flat_map(|n| n.primary.iter().chain(n.replica.iter()))
            .copied()
            .collect();
        let tls = match &config.tls {
            Some(t) => Some(t.client_trust().map_err(|e| e)?),
            None => None,
        };
        Ok(Self {
            mem: RwLock::new(RouteTable::new()),
            path,
            update_mutex: tokio::sync::Mutex::new(()),
            shard_set: RwLock::new(shard_set),
            peers: config
                .node
                .peers
                .iter()
                .map(|p| (p.id, es_raft::normalize_endpoint(&p.addr)))
                .collect(),
            tls,
            self_id: config.node.id,
        })
    }

    /// 启动加载：本地文件与 peers 中取版本最高的表。
    ///
    /// 本地文件存在时**也要**与 peers 比对版本——节点离线期间集群路由表
    /// 可能已前进（迁移切换等），直接用本地旧表会长期服务过期路由，
    /// 把新 append 写到已迁走的分片（静默数据分裂）。
    pub async fn load(&self) -> Result<(), String> {
        let local = self.load_local()?;
        let mut best = local.clone();

        // 向 peers 拉取（首个成功者即可——版本仲裁会拒绝更旧的）
        for (id, addr) in &self.peers {
            if *id == self.self_id {
                continue;
            }
            let mut client = match self.migration_client(addr) {
                Ok(c) => c,
                Err(_) => continue,
            };
            match client.get_route_table(GetRouteTableRequest {}).await {
                Ok(resp) => {
                    let t = proto_to_table(resp.into_inner().table);
                    let t_version = t.version;
                    let is_newer = best.as_ref().map_or(true, |b| t_version > b.version);
                    if is_newer {
                        best = Some(t);
                    }
                    tracing::info!(
                        "路由表从节点 {id} 拉取：version={}（本地 {}）",
                        t_version,
                        local.as_ref().map(|b| b.version).unwrap_or(0)
                    );
                    break; // 任意一个 peer 即可，版本仲裁保证收敛
                }
                Err(_) => continue,
            }
        }

        match best {
            Some(t) => {
                tracing::info!("路由表加载完成：version={}", t.version);
                *self.mem.write().await = t;
            }
            None => {
                tracing::info!("无本地路由表且 peers 不可达，以空表启动（version=0）");
            }
        }
        Ok(())
    }
    /// 读本地文件；文件缺失返回 Ok(None)，损坏返回 Err（调用方保留内存旧表）。
    fn load_local(&self) -> Result<Option<RouteTable>, String> {
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let t: RouteTable =
                    serde_json::from_slice(&bytes).map_err(|e| format!("路由表损坏: {e}"))?;
                Ok(Some(t))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("路由表读取失败: {e}")),
        }
    }

    /// 原子落盘：temp + rename（同目录，rename 原子）+ fsync。
    ///
    /// routes.json 是流归属的唯一权威，掉电后回退会让流被重新分配、
    /// 旧事件成孤儿——文件与目录都必须 fsync（write 后 rename 前各一次）。
    fn persist(&self, table: &RouteTable) -> Result<(), String> {
        if let Some(dir) = self.path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| format!("建路由表目录失败: {e}"))?;
        }
        let tmp = self.path.with_extension("json.tmp");
        let json = serde_json::to_vec_pretty(table).map_err(|e| format!("序列化失败: {e}"))?;
        {
            use std::io::Write;
            let mut f =
                std::fs::File::create(&tmp).map_err(|e| format!("创建临时文件失败: {e}"))?;
            f.write_all(&json)
                .map_err(|e| format!("写临时文件失败: {e}"))?;
            f.sync_all()
                .map_err(|e| format!("临时文件 fsync 失败: {e}"))?;
        }
        std::fs::rename(&tmp, &self.path).map_err(|e| format!("rename 失败: {e}"))?;
        // 目录 fsync：保证 rename 的目录项也落盘
        if let Some(dir) = self.path.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }

    /// 确保路由表文件存在（watcher 需要 watch 已存在的文件；缺失时落盘空表）。
    /// 幂等：文件已存在不覆盖。
    pub async fn ensure_file(&self) -> Result<(), String> {
        if self.path.exists() {
            return Ok(());
        }
        let table = self.mem.read().await.clone();
        self.persist(&table)
    }

    /// 广播整表到全部 peers（尽力而为：失败仅告警，下次变更全表重发自愈）。
    async fn broadcast(&self, table: &RouteTable) {
        let table = table.clone();
        for (id, addr) in &self.peers {
            if *id == self.self_id {
                continue;
            }
            let mut client = match self.migration_client(addr) {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!(peer = id, "广播客户端构建失败：{e}");
                    continue;
                }
            };
            let req = PushRouteTableRequest {
                table: Some(table_to_proto(&table)),
            };
            if let Err(e) = client.push_route_table(req).await {
                tracing::warn!(peer = id, "路由表广播失败（下次变更重发）：{e}");
            }
        }
    }

    /// 查询 stream 归属（快路径，不落盘）。
    pub async fn lookup(&self, stream: &str) -> Option<u64> {
        self.mem.read().await.lookup(stream)
    }

    /// 分配 stream 到「大致最少流」的 shard 并记录（版本 +1 + 落盘 + 广播）。
    ///
    /// 双检查：锁内先查，已存在则直接返回现有归属（不 bump 版本）。
    /// 返回 `(shard_id, 是否新建)`。
    pub async fn allocate(&self, stream: &str) -> Result<(u64, bool), String> {
        let _guard = self.update_mutex.lock().await;
        {
            let mem = self.mem.read().await;
            if let Some(s) = mem.lookup(stream) {
                return Ok((s, false));
            }
        }
        let shard_set = self.shard_set.read().await.clone();
        let mut mem = self.mem.write().await;
        let shard = mem
            .allocate(stream, &shard_set)
            .ok_or_else(|| "放置表为空，无法分配 shard".to_string())?;
        let inserted = mem.insert(stream, shard);
        let table = mem.clone();
        drop(mem);
        self.persist(&table)?;
        // 广播在锁外：不阻塞其它路由表操作（peer 挂起有超时兜底，
        // 乱序由版本仲裁收敛——接收方只采纳更高版本）
        self.broadcast(&table).await;
        Ok((shard, inserted))
    }

    /// 原子切换 stream 归属（迁移切换点）：版本 +1 + 落盘 + 广播，返回新表。
    pub async fn set_stream_shard(&self, stream: &str, shard: u64) -> Result<RouteTable, String> {
        let _guard = self.update_mutex.lock().await;
        let mut mem = self.mem.write().await;
        match mem.lookup(stream) {
            Some(cur) if cur == shard => {}
            _ => {
                if let Some(old) = mem.remove(stream) {
                    let _ = old;
                }
                mem.insert(stream, shard);
            }
        }
        let table = mem.clone();
        drop(mem);
        self.persist(&table)?;
        self.broadcast(&table).await; // 锁外广播（版本仲裁收敛乱序）
        Ok(table)
    }

    /// 应用远端广播的表：版本高于本地才采纳（落盘 + 替换内存态）。
    pub async fn apply_remote(&self, table: RouteTable) -> Result<(), String> {
        let current = self.mem.read().await.clone();
        if table.version <= current.version {
            return Ok(()); // 幂等：旧版/重复广播忽略
        }
        let _guard = self.update_mutex.lock().await;
        // 锁内复查（等待期间可能已被其它路径更新）
        let current = self.mem.read().await.clone();
        if table.version <= current.version {
            return Ok(());
        }
        self.persist(&table)?;
        *self.mem.write().await = table;
        Ok(())
    }

    /// 本地文件变更后重载（watcher 触发；损坏时保留内存旧表并告警）。
    pub async fn reload(&self) {
        match self.load_local() {
            Ok(Some(t)) => {
                let current = self.mem.read().await.clone();
                if t.version > current.version {
                    tracing::info!("路由表热更新：version {} → {}", current.version, t.version);
                    *self.mem.write().await = t;
                }
            }
            Ok(None) => {}
            Err(e) => tracing::error!("路由表重载失败，保留内存旧表：{e}"),
        }
    }

    /// 校准 per-shard 流计数（recount），版本不变。返回校准后的表。
    pub async fn recount(&self) -> Result<RouteTable, String> {
        let _guard = self.update_mutex.lock().await;
        let mut mem = self.mem.write().await;
        mem.recount(); // 版本 +1（使广播可被 peers 采纳）
        let table = mem.clone();
        drop(mem);
        self.persist(&table)?;
        self.broadcast(&table).await; // 锁外广播
        Ok(table)
    }

    /// 当前表快照。
    pub async fn snapshot(&self) -> RouteTable {
        self.mem.read().await.clone()
    }

    /// 更新分配范围（配置热更新后由 watcher 调用）。
    pub async fn set_shard_set(&self, shard_set: BTreeSet<u64>) {
        *self.shard_set.write().await = shard_set;
    }

    /// 构建到 peer 的 Migration 客户端（惰性连接）。
    ///
    /// 请求超时 2s：广播/拉取不能因 peer 挂起（接受连接但不响应）而无限
    /// 等待——广播失败由「下次变更全表重发」自愈。
    fn migration_client(
        &self,
        addr: &str,
    ) -> Result<MigrationClient<tonic::transport::Channel>, String> {
        const BROADCAST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
        let endpoint = tonic::transport::Endpoint::from_shared(addr.to_string())
            .map_err(|e| format!("地址非法 {addr}: {e}"))?;
        let endpoint = endpoint.timeout(BROADCAST_TIMEOUT);
        let endpoint = es_proto::tls::apply_endpoint_tls(endpoint, self.tls.as_ref())
            .map_err(|e| format!("TLS 装配失败（{addr}）: {e}"))?;
        Ok(MigrationClient::new(endpoint.connect_lazy()))
    }
}

/// proto RouteTable → 领域模型（table 缺失视为空表）
pub fn proto_to_table(t: Option<es_proto::eventstore::RouteTable>) -> RouteTable {
    match t {
        Some(t) => RouteTable {
            version: t.version,
            streams: t.streams.into_iter().collect(),
            shard_stream_counts: t.shard_stream_counts.into_iter().collect(),
        },
        None => RouteTable::new(),
    }
}

/// 领域模型 → proto RouteTable
pub fn table_to_proto(t: &RouteTable) -> es_proto::eventstore::RouteTable {
    es_proto::eventstore::RouteTable {
        version: t.version,
        streams: t.streams.clone().into_iter().collect(),
        shard_stream_counts: t.shard_stream_counts.clone().into_iter().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn test_config(data_dir: &std::path::Path) -> Config {
        Config {
            node: crate::config::NodeConfig {
                id: 1,
                listen_addr: "127.0.0.1:0".into(),
                internal_listen_addr: None,
                peers: Vec::new(),
            },
            storage: crate::config::StorageConfig {
                data_dir: data_dir.to_path_buf(),
                memtable_arena_bytes: 4 * 1024 * 1024,
            },
            placement: crate::config::PlacementConfig {
                replication_factor: 1,
                nodes: vec![crate::config::PlacementNode {
                    id: 1,
                    primary: vec![0, 1, 2],
                    replica: vec![],
                }],
            },
            snapshot: Default::default(),
            tls: None,
            limits: Default::default(),
        }
    }

    #[tokio::test]
    async fn allocate_persists_and_roundtrips() {
        let dir = tempfile::tempdir().expect("临时目录");
        let mgr = RouteTableManager::new(&test_config(dir.path()), routes_path(dir.path()))
            .expect("创建");
        let (shard, inserted) = mgr.allocate("s1").await.expect("分配");
        assert!(inserted);
        assert_eq!(shard, 0, "空表分配应选最小 shard");

        // 文件已落盘，新管理器可加载
        let mgr2 = RouteTableManager::new(&test_config(dir.path()), routes_path(dir.path()))
            .expect("创建");
        mgr2.load().await.expect("加载");
        assert_eq!(mgr2.lookup("s1").await, Some(0));

        // 版本递增；最少流分配：s1 → 0，s2 → 1（shard 0 已有 1 个流）
        let (_, inserted2) = mgr.allocate("s2").await.expect("分配");
        assert!(inserted2);
        let t = mgr.snapshot().await;
        assert_eq!(t.version, 2);
        assert_eq!(
            t.shard_stream_counts,
            BTreeMap::from([(0u64, 1u64), (1u64, 1u64)])
        );
    }

    #[tokio::test]
    async fn allocate_existing_returns_without_bump() {
        let dir = tempfile::tempdir().expect("临时目录");
        let mgr = RouteTableManager::new(&test_config(dir.path()), routes_path(dir.path()))
            .expect("创建");
        mgr.allocate("s1").await.expect("分配");
        let v1 = mgr.snapshot().await.version;
        let (shard, inserted) = mgr.allocate("s1").await.expect("再分配");
        assert!(!inserted, "已存在的流不应新建");
        assert_eq!(shard, 0);
        assert_eq!(mgr.snapshot().await.version, v1, "重复分配不 bump 版本");
    }

    #[tokio::test]
    async fn allocate_least_loaded() {
        let dir = tempfile::tempdir().expect("临时目录");
        let mgr = RouteTableManager::new(&test_config(dir.path()), routes_path(dir.path()))
            .expect("创建");
        mgr.allocate("a").await.expect("a"); // shard 0: 1
        mgr.allocate("b").await.expect("b"); // shard 0: 2
        mgr.allocate("c").await.expect("c"); // shard 0: 3
        mgr.allocate("d").await.expect("d"); // shard 0: 4
        mgr.allocate("e").await.expect("e"); // shard 0: 5
        mgr.allocate("f").await.expect("f"); // shard 0: 6
        mgr.allocate("g").await.expect("g"); // shard 0: 7
        // shard 0 有 7 个，shard 1/2 有 0 个 → 下一个去 shard 1
        let (shard, _) = mgr.allocate("h").await.expect("h");
        assert_eq!(shard, 1);
    }

    #[tokio::test]
    async fn set_stream_shard_switches_atomically() {
        let dir = tempfile::tempdir().expect("临时目录");
        let mgr = RouteTableManager::new(&test_config(dir.path()), routes_path(dir.path()))
            .expect("创建");
        mgr.allocate("s").await.expect("分配"); // shard 0
        let t = mgr.set_stream_shard("s", 2).await.expect("切换");
        assert_eq!(t.lookup("s"), Some(2));
        // 计数迁移：shard 0 -1，shard 2 +1
        assert_eq!(t.shard_stream_counts.get(&0), Some(&0));
        assert_eq!(t.shard_stream_counts.get(&2), Some(&1));
        // 同值切换不重复 bump
        let v = t.version;
        let t2 = mgr.set_stream_shard("s", 2).await.expect("同值切换");
        assert_eq!(t2.version, v);
    }

    #[tokio::test]
    async fn set_stream_shard_repairs_unassigned_stream_and_persists() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&test_config(dir.path()), path.clone()).expect("创建");

        // 迁移可修复路由表缺失但数据已存在的孤儿流，切换结果必须可恢复。
        let table = mgr
            .set_stream_shard("orphan-stream", 2)
            .await
            .expect("补齐孤儿流路由");
        assert_eq!(table.lookup("orphan-stream"), Some(2));
        assert_eq!(table.shard_stream_counts.get(&2), Some(&1));

        let recovered = RouteTableManager::new(&test_config(dir.path()), path).expect("重建管理器");
        recovered.load().await.expect("恢复路由表");
        assert_eq!(recovered.lookup("orphan-stream").await, Some(2));
    }

    #[tokio::test]
    async fn apply_remote_version_arbitration() {
        let dir = tempfile::tempdir().expect("临时目录");
        let mgr = RouteTableManager::new(&test_config(dir.path()), routes_path(dir.path()))
            .expect("创建");
        let mut remote = RouteTable::new();
        remote.insert("x", 1);
        remote.version = 5;
        mgr.apply_remote(remote).await.expect("应用远端表");
        assert_eq!(mgr.lookup("x").await, Some(1));

        // 旧版本被忽略
        let mut stale = RouteTable::new();
        stale.insert("y", 0);
        stale.version = 3;
        mgr.apply_remote(stale).await.expect("旧版本应忽略");
        assert_eq!(mgr.lookup("y").await, None, "旧版本不应覆盖");
    }

    #[tokio::test]
    async fn reload_picks_up_external_file_change() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&test_config(dir.path()), path.clone()).expect("创建");
        // 模拟运维手工修改文件（版本更高）
        let mut t = RouteTable::new();
        t.insert("manual", 1);
        std::fs::write(&path, serde_json::to_vec(&t).expect("序列化")).expect("写文件");
        mgr.reload().await;
        assert_eq!(mgr.lookup("manual").await, Some(1));
        // 同版本不采纳（避免热更新回环覆盖）
        let mut stale = RouteTable::new();
        stale.insert("other", 2);
        std::fs::write(&path, serde_json::to_vec(&stale).expect("序列化")).expect("写文件");
        mgr.reload().await;
        assert_eq!(mgr.lookup("other").await, None, "低版本文件不采纳");
    }

    #[tokio::test]
    async fn reload_missing_file_keeps_memory_state() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&test_config(dir.path()), path.clone()).expect("创建");
        mgr.allocate("stable-stream").await.expect("分配");

        // 原子替换 routes.json 时 watcher 可能先看到删除事件，不能清空内存路由。
        std::fs::remove_file(&path).expect("模拟替换中的旧文件删除");
        mgr.reload().await;
        assert_eq!(mgr.lookup("stable-stream").await, Some(0));
    }

    #[tokio::test]
    async fn reload_io_failure_keeps_memory_state() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&test_config(dir.path()), path.clone()).expect("创建");
        mgr.allocate("stable-stream").await.expect("分配");

        // 运维误把路由表文件替换为目录时，读失败不能清空已生效的内存路由。
        std::fs::remove_file(&path).expect("移除路由表文件");
        std::fs::create_dir(&path).expect("模拟错误的目录替换");
        mgr.reload().await;
        assert_eq!(mgr.lookup("stable-stream").await, Some(0));
    }

    #[tokio::test]
    async fn corrupted_file_keeps_memory_state() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&test_config(dir.path()), path.clone()).expect("创建");
        mgr.allocate("s").await.expect("分配");
        std::fs::write(&path, b"not json").expect("写坏文件");
        mgr.reload().await; // 不应 panic，保留内存态
        assert_eq!(mgr.lookup("s").await, Some(0));
    }

    #[tokio::test]
    async fn recount_rebuilds_counts() {
        let dir = tempfile::tempdir().expect("临时目录");
        let mgr = RouteTableManager::new(&test_config(dir.path()), routes_path(dir.path()))
            .expect("创建");
        mgr.allocate("a").await.expect("a");
        mgr.allocate("b").await.expect("b");
        mgr.allocate("c").await.expect("c");
        let t = mgr.recount().await.expect("校准");
        assert_eq!(
            t.shard_stream_counts,
            BTreeMap::from([(0u64, 1u64), (1u64, 1u64), (2u64, 1u64)])
        );
    }

    #[tokio::test]
    async fn ensure_file_creates_empty_table_idempotently() {
        let dir = tempfile::tempdir().expect("临时目录");
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&test_config(dir.path()), path.clone()).expect("创建");
        assert!(!path.exists(), "前置：文件不存在");
        mgr.ensure_file().await.expect("首次创建");
        assert!(path.exists(), "应落盘空表");
        // 幂等：已存在不覆盖（后续 allocate 的数据不被清掉）
        mgr.allocate("s").await.expect("分配");
        let before = std::fs::read(&path).expect("读文件");
        mgr.ensure_file().await.expect("已存在应成功");
        let after = std::fs::read(&path).expect("读文件");
        assert_eq!(before, after, "已存在时不应覆盖");
        // 新管理器可从文件加载
        let mgr2 = RouteTableManager::new(&test_config(dir.path()), path.clone()).expect("创建");
        mgr2.load().await.expect("加载");
        assert_eq!(mgr2.lookup("s").await, Some(0));
    }

    #[tokio::test]
    async fn allocation_survives_invalid_peer_endpoint() {
        let dir = tempfile::tempdir().expect("临时目录");
        let mut config = test_config(dir.path());
        config.node.peers.push(crate::config::PeerConfig {
            id: 2,
            // 运行期配置热更新的过渡态可能包含非法地址；广播失败不能回滚已落盘路由。
            addr: "http://[::1".into(),
            internal_addr: None,
        });
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&config, path.clone()).expect("创建");

        let (shard, inserted) = mgr.allocate("local-durable").await.expect("本地分配");
        assert!(inserted);
        assert_eq!(shard, 0);

        let recovered = RouteTableManager::new(&test_config(dir.path()), path).expect("重建管理器");
        recovered.load().await.expect("恢复路由表");
        assert_eq!(recovered.lookup("local-durable").await, Some(0));
    }

    #[tokio::test]
    async fn allocation_survives_unreachable_peer() {
        let dir = tempfile::tempdir().expect("临时目录");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("预留端口");
        let addr = listener.local_addr().expect("读取预留端口");
        drop(listener);

        let mut config = test_config(dir.path());
        config.node.peers.push(crate::config::PeerConfig {
            id: 2,
            // 客户端可构建但 peer 已下线时，整表广播应降级而非阻塞写入。
            addr: format!("http://{addr}"),
            internal_addr: None,
        });
        let path = routes_path(dir.path());
        let mgr = RouteTableManager::new(&config, path.clone()).expect("创建");

        let (shard, inserted) = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            mgr.allocate("local-with-offline-peer"),
        )
        .await
        .expect("不可达 peer 不应阻塞本地分配")
        .expect("本地分配");
        assert!(inserted);
        assert_eq!(shard, 0);

        let recovered = RouteTableManager::new(&test_config(dir.path()), path).expect("重建管理器");
        recovered.load().await.expect("恢复路由表");
        assert_eq!(recovered.lookup("local-with-offline-peer").await, Some(0));
    }
}
