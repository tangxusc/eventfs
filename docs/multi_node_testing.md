# 多节点集成测试

状态：测试场景已实现；2026-08-16 已独立串行复验全部真实多进程用例。

## 测试结果

分两套：**多进程测试**跑真实 gRPC 路径，**进程内测试**用可控网络层跑分区场景。

### 多进程测试（es-server 14 项：9 手动组建 + 5 自动组建）

2026-08-16 使用 Rust 1.88、`Cargo.lock` 独立串行复验，14/14 通过。历史上
`leader_killed_re_elect_data_intact` 只比较不同日志基线上的 `last_applied`，可能在事件
状态机尚不可读时误判复制完成；现改为轮询真实事件内容，leader 故障恢复用例已通过。

| 测试 | 验证内容 |
|---|---|
| `三节点能正常启动并接受连接` | 3 个独立进程启动、端口就绪、gRPC 可响应 |
| `三节点选主并复制日志` | 选出唯一 leader；三节点 term / current_leader / voters 一致；写入 leader 后数据真实复制到两个 follower 的状态机 |
| `多分片各自选主且互不影响` | 3 节点 × 3 分片：每分片独立选主、各自 3 投票成员；30 个流分布到多分片且数据互不串；跨分片 ReadAll 汇总全部且各分片内 position 序不乱 |
| `杀掉leader后重新选主且数据不丢` | kill leader 进程后剩余 2 节点仍构成多数派并选出新 leader；崩溃前已提交数据完好；新 leader 可继续写入且版本号接续 |
| `节点重启后能重新加入并追平落后的数据` | 复用原数据目录重启 follower，恢复重启前数据；追平停机期间多数派写入的 3 条；版本连续无空洞 |
| `并发首次创建同一流在跨节点线性化` | 两个节点并发首次写同一未知流；控制 Shard 只产生一个权威归属，成功写入不会分叉到不同数据 Shard |
| `控制Shard失去quorum时未知流拒写并可恢复` | 控制 Shard 无多数派时未知流不被临时 hash 分配；quorum 恢复后首次归属与写入成功 |
| `非leader节点拒绝写入并可从其读取` | 非 leader 写入返回 `Unavailable` 且带 leader 地址；客户端重定向后写入成功；follower 可读已复制数据 |
| `SDK只给follower地址经重定向写入成功` | es-client 只给 follower 地址（`leader_addr` 不在初始列表），append 经完整重定向路径写入成功；等复制后从 follower 读回 |
| `三节点配置peers自动组建并复制数据` | 配置完整 peers 启动、不调用任何组建 API，自动收敛为唯一 leader 与 3 投票成员；写入复制到全部节点（同时实测裁决"多节点同时竞选"收敛性） |
| `自动组建后重启节点不重复初始化` | 自动组建后重启 follower：从本地日志恢复、不重复 initialize、追平停机期间写入且 membership 保留 |
| `全集群重启后自动恢复` | 全部节点重启后不调用任何 API，从本地日志自动恢复 leader 与 3 投票成员，重启前数据完好 |
| `节点乱序启动自动组建` | node2 先起（无 quorum 自举、条目未提交），node1/node3 后起探测到已初始化即跳过并投票，收敛提交 |
| `单节点peers只含自己自动自举` | 单节点集群立即自举为 leader，写读闭环 |

### 进程内分区测试（3 项，7.2s，在默认套件内）

位于 `es-raft/tests/partition_test.rs`。

```
test 隔离leader后多数派选出新leader且少数派无法提交 ... ok
test 分区恢复后旧leader追平多数派数据 ... ok
test 单向链路中断不影响集群可用性 ... ok
```

**为何不用多进程**：分区要按「有向链路」精确控制通断（A→B 断而 B→A 通）。
多进程下 TCP 代理只能按节点粒度屏蔽，且 gRPC 客户端源端口随机，无法按来源过滤。
进程内自建 `RaftNetwork` 可直接维护一张链路矩阵，确定且无需真实网络。

| 测试 | 验证内容 |
|---|---|
| `隔离leader后多数派选出新leader且少数派无法提交` | 多数派选出更高 term 的新 leader；被隔离的少数派无法提交写入；多数派可继续写入 |
| `分区恢复后旧leader追平多数派数据` | 分区期间多数派继续提交；恢复后旧 leader 退位、追平，三节点数据收敛一致 |
| `单向链路中断不影响集群可用性` | 切断 leader→某 follower 单向链路后，leader 仍握多数派，写入成功；恢复后该 follower 追平 |

## 集群组建流程

两条路径，每个分片各组建一次：

**自动组建（推荐）**：`node.peers` 非空即触发（etcd 静态引导语义）。日志为空的
节点探测 peer 是否已有集群，无则用完整 peers `initialize` 一步到位；已有日志
的节点（重启）跳过。多进程测试实测：3 节点同时启动竞选收敛为唯一 leader
（`三节点配置peers自动组建并复制数据`）、乱序启动收敛（`节点乱序启动自动组建`）、
全集群重启自动恢复（`全集群重启后自动恢复`）。

**手动组建（peers 为空时）**：

```
1. node1.initialize([node1])        单成员初始化，立刻成为 leader
2. 等待 node1 确认为 leader
3. node1.add_learner(node2, 阻塞)   学习者只收日志、不投票
4. node1.add_learner(node3, 阻塞)
5. node1.change_membership([1,2,3]) 一次性提升为 3 投票成员
```

**为何单成员自举**：早期担忧"三个空节点同时具备投票权会同时竞选导致选举活锁"，
故采用单成员自举保证全程有 leader。自动组建路径实测表明完整成员 `initialize`
同样收敛（600~900ms 随机化选举超时），该担忧被证伪——手动路径保留单成员自举
仅为排障便利（全程有 leader）。

## 实现要点

落地多节点时修掉了 4 个问题，都属于「单节点跑得通、多节点必然错」的类型。

### 1. Raft RPC 缺 shard_id

每个节点为每个分片各持一个独立的 `Raft` 实例。原先的 `RaftAppendEntriesRequest`
等消息没有 `shard_id` 字段，服务端只能硬编码路由到分片 0 —— 多分片下所有
Raft 消息都会投给错误的实例。现已在三个请求消息中补上该字段。

### 2. 网络工厂必须按分片划分

`RaftNetworkFactory::new_client(target, node)` 只传目标节点，不传分片。
因此分片信息必须由工厂自身携带：`GrpcNetwork::new(shard_id)`，
每个分片在 `Raft::new` 时各建一个实例。

### 3. 地址来源统一为 BasicNode.addr

节点地址随 `initialize` / `add_learner` 写入 membership 日志并复制到各节点，
网络层直接读 `BasicNode.addr` 即可，无需另建 `node_id → addr` 映射表。
少一份状态就少一处不一致的可能。

### 4. gRPC 通道必须惰性建立

集群启动时各节点上线有先后，选举期间必然出现对端未就绪。
`Endpoint::connect()` 在对端未就绪时直接失败，改用 `connect_lazy()`：
按需重连，且 `Channel` 自带连接复用。

### 5. 被隔离的 leader 不会主动退位（实测结论）

写分区测试时最初断言「leader 失去多数派联系后应退位」，实测失败：
100ms 心跳下等 5 秒（50 个心跳周期）仍是 Leader 状态。

这不是缺陷。经典 Raft 中 leader 只在**见到更高 term** 时退位，而被隔离时
它收不到任何消息，因此在自己视角里仍是 leader。openraft 0.9 未实现
基于租约（lease）的主动退位。

真正的安全性保证是「**少数派无法提交日志**」，测试应断言这一点而非退位。
旧 leader 会在网络恢复、收到更高 term 消息时才退位。

这对客户端有实际影响：向被隔离的旧 leader 写入不会立即报错，而是**挂起直到超时**
（它一直在尝试联系多数派）。客户端必须设置写超时，不能依赖服务端快速失败。

### 6. ForwardToLeader 要透出 leader 地址

openraft 在非 leader 节点返回的 `ForwardToLeader` 里带着 leader 的
node_id 与地址。原实现把它整体转成 `internal` 错误，地址信息被丢弃，
客户端只能盲目轮询。现映射为 `Unavailable`（gRPC 的可重试语义），
message 中携带 `leader_addr=...`，客户端据此重定向。

## 运行方式

多节点测试标记为 `#[ignore]`，需显式启用：

```bash
# 先编译二进制——测试会以子进程方式启动它
cargo build --bin eventstored

# 串行运行全部多节点测试（并行会争抢端口与 CPU）
cargo test -p es-server --test multi_node_test -- --ignored --nocapture --test-threads=1

# 单个测试
cargo test -p es-server --test multi_node_test 三节点选主并复制日志 -- --ignored --nocapture
```

**为何标 ignore**：每个用例启动 3 个真实进程、约 4 秒，而默认测试套件
70 项只需 1.1 秒。把它们排除在默认套件外可保持快速反馈，
CI 中应作为独立阶段执行。

## 测试框架说明

`TestCluster` 提供：

- `start()` — 分配端口、生成 JSON 配置、启动 3 个 `eventstored` 子进程，
  并轮询 `TcpStream::connect` 确认端口真正就绪（而非固定 sleep）
- `form_cluster()` — 按上述流程组建集群
- `wait_for_leader(timeout)` — 轮询各节点直到出现 leader，返回其 id
- `wait_applied(node, want, timeout)` — 等某节点 `last_applied` 追平
  （复制是异步的，不能写完立即断言）
- `shutdown()` / `Drop` — kill 全部子进程，`TempDir` 自动删数据目录

子进程输出重定向到 `/dev/null` 并设 `RUST_LOG=warn`，避免污染测试输出。

## 已完成场景

| 场景 | 实现 | 测试 | 文档 |
|---|---|---|---|
| **跨分片 ReadAll** | k 路归并 + 逐分片游标 | 4 项(默认套件) | 设计要点在本文 |
| **多分片多副本** | 逐分片组建 Raft group | 1 项(多进程) | 同上 |
| **节点重启重新加入** | 复用数据目录重启 | 1 项(多进程) | 同上 |
| **网络分区** | 进程内可控网络层 | 3 项(默认套件) | 同上 |
| **慢节点/非对称延迟** | 进程内按链路设延迟 | 1 项(默认套件) | 同上 |
| **反向读取** | `read_*_backward` 倒扫 | 2 项 e2e | 实现细节在 `key.rs` / `state_machine.rs` 注释 |
| **在线迁移** | esctl migrate（状态机 + Migration 原语） | e2e（`es-ctl/tests/e2e_test.rs`） | 见 `migrate.md` 完整设计 |

**2026-08-16 验收计数：macOS ARM64 默认 workspace 634 项通过、16 项忽略；真实
多进程用例 16/16 通过（es-server 14 项、esctl 2 项）。Linux 条件编译测试项数以
同一提交的 Release Action 输出为准。**

## 跨分片 ReadAll 设计要点

**用 k 路归并而非「合并后整体排序」**。整体排序会让同一分片内的事件顺序
完全由 HLC 决定，而 HLC 由各 leader 的墙上时钟推进，时钟回拨时同分片内的
顺序就会被打乱——这违反了分片内严格按提交序（position）的保证。
k 路归并只在各路队首之间比较，每路内部顺序原样保留。

**翻页必须用逐分片游标** `from_positions`。跨分片归并后各分片被消费到的位置不同，
用单一 `from_position` 续读会重复或漏掉事件。服务端按本页归并消费水位返回下一页
逐 shard 游标，客户端应原样回传；兼容旧客户端时也可按响应事件自行构造。

**接入节点负责代理未承载的 shard**。它对本地 shard 直接读取，对远端 shard 根据
Raft membership 找到 leader，并发送只包含一个 `from_positions` 项的单 shard
`ReadAll`，从而避免递归 fan-out。之后仍在接入节点执行 HLC k 路归并、全局 limit 和
消费水位计算，因此客户端只连接任一健康节点即可读取全部 shard。

## 已知限制

- **写入必须打到 leader**：服务端不做透明转发，由客户端按错误信息重定向。
  透明转发需要在服务端持有到其它节点的业务连接，会让故障域变复杂。
- **向被隔离的旧 leader 写入会挂起而非快速失败**：见上文第 5 点，
  客户端必须设写超时。
- **自动组建要求全节点 `node.peers` 配置完全一致**：配置不一致可能形成双集群
  （与 etcd 相同约束，运行时无法自动修复）；探测到已初始化 peer 的 voter_ids
  与配置不符时节点会告警并放弃自举。
- **故障注入仅覆盖 kill 进程与网络分区**：磁盘故障未覆盖（进程内网络层
  已支持按链路延迟，可扩展）。完整已知限制清单见根目录 README。

## 后续工作

- [ ] 磁盘故障注入（kill -9 时进程崩溃中断写）
- [x] 客户端 SDK 内置 leader 重定向重试（`es-client` append 与 `es-ctl`
  with_leader 共用 `es-core::redirect` 策略；`sdk_append_redirects_to_leader`
  覆盖只给 follower 地址的完整重定向路径）
