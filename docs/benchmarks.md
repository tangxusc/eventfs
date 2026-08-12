# 性能基准测试

## 端到端压测（3 节点 TLS 集群）

补齐下文的「未覆盖场景」：通过 gRPC 真实路径压测 Append / ReadStream / Subscribe，
覆盖完整 Raft 复制、自动组建与 TLS 加密链路。

### 运行方式

```bash
./eventstore/scripts/perf_test.sh
```

流程：release 构建 → openssl 生成自签证书（每节点独立，CN=127.0.0.1 + SAN，
CA:FALSE，`ca_file` 按节点数拼接全部证书实现互信）→ 生成 N 节点 TLS 配置
（`peers` 显式 `https://`，按 NODE_N 动态生成，自动组建）→ 启动集群
（默认 3 节点 127.0.0.1:50051-50053，8 分片，每分片独立 Raft group）→
运行 `es-server/examples/perf_test.rs` 压测客户端（预热 append 自带重试，
兜底选举收敛，不依赖固定等待）→ 结果 JSON 落盘 `target/perf-results/`；
脚本 `trap EXIT` 统一清理节点进程与数据目录（含失败路径），结果文件在
清理范围之外，中途失败也保留已完成规格。

### 测试参数

| 参数 | 值 |
|---|---|
| 集群 | 3 节点 × 8 分片，每分片独立 Raft group，自动组建 |
| 传输 | TLS（自签证书 + 客户端 CA 严格校验；节点间 RPC 与客户端 API 均加密） |
| 事件规格 | 1KB / 10KB / 100KB payload（不可压缩伪随机字节），每规格固定总量 50MB |
| 客户端 | es-client 单客户端顺序压测（基线值，非并发上限） |
| 测量路径 | 单条 append 延迟抽测 200 次（独立流，不污染批量流）；批量 append 至总量；read_stream 全量分页读（断言读到全部事件，命中未追平 follower 自动重试）；subscribe 从 0 追平（同断言） |

### 结果（2026-08-12 复测，压测工具修复后；原始 JSON 见 `target/perf-results/result-20260812-102146.json`，另一次运行见 `result-20260812-101642.json`）

| 事件大小 | 单条 append p50/p95/p99 (ms) | 批量写入 条/s | MB/s | 全量读 条/s | MB/s | 订阅追平 条/s | MB/s |
|---|---|---|---|---|---|---|---|
| 1KB | 0.41 / 0.79 / 0.89 | 38,009 | 37.1 | 230,470 | 225.1 | 236,226 | 230.7 |
| 10KB | 0.86 / 1.31 / 1.58 | 5,016 | 49.0 | 27,344 | 267.0 | 26,746 | 261.2 |
| 100KB | 1.95 / 2.55 / 3.72 | 424 | 41.4 | 2,617 | 255.6 | 2,926 | 285.8 |

两次运行一致（写入偏差 <5%，读/订阅偏差 ≤11%，受机器负载波动影响），
主要结论：

- **写入吞吐 37–49 MB/s，吞吐与事件大小无单调关系**（10KB 规格最高）：
  写入经 3 节点 Raft 提交（leader fsync + 复制到双 follower），大日志条目
  复制/落盘成本更高，但小事件受每批固定开销拖累。
- **读取 / 订阅吞吐 225–286 MB/s，不随事件大小变化**：读路径瓶颈为每页
  固定开销（gRPC 往返 + LSM 读），而非事件大小；订阅 catch-up 与全量读
  同源，数字一致。
- **单条 append 延迟与事件大小近线性**（p50 0.41 → 0.86 → 1.95 ms）；
  100KB 规格 p99 ≈ 3.7 ms，大日志条目的复制/fsync 尾延迟明显，容量规划需
  考虑。

### 优化：存储编码 serde_json → bincode（2026-08-12）

首轮结果暴露写入瓶颈后定位：**瓶颈是每事件的字节处理成本而非磁盘/复制**。
证据：批延迟随批大小线性增长（≈90µs/事件）、单节点同样饱和（~19 MB/s）、
surrealkv 默认 `Durability::Eventual`（commit 不 fsync）。根因是事件在存储
与 raft 日志中多次 serde_json 序列化——`Vec<u8>` 的 base64 编码膨胀 33%
且 JSON 序列化慢数倍。

修复（es-storage `encode` 模块，存储值全量 bincode，快照文件头 meta 保留
JSON）：优化后 3 节点 TLS 复测（2026-08-12，原始 JSON 见
`target/perf-results/result-20260812-102146.json`；serde_json 基线为 2026-07
历史记录，原始 JSON 已清理）：

| 事件大小 | 批量写入 MB/s | 提升 | 全量读 MB/s | 提升 | 单条 append p50/p99 (ms) |
|---|---|---|---|---|---|
| 1KB | 37.1（原 10.7） | **3.5×** | 225（原 63） | 3.6× | 0.41 / 0.89（原 0.46 / 2.2） |
| 10KB | 49.0（原 9.2） | **5.3×** | 267（原 61） | 4.4× | 0.86 / 1.58（原 1.0 / 1.7） |
| 100KB | 41.4（原 7.9） | **5.2×** | 256（原 64） | 4.0× | 1.95 / 3.72（原 7.2 / 70.9） |

- 写入吞吐 **3.5–5.3×**，读取/订阅 **3.6–4.4×**（读已接近本机环回 + 单客户端平台）
- 100KB 单条 p99 从 70.9ms 降到 3.7ms（**19×**），尾延迟显著收敛
- 读取瓶颈随序列化提速转移到页固定开销，不再随事件大小变化
- 存储格式为内部格式（与网络 proto / 客户端 API 解耦），无兼容影响；
  离线工具（快照恢复）与迁移复制路径（`AppendMigrated` 走同一 encode）已同步
  同一格式，全量测试 422 项（含多节点 12 项）通过

### 限制

- 单客户端顺序压测，未覆盖并发客户端扩展性
- 本机 3 进程（127.0.0.1），不含真实网络延迟与跨机抖动
- 数据目录为本地磁盘（非 tmpfs）

## 存储层基准（criterion）

## 运行方式

```bash
# 运行全部基准测试
cargo bench -p es-storage

# 只运行某一项
cargo bench -p es-storage --bench storage_bench -- read_empty_stream

# 快速验证(不测量,只跑一遍确认无 panic)
cargo bench -p es-storage --bench storage_bench -- --test

# 查看 HTML 报告
open target/criterion/report/index.html
```

## 基准结果

测试环境:
- 平台: darwin (Apple Silicon)
- Rust: 1.94.1
- 编译: `--release` (opt-level=3)
- 存储: surrealkv 0.21.3 (LSM),数据在 tmpfs 临时目录

### 读取延迟

| 场景 | 中位数 | 说明 |
|---|---|---|
| `read_empty_stream` | **1.50 µs** | 读不存在的流,走完整 range 扫描但立即返回空 |

这个数字反映了单次存储层查询的**固定开销**:开事务 + 构造 key + range 扫描 + 判空。
真实读取会在此基础上加上反序列化每条事件的成本。

## 未覆盖的场景

以下场景需要完整 Raft 环境或多节点集群,当前基准测试未覆盖:

| 场景 | 为何未测 | 如何补 |
|---|---|---|
| **写入吞吐** | `apply` 是 `RaftStateMachine` trait 的内部方法,基准测试无法直接调用;走 `client_write` 需完整 Raft 实例 | 建单节点集群,通过 gRPC 压测 |
| **读取大流** | 同上,需先通过 Raft 写入数据 | 同上 |
| **Raft 复制延迟** | 需多节点集群 | 用 `partition_test.rs` 的进程内集群加计时 |
| **跨分片 ReadAll 归并成本** | 需多分片数据 | 同上 |

**为何不绕过 Raft 直接写存储层**:那样测出的数字不反映真实路径。
EventStore 的写入必须经过 Raft 达成共识,绕过它测出的吞吐会严重偏高,
反而误导容量规划。宁可不测,也不给出误导性数字。

## 基准测试的技术要点

### surrealkv 需要 tokio runtime 上下文

`TreeBuilder::build()` 与 `Transaction::commit()` 都要求运行在 tokio runtime 内,
否则 panic:`there is no reactor running`。

criterion 有自己的 `main`,不能用 `#[tokio::main]`。解决办法是用全局 runtime:

```rust
static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

// 建 tree 与提交事务都包在 block_on 里
let storage = RT.block_on(async { /* 建 tree */ });
```

### 不能在 `iter_batched` 的同步 setup 里 block_on

`b.to_async(&rt).iter_batched(setup, ...)` 中,setup 闭包已在 runtime 内执行,
再调 `rt.block_on` 会 panic:`Cannot start a runtime from within a runtime`。

当前实现改用同步 `b.iter(|| RT.block_on(async { ... }))`,把整个操作
(含数据准备)放进一次 `block_on`,避免嵌套。代价是数据准备时间计入测量,
但对操作本身耗时几十毫秒的场景影响可接受,且反映真实使用成本。

## 后续改进

- [x] 端到端写入吞吐:建单节点集群,通过 gRPC 压测 Append（已扩展为 3 节点 TLS 集群全链路，见上文）
- [x] 大流读取延迟:1k / 10k / 100k 事件的流（事件大小 1KB/10KB/100KB，各 50MB）
- [x] 存储编码优化:serde_json → bincode（2026-08-12，写入 4–6×、读取 3.5–4×，见「优化」章节）
- [ ] Raft 复制延迟:leader 写入到 follower apply 的时间分布（可扩展 perf_test.rs 记录每批副本 apply 水位）
- [ ] 真实数据量的在线迁移压测:含事件复制而非只有流元数据（esctl migrate 全链路）
- [ ] 火焰图分析:定位热点(`cargo flamegraph`)
- [ ] 并发客户端压测（当前为单客户端顺序基线）
