# 性能基准测试

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

### Reshard 吞吐

| 流数 | 中位数 | 每流成本 |
|---|---|---|
| 10 | **47.8 ms** | 4.78 ms/流 |
| 100 | **50.9 ms** | 0.51 ms/流 |

**关键观察**:从 10 流到 100 流,总耗时只增加 6%,说明:
- **固定开销占主导**:建两个 surrealkv tree(源与目标)、开关事务、LSM 初始化
- **单流处理成本极低**:0.5 ms/流,且随规模摊薄

这意味着 reshard 的耗时主要由**数据总量**而非流数决定。上面的测试每流只有
StreamMeta 无事件,真实场景下事件读写会成为主要成本。

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
但对 reshard 这类本身耗时几十毫秒的操作影响可接受,且反映真实使用成本。

## 后续改进

- [ ] 端到端写入吞吐:建单节点集群,通过 gRPC 压测 Append
- [ ] 大流读取延迟:1k / 10k / 100k 事件的流
- [ ] Raft 复制延迟:leader 写入到 follower apply 的时间分布
- [ ] 真实数据量的 reshard:含事件而非只有 StreamMeta
- [ ] 火焰图分析:定位热点(`cargo flamegraph`)
