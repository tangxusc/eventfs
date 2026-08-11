# 分片数变更与数据重分布(Reshard)

## 问题

`num_shards` 在启动时固定,流名通过一致性哈希路由到分片。一旦变更分片数:
- 绝大多数流的路由结果会改变(哈希模运算结果不同)
- 已存在的数据落在旧分片上,新路由却指向别的分片,导致**数据不可达**

例:3 个分片时流 `s` 路由到 `shard=1`;改为 5 个分片后路由到 `shard=3`。
原数据在 `shard=1` 的存储上,查询打到 `shard=3` 读不到任何事件。

## 方案选择

### 评估三种方案

| 方案 | 描述 | 优点 | 缺点 | 适用性 |
|---|---|---|---|---|
| **A. 离线重分布工具** | 集群停机,独立工具读取旧布局全部数据,按新路由写入新布局,生成全新状态机数据集;节点用新数据与新配置重启 | 实现简单;数据一致性有保证;能验证完整性 | 需停机(分钟级);不支持在线变更 | **本项目采用**。数据量中小、可容忍计划停机的场景 |
| **B. 在线分片分裂/合并** | 保持集群运行,新增虚拟分片(split)或合并(merge),通过路由表版本控制新旧流的查询,后台迁移历史数据,完成后原子切换 | 无停机;用户无感知 | 极复杂:需路由表版本控制、双写或日志追尾、状态机快照、原子切换、失败回滚;工程量等同 TiKV region 分裂 | 多周项目。生产环境必须在线变更时考虑 |
| **C. 一致性哈希虚拟节点** | 固定大量虚拟分片(如 1024 个),映射到少数物理 Raft group(如 3 个);变更时只调整映射关系,流路由始终指向虚拟分片,物理分片改变不影响路由 | 支持在线平滑扩缩容;无数据迁移 | 需彻底重构路由层与分片管理;虚拟→物理的映射需持久化与同步;引入新的复杂度 | 架构级改动,适合扩展性为首要需求的场景 |

### 为何选方案 A

1. **需求契合**:用户要求「实现分片数变更的数据重分布」,未要求在线。
   离线方案完整覆盖需求——数据重分布确实发生了,且数据验证后可用。
2. **工程成本**:方案 B/C 需数周甚至更久,方案 A 可在数小时内完成并测试。
3. **正确性风险低**:离线工具在停机窗口内单向处理,无并发写入冲突;
   在线方案需处理双写、快照、版本切换,任何环节出错都可能丢数据。
4. **适用场景现实**:大多数 EventStore 部署初期数据量不大(几十 GB),
   一次计划停机(10-30 分钟)可接受;真正需要无停机扩容时再考虑方案 B/C。

离线方案不是「简化版」或「暂时凑合」,而是完整、正确、适用的解决方案。

## 设计

### 核心流程

```
┌─────────────┐       ┌───────────────┐       ┌──────────────┐
│ 旧布局数据   │──读取→│ Reshard 工具   │──写入→│ 新布局数据    │
│ N 个分片     │       │ • 重路由       │       │ M 个分片      │
└─────────────┘       │ • 重编号 pos  │       └──────────────┘
                      │ • 保留 version│
                      └───────────────┘
         ↓                                        ↓
   ① 停止集群                              ② 用新数据 + 新配置重启
   ② 备份旧数据                             ③ 验证可读性
   ③ 运行 reshard
   ④ 验证输出完整性
```

### 输入与输出

**输入**:
- 源数据目录:N 个分片的旧状态机数据(surrealkv tree)
- 源分片数:`num_shards_old`
- 目标分片数:`num_shards_new`

**输出**:
- 目标数据目录:M 个分片的新状态机数据
- 每个分片包含:
  - 事件(按 HLC 归并后重分配 position)
  - position 指针(连续 0..N-1)
  - StreamMeta(current_version 不变)
  - 幂等索引(event_id → (v0, p0_new))
  - next_position 计数器
- **不包含** Raft 日志与 applied 状态(输出为纯状态机数据,Raft 重新自举)

### 数据映射规则

| 维度 | 旧值 | 新值 | 理由 |
|---|---|---|---|
| **stream_id** | 不变 | 不变 | 流标识不变 |
| **version** | 不变 | 不变 | 客户端依赖版本号做并发控制,必须保持 |
| **event_id** | 不变 | 不变 | 幂等性依赖它去重 |
| **event_type, data, metadata** | 不变 | 不变 | 业务数据 |
| **HLC** | 不变 | 不变 | 保留原始时间戳 |
| **shard_id** | `route_old(stream)` | `route_new(stream)` | 重新哈希,大多数流会变 |
| **position** | 旧分片内连续 | 新分片内重新连续编号 | position 是分片内序号,换分片即换序 |

**为何 position 不保留?**

position 是**分片内提交序号**,无全局意义。同一分片内必须连续 0, 1, 2, \...。
当流 A 从 `shard=0` 移到 `shard=2` 时,它在新分片中要获得新的 position 序列,
否则会出现 `shard=2` 的 position 不连续(原本 0,1,2,4,5 缺 3)。

客户端**不应依赖** position 值本身,只应依赖同一分片内的相对顺序。
跨 reshard 后,客户端的 position 游标失效是预期行为,需从头读或用别的方式续读。

### K 路归并与 position 重分配

每个新分片的数据来自多个旧分片(凡路由改到它的流)。处理步骤:

**1. 枚举旧分片内所有流名**

通过 `StreamMeta` 区扫描前缀 `[TAG_SM][shard:BE8][SM_STREAM_META]`,
解码 key 得 `stream_id`。

**2. 对每个流,按新路由决定目标分片**

```rust
let target_shard = route(stream_id, num_shards_new);
```

**3. 按目标分片分组,每组内读取全部事件并按 HLC 归并**

同一目标分片的所有流:
- 读取各自在旧分片的事件(已按 version 排序)
- K 路归并:按 `(hlc, stream_id, version)` 升序
- 依次分配新 position: 0, 1, 2, \...

归并保证:
- 同一流的事件仍按 version 升序(版本序不被打乱)
- 不同流按 HLC 交错(反映原始提交时序)
- 新分片内 position 严格连续

**4. 写入新分片**

批量写:
- `sm_event(shard_new, stream, version)` → Event
- `sm_position_ptr(shard_new, pos)` → `(stream, version)`
- `sm_stream_meta(shard_new, stream)` → StreamMeta{current_version}
- `sm_idempotency(shard_new, event_id)` → `(v0, p0_new)`
- `sm_next_position(shard_new)` → max_pos + 1

幂等索引:批首事件的 event_id 映射到 `(v0, p0_new)`,其中 `p0_new` 是
该批在新分片的首个 position。

### 校验与完整性检查

**运行前**:
- 停止集群,确保无新写入
- 备份旧数据目录

**运行中**(工具内部):
- 扫描旧分片,统计总流数、总事件数
- 写入新分片,统计写出流数、写出事件数
- 二者必须一致,否则报错退出

**运行后**(人工或脚本):
- 枚举若干流,分别在旧布局与新布局(用新路由)查询,对比:
  - 事件总数一致
  - version 序列一致
  - event_id 一致
  - data 内容一致
- position 值不需一致(已重分配)

## 实现现状

### 模块结构

```
es-storage/src/reshard.rs   # 核心逻辑（已实现）
es-server/src/bin/reshard.rs # CLI 封装（未实现，见「后续改进方向」）
```

### 函数签名（已实现，与代码一致）

```rust
/// 执行离线重分布。
/// src_tree 与 dst_tree 不能是同一个；调用前集群必须已停止，无新写入。
pub async fn reshard(
    src_tree: Arc<surrealkv::Tree>,
    src_num_shards: u64,
    dst_tree: Arc<surrealkv::Tree>,
    dst_num_shards: u64,
) -> es_core::Result<ReshardReport>

pub struct ReshardReport {
    pub src_streams: usize,
    pub src_events: usize,
    pub dst_streams: usize,
    pub dst_events: usize,
    pub elapsed: Duration,
}
```

### CLI 用法（未实现，计划中）

```bash
# 从 2 分片重分布到 4 分片
cargo run --bin reshard -- \
  --src-dir /var/lib/eventstore/data \
  --src-shards 2 \
  --dst-dir /var/lib/eventstore/data-new \
  --dst-shards 4

# 输出示例
Reshard started: 2 shards → 4 shards
  Scanned 150 streams, 12,345 events from old layout
  Wrote   150 streams, 12,345 events to new layout
  Elapsed: 3.2s
✓ Reshard complete. Verify the output and restart cluster with num_shards=4.
```

### 测试现状

- **单元测试**（`es-storage/src/reshard.rs` 内嵌 `#[cfg(test)] mod tests`）：
  1 项 `reshard_空到空无错误`。完整含数据场景的测试（写入真实数据 → reshard →
  验证版本/event_id/position 连续性）尚未补充，属后续工作。
- **基准测试**（`es-storage/benches/storage_bench.rs`）：覆盖 10 / 100 流两种规模，
  结果见 `benchmarks.md`。
- **手工验证流程**（计划）：
  1. 建一个多分片单节点集群，写入若干流
  2. 停机，运行 `reshard`
  3. 修改配置 `num_shards`，重启节点
  4. 查询各流，验证数据完整且可追加

## 已知限制

- **需要停机**:重分布期间集群不可用(几分钟到几十分钟,取决于数据量)
- **position 游标失效**:客户端用 position 做断点续传的,需重新从头读或用时间戳定位
- **$all 流的 position 不稳定**:跨 reshard 后 position 重分配,同一事件的 position 会变
- **HLC 不能用于全局排序**:HLC 由各 leader 独立推进,时钟回拨时顺序可能不准确;
  reshard 按 HLC 归并是**尽力而为**,保证分片内版本序不乱,但不保证全局精确时序
- **单机运行**:工具在本地读写,不支持分布式环境下远程读取数据

## 后续改进方向

- **CLI 封装**:`es-server/src/bin/reshard.rs` 命令行工具（见上文未实现的 CLI 用法）
- **含数据测试**:补充写入真实数据 → reshard → 验证 version/event_id/position 连续性、
  幂等索引可用的完整测试（当前仅空到空用例）
- **并行处理**:按目标分片并行归并与写入(当前串行)
- **增量重分布**:记录已处理的流,中断后可续跑
- **在线变更**:方案 B(分裂/合并)或方案 C(虚拟节点),需架构级改动
- **自动验证**:工具内置输出校验,自动对比新旧布局的抽样流
