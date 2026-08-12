# 在线迁移（esctl migrate）

## 概述

在线迁移把流从一个 shard 搬到另一个 shard，**流的数据处理不暂停**（读写
全程可用），取代旧离线 reshard（`docs/reshard.md` 已删除，离线重分布下线）。

适用场景：

- **负载均衡**：部分 shard 承载的流数/流量明显偏高，把热点流迁到空闲 shard
- **容量调整**：新 shard 加入（运行期动态扩容，见 design.md §6.5）后，把流迁入
  以利用新增承载
- **孤儿流修复**：`esctl route check` 发现「存储有但路由表无记录」的流，
  `esctl migrate` 会自动枚举分片定位孤儿流所在 shard 并迁移（无需指定源）

迁移按 **Migration 服务原语**（显式 shard 寻址，不走路由表）驱动：读走源
leader、写走目标 leader（leader 由 `GetRaftState` 探测定位），任意节点执行
切换（版本仲裁收敛）。**数据面路由唯一权威是路由表**——切换前新写落源、
切换后新写落目标，迁移工具自己永远按显式 shard 读写，不受切换影响。

## 状态机

```
Preparing → FullCopying → Tailing → Switching → Draining → Verifying → Finalizing
```

| 阶段 | 动作 | 失败语义 |
|---|---|---|
| **Preparing** | 路由表查源归属（无记录 = 孤儿流 → 枚举分片自动定位）；**源 == 目标不报错**——检查其它分片是否有残留数据，有则直接进入排水收尾（上次切换后中断的自愈），无则提示完成；目标分片存在性检查；读源/目标元数据得版本差（dry-run 只报告此差） | 未产生任何写入，重跑安全 |
| **FullCopying** | 从「目标当前版本 +1」读源 `[from, to]` 补差（每批 500 条，打源 leader 本地存储读），逐条写目标（Exact 版本链）；循环直到追平源当前版本 | 已写事件被幂等索引挡住，重跑从目标当前版本续 |
| **Tailing** | 与 FullCopying 同一循环——FullCopying 追平时窗口内新写可能已落源，循环直到版本再次收敛 | 同上 |
| **Switching** | `SetStreamShard` 原子切换路由表（版本 +1 + 落盘 + 整表广播），**切换点** | 失败则未切换，重跑从 Preparing 开始 |
| **Draining** | 切换后客户端新写直达目标（路由已切）；复制从目标当前版本续，天然兼容并发写入；**客户端写入占用版本槽（Exact 冲突）时自动改用 Any 兜底**——目标分配新版本，事件载荷/event_id/hlc 保真，version 允许重排（数据保真优先）；收敛判据 = **目标版本 ≥ 源版本且源连续 N 轮安静**（`--drain-quiet-rounds`，间隔 2s） | 排水超时（`--drain-timeout-secs`，默认 300s）退出：数据无害（源未动、目标只多不少），可重跑完成排水 |
| **Verifying** | 源 ⊆ 目标：按 event_id 匹配，**内容保真**（hlc / event_type / data / metadata 全比对——复制中载荷截断或篡改必须在此拦截）；version 允许不同（排水 Any 兜底可能重排）；分页读取（整条流可能超 8MB 单消息，服务端已分块流式发送） | **失败自动回切路由到源**——源数据从未被动过，回切安全；已复制到目标的数据留着（重跑时幂等续写） |
| **Finalizing** | `DeleteStreamFromShard` 删除源分片数据（幂等 no-op） | 残留源数据无碍读写，重跑清尾即可 |

**为何 Verifying 不做数量相等断言**：切换后客户端新写直达目标，目标可能比源
多——源是旧数据，Finalizing 才删除。校验失败 = 复制遗漏（数据丢失），这正是
必须拦截的错误。

## 幂等原语

| 原语 | 幂等性保证 |
|---|---|
| `AppendMigrated` | 单事件一条 raft 日志，**hlc 保留源值**（迁移保真要求），期望版本链由工具驱动（version 0 用 `NoStream`，其余 `Exact(v-1)`）；**排水阶段冲突时工具自动改用 Any 兜底**；幂等索引逐事件记录 → 重放返回原结果，**断点续传不重复** |
| `SetStreamShard` | 切换是路由表版本号原子点；同值切换不重复 bump；接收方按版本仲裁，重复广播无害 |
| `DeleteStreamFromShard` | 不存在的流 no-op |

## 切换窗口语义

- 切换点（SetStreamShard）之前：新写落源，复制按源版本追（FullCopying/Tailing）
- 切换点之后：客户端新写直达目标；复制从**目标当前版本**续——切换窗口内
  源侧增量（旧客户端缓存的路由、广播未收敛窗口）在 Draining 阶段被补完
- 收敛判据：目标版本 ≥ 源版本 **且** 源连续 `drain-quiet-rounds` 轮（间隔 2s）
  无新增——两条件都满足才认为窗口彻底关闭
- 收敛前中断：重跑命令即可，Draining 从目标当前版本继续（数据无害）；
  若路由已切换（中断发生在切换后），重跑会检测到残留源数据并自动进入排水收尾

## 断点续传

FullCopying / Tailing / Draining 三段都以「目标当前版本」为起点读源补差：
已复制的事件被幂等索引挡住、不会重复；未复制的从当前位置继续。因此
**任意阶段中断后重跑本命令都是安全的**，无需清理现场。

## 用法

### 单流

```bash
# 把流 order-1 从当前 shard 迁到 shard 4（dry-run 先看计划与版本差）
esctl migrate --stream order-1 --to 4 --dry-run

# 正式迁移
esctl migrate --stream order-1 --to 4
# 输出：order-1: shard 1 → 4，N 条事件

# 源持续生产的流：调大排水安静轮数与超时
esctl migrate --stream order-1 --to 4 --drain-quiet-rounds 5 --drain-timeout-secs 600
```

### 批量（整个分片）

```bash
# 把 shard 1 的全部流迁到 shard 4（逐流独立状态机，失败隔离）
esctl migrate --shard 1 --to 4
```

- 失败的流单独重跑（`--stream`），成功的不受影响
- 完成后建议 `esctl route recount` 校准 per-shard 流计数

### 孤儿流检测与修复（route check）

```bash
esctl route check
# 孤儿：order-9 存在于 shard 2，但路由表无记录
esctl migrate --stream order-9 --to 2   # 合并修复（与路由表归属一致）
```

- **孤儿**：存储中有但路由表无记录——隐式建流跨节点竞态等残留，可迁移修复
- **虚挂**：路由表指向的分片与存储实际所在不一致——迁移切换后未收敛或
  路由表手工编辑出错，指向的写入会 NotFound；虚挂流需先确认路由表期望归属
  再迁移对齐

## 与旧离线 reshard 的对比

| 维度 | 旧 reshard（已下线） | esctl migrate |
|---|---|---|
| 停机 | 要求集群完全停机（LOCK 安全网） | **无需停机**，流数据处理不暂停 |
| 数据路径 | 读旧目录 → 归并重分配 position → 写新目录 | 源 leader 读 → 目标 leader 写（raft 复制），保留源 position/HLC |
| position | 全局重分配，跨 reshard 游标失效 | 目标分片内重新分配（换分片即换序，见 design.md §5.5），源分片旧 position 随 Finalizing 删除 |
| 断点续传 | 无（一次性离线工具） | 任意阶段中断重跑安全 |
| 失败回滚 | 无（只能备份恢复） | Verifying 失败自动回切路由 |
| 流路由 | `hash(stream_id) % num_shards` 隐式推导 | 路由表显式分配，切换即迁移的原子点 |

## 已知限制

- **写必须打到 leader**：迁移读写都定位各分片 leader（GetRaftState 探测）；
  选举期间 Unavailable 自动重试（写操作 10 次预算），重试耗尽报错可重跑
- **排水超时 ≠ 失败**：超时退出时源数据未动、目标数据只多不少，重跑命令
  继续排水即可；持续不收敛通常是源仍在高并发写入，应提高
  `--drain-quiet-rounds`/`--drain-timeout-secs`
- **Verifying 是全量对比**：大流迁移的校验耗时与数据量成正比（与复制同量级）
- **不做跨流原子性**：批量迁移是逐流独立状态机，每个流的切换点独立；
  需要整组原子迁移的场景应逐流确认（或串行执行批量）
- **计数漂移**：迁移不精确扣减路由表计数（设计上允许），用
  `esctl route recount` 校准
