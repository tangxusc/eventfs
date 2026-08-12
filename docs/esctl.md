# esctl：EventStore 命令行管理工具

`esctl` 是参照 [etcdctl](https://etcd.io/docs/latest/etcdctl/) 的 EventStore 管理工具，
独立二进制（workspace 成员 `es-ctl`），覆盖数据面读写、订阅、集群组建与管理、
端点健康、在线迁移。

## 构建

```bash
cargo build --bin esctl
./target/debug/esctl --help
```

## 全局参数（位于子命令之前）

| 参数 | 默认 | 说明 |
|---|---|---|
| `--endpoints <ADDRS>` | `http://127.0.0.1:50051` | 集群节点 gRPC 地址列表，逗号分隔；裸地址自动补 `http://` 前缀 |
| `--dial-timeout <SECS>` | 5 | 建立连接的超时时间（秒） |
| `--timeout <SECS>` | 10 | 单次 RPC 请求超时（秒），0 表示不设；watch 长连接不受影响 |
| `--cacert <FILE>` | 无 | 严格校验服务端证书的 CA 文件（PEM）；与 `--insecure-skip-tls-verify` 互斥；仅 https 端点生效 |
| `--insecure-skip-tls-verify` | false | 跳过 https 端点证书校验（自签友好，默认行为）；仅 https 端点生效 |
| `-w, --write-out <FMT>` | simple | 输出格式：`simple`（逐行文本）/ `table`（对齐表格）/ `json`（结构化） |
| `--shards <N>` | 自动探测 | 分片范围：显式指定时 = `0..N`（不触网）；缺省时逐端点 `ListShards` 取**并集**（节点只承载放置表分配的部分分片，旧「GetRaftState 连续扫描」在部分承载布局下会误探到 0）；探测失败回退默认 8 并告警 |

退出码：**0** 成功 / **1** 运行时失败（连接失败、无 leader、乐观并发冲突等）/ **2** 参数错误（clap）。

## 命令一览

### 数据面

```
esctl append <STREAM> --event-type <TYPE> (--data <STR> | --data-file <PATH>)
       [--metadata <STR> | --metadata-file <PATH>] [--event-id <UUID>]
       [--expected-version any|nostream|exists|<N>]
```

每次写入 1 条事件。`--event-id` 缺省随机生成 v4（幂等去重依赖它）。`--expected-version`
默认 `any`：`nostream` 要求流不存在（首次创建），`exists` 要求已存在，数字为精确版本。
期望版本冲突时报错并退出码 1。

```
esctl create-stream <STREAM>
```

显式创建流：服务端分配 shard（大致最少流）并记录路由表，返回 `shard_id` 与
目标分片 leader 地址（尽力探测，未知为空串）。幂等：流已存在时返回现有归属
（`exists=true`），不重复分配。**append 未知名流会隐式建流**（服务端分配并
记录归属），但读（`read`/`meta`）未创建流返回 NotFound（显式分配语义）。
预显示的「路由分片」仅为提示，以服务端落盘归属为准。

```
esctl read <STREAM> [--from-version <N=0>] [--max-count <N=0>] [--backward]
esctl readall [--from-position <N=0>] [--from-positions <"shard:pos,...">]
       [--max-count <N=0>] [--backward] [--shard-ids <"0,1,3">]
esctl meta <STREAM>
```

- `read` 与 `readall` 走本地副本，follower 也可读（任一可达端点即可）
- `read`/`meta` 读未创建（路由表无记录）的流 → NotFound（退出码 1）
- `--max-count 0` 表示不限量；`--backward` 反向读，未指定 `--from-version` 时从最新开始
- `readall` 的 `--from-positions` 非空时覆盖 `--from-position` 与 `--shard-ids`；
  `--max-count` 取满时输出下一页续读游标（json 为 `next_from_positions` 字段，
  simple/table 为 stderr 提示行）。**续读游标由服务端驱动**（覆盖全部分片，
  本页被跨分片归并丢弃的分片也会推进），把提示的游标原样传给 `--from-positions`
  即续读——不要自行从本页事件推算游标，页内缺失分片的事件会永久读不到
- **反向终止**：`--backward` 反向读到分片最早事件（position 0）后，该分片游标
  带 `ended` 标记（已读尽，不再出现在续读提示中）；继续翻页会得到空页
  ——**空页即终止**，正反两个方向一致
- 事件行格式（simple）：`{version}\t{RFC3339}\t[{event_type}]\t{data}`，
  data 非 UTF-8 时输出 `hex:..`

### 订阅

```
esctl watch <STREAM> [--from-exclusive <N>] [--from-start] [--once]
esctl watch --all [--shard <N=0>] [--from-exclusive <N>] [--from-start] [--once]
```

先补齐历史（catch-up），追平后显示「已追平，进入实时推送」并转为实时推送。
`--once` 追平即退出（退出码 0），供脚本与测试使用；不带 `--once` 持续运行，Ctrl-C 终止。
`--all` 订阅 $all 时用 `--shard <N>` 指定分片（默认 0）：一次订阅一个分片的 $all，
多分片需各自发起订阅。`--once` 在收到 caught_up 前流被关闭（如订阅者落后被服务端断开）
时以退出码 1 报错——退出码 0 只代表「已追平」。

### 管理面

```
esctl init [--shard <N> | --all-shards] --member <ID@ADDR>... [--yes]
esctl member add [--shard <N> | --all-shards] --member <ID@ADDR>
       [--no-blocking] [--learner-only]
esctl member remove [--shard <N> | --all-shards] --node-id <ID> [--retain]
esctl member list [--shards <N>]
```

- **每个分片是独立的 Raft group**：多分片集群必须对每个分片各自执行。
  `--all-shards` 对全部分片执行相同操作（分片数来自 `--shards`/自动探测）
- `init`：把给定成员写入首条 membership 日志，只需在一个节点调用一次；
  initialize 不需要 leader；已初始化的分片报错（退出码 1）。
  `--all-shards` 遇已初始化的分片**告警后继续补完其余分片**，最后整体报错
- `member add`：先加为 learner（默认等待追平，`--no-blocking` 关闭），
  再 `change_membership` 提升为投票成员；`--learner-only` 只加 learner 不提升。
  `change_membership` 携带 CAS 期望快照（当前 voters 集合）：读-改-写窗口内
  并发变更会使后到者返回 `FailedPrecondition`，esctl 自动重读重试
- `member remove`：从投票成员中移除；`--retain` 降级为 learner 而非剔除。
  **learner 无法移除**（RaftAdmin 无 remove_learner RPC）；目标不在 voters 时校验失败
- `member list`：遍历 0..N × `--endpoints` 聚合 `GetRaftState`；
  全部端点不可达时退出码 1（不把网络故障误报为"未初始化"）。
  已知限制：RPC 不暴露成员地址与 learner 集合，故无地址列、无 learner 行

### 端点健康

```
esctl status [--shards <N>]
```

对每个端点遍历全部分片探测 `GetRaftState`，输出可达性、leader 归属（leader_of /
following_of）与 term。全部端点不可达时退出码 1。

### 离线快照（snapshot list / restore）

```
esctl snapshot list <data_dir> [--snapshot-dir <DIR>]
esctl snapshot restore <data_dir> <snapshot_file> [--snapshot-dir <DIR>] [--yes]
```

快照存独立文件（`{data_dir}/snapshots/snap-{shard}-{term}-{index}.esnap`，zstd/lz4 压缩）。
`--snapshot-dir` 缺省 `{data_dir}/snapshots`；服务端配置了 `[snapshot].dir`
自定义目录时须显式传入（否则 CLI 与服务器的快照视图不一致）。

- **list**：列出全部快照文件（分片 / term / index / snapshot_id / 压缩算法 / 体积），
  只读文件头不解压 payload；损坏文件标记「损坏」不中断。目录不存在时报错。
- **restore**：把快照恢复到数据目录中对应分片（快照头记录分片号）。
  **离线操作**：要求集群完全停机（LOCK 安全网，在线执行直接拒绝，退出码 1）；
  非 `--yes` 时交互确认。恢复语义：该分片回到快照时刻——清空日志与状态机
  （保留 vote），`raft_last_purged`/`raft_committed` 写回快照点，重启后以快照点
  继续参与集群（单节点直接恢复领导；多节点由 leader 复制快照点之后的日志或新快照）。
  与 etcd `snapshot restore` 等价但作用于单分片。

### 流路由表

```
esctl route [--recount] [--check]
```

查看/校准流路由表（stream → shard 归属）。

- 默认展示路由表：逐条 `stream -> shard N` + 表版本（`version=N`）；
  json 格式含 `streams` 与 `shard_stream_counts`
- `--recount`：校准 per-shard 流计数（从路由表重建，版本不变），并输出校准后的表
- `--check`（与 `--recount` 互斥）：**孤儿流检测**——枚举各分片实际存储的流
  （`ListStreams`，打各 shard leader）与路由表对比：
  - **孤儿**：存储中有但路由表无记录（隐式建流跨节点竞态等残留），
    可用 `migrate --stream <s> --to <shard>` 合并修复
  - **虚挂**：路由表指向的分片与存储实际所在不一致（迁移切换后未收敛或
    路由表手工编辑出错），指向的写入会 NotFound

### 在线迁移（取代旧 reshard）

```
esctl migrate (--stream <STREAM> | --shard <N>) --to <M>
       [--dry-run] [--drain-quiet-rounds <N=2>] [--drain-timeout-secs <S=300>]
```

在线迁移流到目标分片，**流的数据处理不暂停**。`--stream` 迁移单个流；
`--shard` 批量迁移整个分片的全部流（逐流独立状态机，失败隔离——失败的流
可单独重跑，其余不受影响）。`--to` 目标分片；源与目标相同报错。
`--dry-run` 只报告迁移计划与版本差，不执行。排水收敛判据 = 目标版本 ≥ 源版本
且源连续 `--drain-quiet-rounds` 次（间隔 2s）无新增；超过
`--drain-timeout-secs`（默认 300s）退出（数据无害，可重跑完成排水）。

状态机 `Preparing → FullCopying → Tailing → Switching → Draining → Verifying → Finalizing`；
切换点（SetStreamShard）后客户端新写直达目标，收敛后校验失败自动回切路由。
复制按「目标当前版本」读源补差（Exact 版本链写目标，幂等索引防重放），
**断点续传天然成立，重复执行无害**。完成后建议 `esctl route recount` 校准流计数。
完整设计见 [docs/migrate.md](migrate.md)。

## 输出格式

`-w simple`（默认）逐行文本；`-w table` 对齐表格（示例）：

```
$ esctl -w table member list
SHARD  NODE  STATE     TERM  LEADER  LAST_APPLIED  VOTER
0      1     Leader    3     1       128           yes
0      2     Follower  3     1       128           yes
```

`-w json` 结构化输出，便于脚本解析（`jq` 等）。

## 连接与 leader 发现策略

- **连接**：端点归一化（裸地址补 `http://`，与节点间 Raft 网络同一规则）、
  TLS 装配（`--cacert` 严格校验 / 默认跳过校验，仅 https 生效）、按端点惰性建连并缓存。
  单个端点**建连失败会故障转移到下一个端点**（不会中止整个命令）
- **数据面写**（append）：依序尝试各端点（轮询起点分散负载）→ 非 leader 返回
  `Unavailable` 且 message 带 `leader_addr` 时优先重定向该地址 →
  `leader unknown`（选举中）退避重试并轮换其它端点 → 乐观冲突（`FailedPrecondition`）
  原样上抛。已知限制：openraft 不总填充 `leader_node` 信息（`leader_addr=` 为空），
  此时无法重定向，靠端点列表轮换兜底——多端点部署建议 `--endpoints` 给出全部节点
- **管理面写**（member add/remove）：管理面错误不带 leader 提示，先对每个端点
  `GetRaftState` 找 `is_leader` 的端点再执行；leader 探测失败（选举中/端点不可达）
  与 RPC 失败都重试，最多 3 轮（分片未初始化是永久错误，直接返回）
- **读**（read/readall/meta）：任一可达端点即可

## 与 etcdctl 对应关系

| etcdctl | esctl | 说明 |
|---|---|---|
| `put` / `get` | `append` / `read` | 事件写读（带期望版本） |
| `watch` | `watch` | 订阅（catch-up → live） |
| `--endpoints` / `-w` / `--dial-timeout` | 同左 | 全局参数对齐 |
| `member list` / `member add/remove` | `member list` / `member add/remove` | 成员管理（esctl 按分片） |
| `endpoint health` / `endpoint status` | `status` | 端点健康视图 |
| `snapshot save` | `snapshot list` | 快照已存独立文件，list 查看后可自行备份/拷贝 |
| `snapshot restore` | `snapshot restore` | 离线恢复到快照点（作用于单分片，需停机） |
| `auth enable` / `user add` 等 | — | eventstore 无认证机制 |

## 测试

```bash
# 默认套件（单测 + 进程内 e2e + 在线迁移 e2e）
cargo test -p es-ctl

# 三节点真实进程组建（需先 cargo build --bin eventstored；串行）
cargo test -p es-ctl --test multi_node_test -- --ignored --test-threads=1
```

覆盖率（行/分支 ≥80%）验收：

```bash
cargo llvm-cov -p es-ctl
```
