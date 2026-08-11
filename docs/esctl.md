# esctl：EventStore 命令行管理工具

`esctl` 是参照 [etcdctl](https://etcd.io/docs/latest/etcdctl/) 的 EventStore 管理工具，
独立二进制（workspace 成员 `es-ctl`），覆盖数据面读写、订阅、集群组建与管理、
端点健康、离线 reshard。

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
| `--shards <N>` | 自动探测 | 分片总数；缺省时对 shard 0,1,2,… 逐次 `GetRaftState` 探测（首个 `not_found` 即边界），探测失败回退默认 8 并告警 |

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
esctl read <STREAM> [--from-version <N=0>] [--max-count <N=0>] [--backward]
esctl readall [--from-position <N=0>] [--from-positions <"shard:pos,...">]
       [--max-count <N=0>] [--backward] [--shard-ids <"0,1,3">]
esctl meta <STREAM>
```

- `read` 与 `readall` 走本地副本，follower 也可读（任一可达端点即可）
- `--max-count 0` 表示不限量；`--backward` 反向读，未指定 `--from-version` 时从最新开始
- `readall` 的 `--from-positions` 非空时覆盖 `--from-position` 与 `--shard-ids`；
  `--max-count` 取满时输出下一页续读游标（json 为 `next_from_positions` 字段，
  simple/table 为 stderr 提示行）
- 事件行格式（simple）：`{version}\t{RFC3339}\t[{event_type}]\t{data}`，
  data 非 UTF-8 时输出 `hex:..`

### 订阅

```
esctl watch <STREAM> [--from-exclusive <N>] [--from-start] [--once]
esctl watch --all [--shard <N=0>] [--from-exclusive <N>] [--from-start] [--once]
```

先补齐历史（catch-up），追平后显示「已追平，进入实时推送」并转为实时推送。
`--once` 追平即退出（退出码 0），供脚本与测试使用；不带 `--once` 持续运行，Ctrl-C 终止。
已知限制：`--all`（$all 订阅）服务端目前仅支持分片 0。

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
  initialize 不需要 leader；已初始化的分片报错（退出码 1）
- `member add`：先加为 learner（默认等待追平，`--no-blocking` 关闭），
  再 `change_membership` 提升为投票成员；`--learner-only` 只加 learner 不提升
- `member remove`：从投票成员中移除；`--retain` 降级为 learner 而非剔除。
  **learner 无法移除**（RaftAdmin 无 remove_learner RPC）；目标不在 voters 时校验失败
- `member list`：遍历 0..N × `--endpoints` 聚合 `GetRaftState`。
  已知限制：RPC 不暴露成员地址与 learner 集合，故无地址列、无 learner 行

### 端点健康

```
esctl status [--shards <N>]
```

对每个端点遍历全部分片探测 `GetRaftState`，输出可达性、leader 归属（leader_of /
following_of）与 term。全部端点不可达时退出码 1。

### 离线 reshard

```
esctl reshard --src-dir <DIR> --src-shards <N> --dst-dir <DIR> --dst-shards <M> [--yes]
```

变更分片数并重分布数据。**离线操作**：要求集群完全停机、已备份数据目录；
集群未停时目标目录被 LOCK 占用，命令直接拒绝（退出码 1）。核心逻辑复用
`es_storage::reshard::reshard()`（K 路归并 + position 重分配，保留
stream_id/version/event_id/HLC）。非 `--yes` 时交互确认（非交互 stdin 视为拒绝）。
目标目录已存在且非空时需 `--yes` 确认覆盖。参数与输出示例见 [docs/reshard.md](reshard.md)。

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
  TLS 装配（`--cacert` 严格校验 / 默认跳过校验，仅 https 生效）、按端点惰性建连并缓存
- **数据面写**（append）：依序尝试各端点（轮询起点分散负载）→ 非 leader 返回
  `Unavailable` 且 message 带 `leader_addr` 时优先重定向该地址 →
  `leader unknown`（选举中）退避重试并轮换其它端点 → 乐观冲突（`FailedPrecondition`）
  原样上抛。已知限制：openraft 不总填充 `leader_node` 信息（`leader_addr=` 为空），
  此时无法重定向，靠端点列表轮换兜底——多端点部署建议 `--endpoints` 给出全部节点
- **管理面写**（member add/remove）：管理面错误不带 leader 提示，先对每个端点
  `GetRaftState` 找 `is_leader` 的端点再执行，失败重跑最多 3 轮
- **读**（read/readall/meta）：任一可达端点即可

## 与 etcdctl 对应关系

| etcdctl | esctl | 说明 |
|---|---|---|
| `put` / `get` | `append` / `read` | 事件写读（带期望版本） |
| `watch` | `watch` | 订阅（catch-up → live） |
| `--endpoints` / `-w` / `--dial-timeout` | 同左 | 全局参数对齐 |
| `member list` / `member add/remove` | `member list` / `member add/remove` | 成员管理（esctl 按分片） |
| `endpoint health` / `endpoint status` | `status` | 端点健康视图 |
| `snapshot save/restore` | `reshard` | 离线数据操作（eventstore 无快照 RPC，以 reshard 对应） |
| `auth enable` / `user add` 等 | — | eventstore 无认证机制 |

## 测试

```bash
# 默认套件（单测 + 进程内 e2e + 离线 reshard）
cargo test -p es-ctl

# 三节点真实进程组建（需先 cargo build --bin eventstored；串行）
cargo test -p es-ctl --test multi_node_test -- --ignored --test-threads=1
```

覆盖率（行/分支 ≥80%）验收：

```bash
cargo llvm-cov -p es-ctl
```
