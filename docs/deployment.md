# 部署与运维

## 数据兼容

Aggregate-only 版本改变 protobuf、Raft 命令和持久化 key 空间。部署时必须使用全新、空的
`storage.data_dir`，不能挂载旧版本数据目录，也不能混合新旧节点滚动升级。系统不提供
迁移服务、CLI 或离线转换工具。

## 配置

以 [config.example.toml](../config.example.toml) 为权威示例。主要字段：

| 字段 | 默认/约束 | 用途 |
|---|---|---|
| `node.id` | 必填，非零 | Raft node ID |
| `node.listen_addr` | 必填 | AggregateStore、RaftRpc、RaftAdmin listener |
| `node.internal_listen_addr` | 缺省为公共地址 | AggregateStoreInternal listener |
| `node.peers` | 可空 | 非空时执行静态自动引导；所有节点必须一致 |
| `storage.data_dir` | 必填且全新 | 每节点数据根目录 |
| `storage.memtable_arena_bytes` | 默认 4 MiB | 每 Shard surrealkv arena |
| `placement.replication_factor` | 默认 1 | 每 Shard 投票成员数 |
| `placement.nodes` | 必填 | Shard 的 primary/replica 放置 |
| `limits.max_event_bytes` | 默认 1 MiB，最大 7 MiB | 单事件 data+metadata 上限 |
| `snapshot.compression` | `zstd` | `zstd` / `lz4` / `none` |
| `snapshot.keep` | 3 | 每 Shard 保留快照数 |
| `snapshot.max_chunk_size` | 3 MiB，最大 6 MiB | Raft snapshot transport 分块 |

每个 Shard 是独立 Raft group。`replication_factor` 必须等于该 Shard 在 placement 中的总
承载节点数，primary 集合必须互斥。配置热更新可增加节点或 Shard；运行期间不得移除现有
Shard。AggregateType 的 256 个虚拟分区由 catalog 放置到这些 Shard，调用方无需配置。
节点会先创建并自举全部新增本地 Shard，再原子发布新的 Shard 集合与 peer 定位器；创建、
地址校验或拓扑约束失败时保留旧运行状态。热更新前已创建的 AggregateStore module 也会
使用新拓扑，后续注册的 AggregateType 可放置到新增 Shard。

## 单节点

```toml
[node]
id = 1
listen_addr = "127.0.0.1:50051"
internal_listen_addr = "127.0.0.1:51051"

[storage]
data_dir = "./data/node1"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]
replica = []
```

```bash
cargo build --workspace --bins --locked
./target/debug/eventstored --config ./config.toml
./target/debug/esctl init --shard 0 --member 1@127.0.0.1:50051 --yes
./target/debug/esctl aggregate type register orders order
```

`node.peers` 为空时手工初始化每个 Shard。只有一个 Shard 时使用 `--shard 0`；多 Shard
可使用 `--all-shards --shards N`。

## 多节点静态引导

所有节点使用完全相同的 `node.peers` 和 `placement`，但 `node.id`、listen 地址和数据目录
各自不同。每个 peer 同时配置公共 `addr` 与内部 `internal_addr`。非空 peers 会在启动时
按 placement 为所有 Shard 组建对应 membership；不要再并发执行手工 `init`。

三节点推荐：

- `replication_factor = 3` 提供单节点故障容忍；
- 公共 listener 仅向客户端网段开放 AggregateStore/RaftAdmin 所需访问；
- RaftRpc 和内部 listener 仅允许集群节点访问；
- 每节点使用独立持久卷，禁止多个进程共享 `data_dir`。

健康检查：

```bash
esctl --endpoints http://node1:50051,http://node2:50051,http://node3:50051 status
esctl --endpoints http://node1:50051 member list
esctl --endpoints http://node1:50051 aggregate status
```

## TLS

`tls.cert_file` 与 `tls.key_file` 必须成对配置。`tls.ca_file` 存在时，节点间连接和官方
客户端严格验证证书链；未配置 CA 时 HTTPS 默认跳过验证，适合本地自签名环境但不适合
生产。TLS 部署的 peer 地址必须显式使用 `https://`。

```toml
[tls]
cert_file = "./certs/server.crt"
key_file = "./certs/server.key"
ca_file = "./certs/ca.crt"
```

证书 SAN 必须覆盖节点连接使用的主机名或 IP。证书轮换后重启节点。CLI 使用
`--cacert` 严格验证，或在受控测试环境使用 `--insecure-skip-tls-verify`。

## 成员变更

加入节点分两步：先作为 learner 追平，再变更完整 voter 集合。`esctl member add` 封装这
一流程；移除节点使用 expected voters 做 CAS，避免并发 membership 更新互相覆盖。

```bash
esctl member add --shard 0 --member 4@node4:50054 --promote
esctl member remove --shard 0 --node-id 2
```

placement 配置与 Raft membership 是两个事实：placement 决定节点应承载哪些 Shard，
membership 决定当前 Raft 投票集合。变更后必须同时检查 `status` 和 `member list`。

## 快照与恢复

快照是 Shard 状态机备份，不是聚合状态文档。列出和离线恢复：

```bash
esctl snapshot list /srv/eventfs/node1
esctl snapshot restore /srv/eventfs/node1 /backup/shard-0.snap --yes
```

恢复必须停机，且恢复文件、目标 Shard 和部署版本必须匹配。详细约束见
[snapshot.md](snapshot.md)。

## Docker Compose

仓库 Compose 拓扑运行三个服务节点和一个 FUSE 客户端。下载当前提交的 release artifact
后启动：

```bash
./scripts/download-release-artifact.sh
docker compose up --build -d
docker compose ps
docker compose exec client mountpoint /mnt/eventfs
```

容器内真实 FUSE 需要 `/dev/fuse` 和相应 capability。Compose 数据是临时测试数据，不得
作为旧版本升级路径。

## 故障排查

| 现象 | 检查 |
|---|---|
| `Unavailable` / leader hint | `esctl status`、端点连通性、Raft quorum |
| 聚合类型长期 REGISTERING | control/data Shard leader、内部 listener、partition fence |
| feed 出现 degraded | 对应 Shard 或内部 RPC 不可用；保留 cursor 重连 |
| 状态 CAS 冲突 | 重读 revision 后按业务规则重试 |
| 消费投递 stale lease | 停止处理旧 token，重新 Fetch |
| 启动反序列化失败 | 是否误用了旧数据目录；换全新目录 |
