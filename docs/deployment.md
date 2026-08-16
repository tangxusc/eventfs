# 部署与运维

本文记录 `eventstored` 的构建、配置、集群组建、TLS、动态扩容、Release artifact
和本地 Compose 集群。协议与数据路径见 [design.md](design.md)，CLI 参数见
[esctl.md](esctl.md)。

## 前置条件

源码构建需要：

- Rust 1.88 或更高版本；
- `protoc`；
- 与目标平台匹配的本地构建工具链。

默认使用 debug 构建：

```bash
cargo build --workspace --bins --locked
```

主要二进制位于 `target/debug/eventstored`、`target/debug/esctl`；Linux 还可运行
`target/debug/eventfs-fuse`。

## 服务配置

`eventstored --config <PATH>` 按扩展名读取 TOML 或 JSON。配置文件加载、解析或校验失败
会直接退出，不会静默回退默认配置。旧 `[shards] num_shards` 格式不再支持。
当前 watcher 要求配置路径包含可监听的父目录；请使用 `./config.toml` 或绝对路径。
传裸文件名 `config.toml` 时服务仍会启动，但日志会报告 watcher 不可用，运行期配置
热更新随之停用。

最小单节点配置：

```toml
[node]
id = 1
listen_addr = "127.0.0.1:50051"

[storage]
data_dir = "./data/node1"

[placement]
replication_factor = 1

[[placement.nodes]]
id = 1
primary = [0]
replica = []
```

关键配置：

| 字段 | 默认/约束 | 说明 |
|---|---|---|
| `node.id` | 必填 | 集群内唯一节点 ID |
| `node.listen_addr` | 必填 | 六个公共 gRPC service 共用的监听地址 |
| `node.internal_listen_addr` | 多节点必填 | 三个节点间 service 的监听地址，不能与公共地址相同 |
| `node.peers` | 缺省为空 | 非空时触发自动组建；所有节点必须配置同一成员集合 |
| `storage.data_dir` | 必填 | 每个节点必须独立；内部按 `shard-{id}/` 分树 |
| `storage.memtable_arena_bytes` | 4 MiB | 每个 Shard 一份，合法范围 1 MiB 到 16 MiB |
| `placement.replication_factor` | 1 | 每个 Shard 的投票成员数；运行期不调整 rf |
| `placement.nodes` | 必须非空 | `primary` 互斥，`primary + replica` 承载数必须等于 rf |
| `snapshot.compression` | `zstd` | 支持 `zstd`、`lz4`、`none` |
| `snapshot.keep` | 3 | 至少保留一个快照 |
| `limits.max_event_bytes` | 1 MiB | 单事件 data 与 metadata 上限 |
| `limits.max_append_batch_bytes` | 7 MiB | 单次 Append 的 protobuf 编码后上限 |

完整三节点 rf=2 示例见 [config.example.toml](../config.example.toml)。该文件显式启用了
TLS，直接使用前需要准备证书；明文开发环境应删除整个 `[tls]` 段，并把 peer 地址改为
`http://` 或裸地址。

`--node-id` 与 `--listen` 可以覆盖配置文件对应字段。热加载仍以启动时指定的配置文件
为来源，并按覆盖后的实际节点 ID 计算本地新增 Shard。

## 单节点组建

`node.peers` 为空时不会自动初始化 Raft。启动服务后执行：

```bash
eventstored --config ./config.toml
```

在另一个终端初始化每个 Shard：

```bash
esctl init --shard 0 --member 1@127.0.0.1:50051 --yes
```

多 Shard 可以用 `--all-shards`，其范围来自 `--shards` 或各端点
`ListShards` 的并集。

## 多节点自动组建

多节点配置必须满足：

- 每个节点的 `node.peers` 成员集合一致，并包含本节点；
- 每个 peer 同时配置公共 `addr` 和内部 `internal_addr`；
- 每个节点的 `placement` 一致；
- 每个节点使用不同的 `storage.data_dir`；
- 网络允许节点互访公共和内部端口，内部端口不对客户端开放。

`node.peers` 非空时，各节点启动后按 Shard 自动探测现有集群。没有现存日志时使用该
Shard 的完整承载成员初始化；已有日志的重启节点从本地恢复，不再次组建。每个 Shard
拥有独立 membership 和 leader，成员只包括 placement 中承载该 Shard 的节点。

不同节点若使用不同的 peers 或 placement，可能形成双集群，运行时无法自动修复。

## 多节点手动组建

`node.peers` 为空时，也可以通过 `esctl` 逐 Shard 组建：

1. 在一个节点上用单成员 `init` 自举；
2. 用 `member add` 添加 learner 并等待追平；
3. 将 learner 提升为投票成员。

```bash
esctl init --shard 0 --member 1@127.0.0.1:50051 --yes
esctl member add --shard 0 --member 2@127.0.0.1:50052
```

`member add` 默认完成 learner 添加和投票成员提升；只保留 learner 时传入
`--learner-only`。详细成员变更语义见 [esctl.md](esctl.md#管理面)。

## TLS

配置 `[tls]` 即同时为公共和内部 listener 启用 TLS：

```toml
[tls]
cert_file = "./certs/server.crt"
key_file = "./certs/server.key"
ca_file = "./certs/ca.crt"
```

`cert_file` 与 `key_file` 必须成对存在且非空。配置 `ca_file` 后，节点间客户端
严格校验证书链；不配置 CA 时，HTTPS 客户端默认跳过证书校验，仅适合受控开发环境。

TLS 部署中的 `node.peers[].addr` 与 `internal_addr` 必须显式使用 `https://`。
裸地址会被补为 `http://`，导致节点用明文连接 TLS listener。

自签终端证书必须显式设置 `CA:FALSE`，并包含连接地址对应的 SAN：

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server.key -out server.crt \
  -days 365 -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1" \
  -addext "basicConstraints=critical,CA:FALSE"
```

证书和 CA 在启动时读取，轮换后需要重启节点。

## 动态增加节点与 Shard

运行期扩容流程：

1. 在所有节点配置中加入相同的 `node.peers` 条目；
2. 在所有节点的 `placement.nodes` 中加入相同的新节点和 Shard 放置；
3. 等待 watcher 校验并热加载配置；
4. 用 `esctl status` 和 `esctl member list` 检查新 Shard；
5. 需要搬迁已有 Stream 时运行 `esctl migrate`。

配置解析或语义校验失败时保留旧运行状态。新增本地 Shard 会动态创建并幂等自举；
从 placement 移除已有 Shard 只会告警，数据目录和运行实例不会被在线删除。控制 Shard
不能移除，`replication_factor` 也不支持在线调整。

## 数据与关闭

每个 Shard 使用独立的 `{data_dir}/shard-{id}/` surrealkv tree 和 LOCK。单进程内
不能重复打开同一目录，不同节点也不能共享数据目录。

Ctrl-C 或 SIGTERM 会先停止 watcher，再逐 Shard 停止 Raft、刷新并关闭存储。若进程没有
完成关闭，同目录重启可能暂时报 `already locked`；应先确认旧进程已退出，不能通过删除
LOCK 文件绕过仍在运行的写入者。

快照文件默认位于 `{data_dir}/snapshots/`。离线恢复操作见
[snapshot.md](snapshot.md)。

## Release 产物

`.github/workflows/release.yml` 只接受 `workflow_dispatch`，不响应 tag，也不创建
GitHub Release。构建矩阵：

| 目标 | 二进制 |
|---|---|
| `x86_64-unknown-linux-gnu` | `eventstored`、`esctl`、`eventfs-fuse` |
| `aarch64-unknown-linux-gnu` | `eventstored`、`esctl`、`eventfs-fuse` |
| `x86_64-apple-darwin` | `eventstored`、`esctl` |
| `aarch64-apple-darwin` | `eventstored`、`esctl` |

每个平台压缩包名为 `eventfs-sha-<commit>-<target>.tar.gz`。汇总 artifact
`eventfs-release-assets-sha-<commit>` 保留 30 天，并包含 `SHA256SUMS`。

Linux 可验证全部摘要：

```bash
sha256sum --check SHA256SUMS
```

macOS 可使用：

```bash
shasum -a 256 --check SHA256SUMS
```

## Docker 三节点集群

[compose.yaml](../compose.yaml) 不在本地编译 Rust，而是使用当前提交对应的 Linux
Release artifact。下载脚本需要 `gh` 已登录，并要求当前分支、提交存在成功的手动
`Release` workflow：

```bash
./scripts/download-release-artifact.sh
docker compose up --build -d
docker compose ps
```

脚本下载汇总 artifact、验证 `SHA256SUMS`，再按 Docker 宿主架构生成
`.docker-artifacts/eventfs-linux-native.tar.gz`。

使用代理：

```bash
EVENTFS_PROXY=http://127.0.0.1:7897 \
  ./scripts/download-release-artifact.sh
```

下载其他提交时，`EVENTFS_RUN_ID` 和 `EVENTFS_VERSION` 必须成对匹配：

```bash
EVENTFS_RUN_ID=<run-id> EVENTFS_VERSION=sha-<commit> \
  ./scripts/download-release-artifact.sh
```

Compose 拓扑：

- 三个 Debian 12 server，宿主端口为 `50051`、`50052`、`50053`；
- 一个 Debian 13 client，运行 `eventfs-fuse` 并包含 `esctl`；
- 内部端口只在 Compose 网络中使用，不映射到宿主；
- client 只增加 `SYS_ADMIN`、映射 `/dev/fuse` 并禁用 AppArmor profile，不使用
  `privileged`；
- 数据目录与 `/mnt/eventfs` 不使用持久化 volume。

写读检查：

```bash
docker compose exec client esctl \
  --endpoints http://eventfs-node1:50051,http://eventfs-node2:50051,http://eventfs-node3:50051 \
  append docker/smoke --event-type DockerSmoke \
  --data '{"source":"compose"}' --expected-version nostream

docker compose exec client esctl \
  --endpoints http://eventfs-node1:50051,http://eventfs-node2:50051,http://eventfs-node3:50051 \
  read docker/smoke --from-version 0 --max-count 10

docker compose exec client mountpoint /mnt/eventfs
```

在 macOS 上，FUSE 挂载只存在于 client 容器的 Linux mount namespace，不能直接从宿主
访问。
