# Docker 三节点集群设计自检

本文记录本地三节点 `eventstored` 与常驻 `esctl` 客户端的方案依据、边界和恢复方式。
实现入口为 `compose.yaml`。

## 需求映射

| 需求 | 设计 |
|---|---|
| 三个服务节点 | `eventfs-node1`、`eventfs-node2`、`eventfs-node3` 并行启动 |
| Debian 运行环境 | Debian 12 server 安装 `eventstored`/`esctl`；Debian 13 client 安装 `eventfs-fuse`/`esctl`/`fuse3` |
| 使用已编译产物 | 按当前提交查找成功的 `Release` 手动运行，并选择宿主原生 Linux 包 |
| 可选本地代理 | 下载脚本接受 `EVENTFS_PROXY=http://127.0.0.1:7897` |
| FUSE 客户端 | `client` 通过 Docker DNS 访问三个节点，并把 EventFS 挂载到容器内 `/mnt/eventfs` |
| 无持久化 volume | 数据只写入容器可写层；`docker compose down` 删除容器后数据丢失 |
| 宿主访问 | 公共端口映射为 `50051`、`50052`、`50053` |
| 内部 RPC 隔离 | `51051` 只在 Compose 网络内使用，不映射宿主端口 |

## 方案选择

采用 GitHub Actions 原生 Linux release 产物，而不是在本机 Docker 中重新编译。
该产物已经通过 workspace 默认测试、`--help` 冒烟和 ELF 架构检查，下载后再按
`SHA256SUMS` 校验。运行镜像因此只需要 Debian glibc 环境，构建耗时和依赖面更小。

替代方案是在已有 `rust:1.88-bookworm` 镜像中按 `Cargo.lock` 编译。它不依赖 Actions
artifact 的保留期和 GitHub 登录，但需要下载 Cargo 依赖与 protoc，耗时更长，也会
产生较大的编译缓存。本地 release artifact 过期或不可下载时才回退到该方案。

## 集群不变量

- 三份配置的 peers、内部地址和 placement 完全一致，仅本节点 `id` 不同。
- 容器内公共监听统一为 `0.0.0.0:50051`，通过 Compose 端口映射区分宿主入口。
- 每个 shard 恰有两个投票成员；六个 shard 按三节点环形分布。
- peers 使用 Compose 服务名，不能使用容器内 `127.0.0.1` 指向其他节点。
- 公共 API 使用明文 HTTP，仅用于本机开发；不应将该配置直接用于不可信网络。

## 权限与数据边界

镜像内进程以 UID/GID `65532` 运行。配置以只读 bind mount 注入；节点数据位于各自
容器的 `/var/lib/eventfs` 可写层，没有 named volume。`docker compose restart` 保留
容器可写层，`docker compose down` 删除容器和数据，镜像与下载缓存不受影响。

客户端容器没有宿主端口和 Docker socket。它以 FUSE 前台进程作为容器主进程，映射
宿主 Docker VM 的 `/dev/fuse`，丢弃全部默认 capabilities 后只加入 `SYS_ADMIN`，并以
`apparmor=unconfined` 允许 mount syscall；不使用权限范围更大的 `privileged`。客户端
配置关闭 `allow_other`，挂载仅对容器内 root 可见。节点内部端口没有发布到宿主，只能
由同一 Compose 网络中的容器访问。

server 与 client 使用 Dockerfile 的独立 target，三个服务节点不会安装 `fuse3` 或携带
`eventfs-fuse`。`/mnt/eventfs` 位于 client 的 mount namespace 和可写层，不绑定到
macOS，也不使用 named volume；删除或重建 client 后挂载随之消失。

Linux Action 产物由 Ubuntu 24.04 原生 runner 编译，其中 `eventfs-fuse` 引用了
`GLIBC_2.39`；Debian 12 的 glibc 2.36 无法加载。client 因此使用 Debian 13，server
继续使用已验证可运行 `eventstored` 的 Debian 12。替代方案是在 Debian 12 内重新编译
FUSE 二进制，但会引入 Rust/protoc 构建依赖并偏离复用 Action 产物的既定方案。

## 路由广播反馈环修复

首次容器验收发现旧产物在路由变更后形成
`PushRouteTable → Bootstrap → publish_authoritative → PushRouteTable` 反馈环：控制 shard
快速生成上万条日志，广播超时还会让单节点路由投影长期落后。根因是控制 shard 返回的
权威表即使与本地投影完全一致，`publish_authoritative` 仍再次广播。

修复后该方法返回本次是否实际发布：相同表直接返回 `false`，只有内容变化才持久化并
广播。单元回归测试覆盖首次发布为 `true`、重复表为 `false`；最终容器 e2e 还必须检查
三个节点的 `routes.json` 一致，并从目标 shard 的两个副本读回同一事件。

单流读取仍由客户端依次尝试 `--endpoints` 显式提供的地址。跨分片 `ReadAll` 不同：
接入节点在本地读取所承载 shard，并把其余单 shard 子请求代理到对应 leader，最后复用
HLC 归并与逐 shard 消费水位生成统一响应。代理请求只含一个 shard，目标 leader 必然
本地终止，不会形成递归。这样 RF=2 布局下即使没有节点承载全部 6 个 shard，单一入口
也能完成 `$all`；客户端仍建议配置三个端点，以覆盖入口节点本身不可达的情况。

## FUSE 首次状态创建修复

真实挂载验收发现，Linux 内核在 `CREATE` 返回写句柄后、首个 `WRITE` 前会先发送
`GETATTR`。此时新状态尚未提交到服务端，旧实现把服务端的 NotFound 直接转换为
`ENOENT`，内核随即 `RELEASE` 句柄，因此应用的首次写入不会到达 FUSE 进程。

修复只对同一状态存在 revision 为空的活动写句柄这一瞬态返回大小为 0 的属性；普通
状态仍从服务端读取属性，避免掩盖真实的 NotFound。单元测试
`new_state_getattr_survives_until_first_write` 覆盖无句柄时仍返回 `ENOENT`、创建句柄后
返回空状态属性。最终容器 e2e 必须直接通过 FUSE 首次创建状态并从 `esctl` 读回，不能
预先用 `esctl` 创建状态，否则无法覆盖该内核调用顺序。

## 验证与失败恢复

启动验收依次检查：Compose 配置解析、下载摘要、ARM64 ELF、三个节点健康、成员列表、
一次 append/read 闭环、真实 FUSE 挂载读写，以及重启一个节点后的恢复。server 健康
检查只证明节点 gRPC 与本地 Raft shard 可查询；client 健康检查使用 `mountpoint` 验证
`/mnt/eventfs` 已成为真实 FUSE 挂载。最终以三端点 `status`、`member list`、数据闭环
和 FUSE 文件契约为准。

2026-08-16 当前源码 Linux ARM64 临时包验收通过：三个二进制均为 ARM64 ELF 且
`--help` 可执行；三个节点与 FUSE client 全部 healthy。6 个 shard 均为两 voter，
仅连接不承载 shard 4/5 的 node1 仍能 `ReadAll` 汇总 6 个 shard。在线迁移从 shard 0
切换到 shard 4 后两条事件完整，持久订阅与 AggregateStore 消费者组均完成 Fetch/Ack，
状态 CAS 从 revision 0 更新到 1。真 FUSE 完成事件写入/fsync、event/caught-up 读取、
状态首次创建/覆盖和消费者组结算；node1 重启后流、状态与挂载均保持可用。

启动失败时先执行 `docker compose logs eventfs-node1 eventfs-node2 eventfs-node3`。
配置不一致或旧容器残留时，执行 `docker compose down` 后重建；本方案没有持久化数据，
该操作会清空本地集群。下载损坏时移除 `.docker-artifacts` 后重新运行下载脚本。回滚
仓库改动不会停止已运行容器；需显式执行 `docker compose down`。

## 已知边界

- 默认产物由当前提交和分支自动定位；Actions artifact 到期后需要为同一提交重新运行
  workflow，或同时指定其他运行的 `EVENTFS_RUN_ID` 与 `EVENTFS_VERSION`。
- 下载脚本仅支持 ARM64 与 x86_64 Docker 宿主，并选择对应的 GNU/Linux 原生包。
- Docker daemon 必须提供 `/dev/fuse`；当前已在 OrbStack Linux VM 验证。挂载只在
  client 容器内可见，不能直接从 macOS Finder 访问。
- 这是临时本地开发集群，没有 TLS、跨主机网络、备份、监控和资源配额。
- 路由广播修复由 `publish_authoritative_skips_unchanged_table` 单元测试覆盖；运行中的
  三容器闭环作为环境型 e2e，不纳入默认覆盖率统计。
