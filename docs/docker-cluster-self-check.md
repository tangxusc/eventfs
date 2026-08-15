# Docker 三节点集群设计自检

本文记录本地三节点 `eventstored` 与常驻 `esctl` 客户端的方案依据、边界和恢复方式。
实现入口为 `compose.yaml`。

## 需求映射

| 需求 | 设计 |
|---|---|
| 三个服务节点 | `eventfs-node1`、`eventfs-node2`、`eventfs-node3` 并行启动 |
| Debian 运行环境 | `debian:12-slim` 仅安装 Actions 产物中的 `eventstored` 与 `esctl` |
| 使用已编译产物 | 按当前提交查找成功的 `Release` 手动运行，并选择宿主原生 Linux 包 |
| 可选本地代理 | 下载脚本接受 `EVENTFS_PROXY=http://127.0.0.1:7897` |
| 常驻客户端 | `client` 容器通过 Docker DNS 访问三个节点，并在节点健康后启动 |
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

客户端容器没有宿主端口和 Docker socket。节点内部端口也没有发布到宿主，只能由同一
Compose 网络中的容器访问。

## 路由广播反馈环修复

首次容器验收发现旧产物在路由变更后形成
`PushRouteTable → Bootstrap → publish_authoritative → PushRouteTable` 反馈环：控制 shard
快速生成上万条日志，广播超时还会让单节点路由投影长期落后。根因是控制 shard 返回的
权威表即使与本地投影完全一致，`publish_authoritative` 仍再次广播。

修复后该方法返回本次是否实际发布：相同表直接返回 `false`，只有内容变化才持久化并
广播。单元回归测试覆盖首次发布为 `true`、重复表为 `false`；最终容器 e2e 还必须检查
三个节点的 `routes.json` 一致，并分别通过单一端点读回同一事件。

## 验证与失败恢复

启动验收依次检查：Compose 配置解析、下载摘要、ARM64 ELF、三个节点健康、成员列表、
一次 append/read 闭环，以及重启一个节点后的恢复。健康检查只证明节点 gRPC 与本地
Raft shard 可查询；最终以三端点 `status`、`member list` 和数据闭环为准。

启动失败时先执行 `docker compose logs eventfs-node1 eventfs-node2 eventfs-node3`。
配置不一致或旧容器残留时，执行 `docker compose down` 后重建；本方案没有持久化数据，
该操作会清空本地集群。下载损坏时移除 `.docker-artifacts` 后重新运行下载脚本。回滚
仓库改动不会停止已运行容器；需显式执行 `docker compose down`。

## 已知边界

- 默认产物由当前提交和分支自动定位；Actions artifact 到期后需要为同一提交重新运行
  workflow，或同时指定其他运行的 `EVENTFS_RUN_ID` 与 `EVENTFS_VERSION`。
- 下载脚本仅支持 ARM64 与 x86_64 Docker 宿主，并选择对应的 GNU/Linux 原生包。
- 这是临时本地开发集群，没有 TLS、跨主机网络、备份、监控和资源配额。
- 本轮不改 Rust 代码，因此不新增单元覆盖率；运行中的三容器闭环作为环境型 e2e。
