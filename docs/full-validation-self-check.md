# 全功能验收闭环自检

本文固化 EventFS 从开发到本地二进制验收的完整闭环。验收分支为
`codex/full-validation`，基线为 `codex/docker-three-node` 的 `42c5889`。只有本文全部
门禁均有当前提交的直接证据，才允许合入 `main`。

## 范围与边界

本轮验证仓库中已经实现的 Rust workspace、`eventstored`、`esctl`、AggregateStore、
持久订阅、快照、在线迁移、三节点 Raft、Release Action 和 Linux FUSE。路线图中明确
未实现的 Projection、多语言 SDK、自动再平衡、Prometheus 与性能扩展项不伪装成缺陷，
也不纳入完成条件。

采用穷尽式方案，而不是只运行默认测试和冒烟。后者适合日常提交，但会遗漏真实进程
选举、重启追平、CLI 重定向及 `/dev/fuse` 挂载路径。穷尽式方案成本更高，却与发布前
“全部功能”的风险范围一致。

## 权威门禁

| 门禁 | 必须取得的证据 |
|---|---|
| 静态质量 | `cargo fmt --check`、`git diff --check`、`actionlint`、Compose 配置解析通过 |
| 默认套件 | Rust 1.88、`Cargo.lock` 下 `cargo test --workspace --locked` 全部通过 |
| 环境型测试 | `es-server` 14 项、`esctl` 2 项真实多进程测试和 1 项真 FUSE 测试全部通过 |
| 覆盖率 | 使用彼此兼容的 rustc、LLVM profile 与 `cargo-llvm-cov`；行和分支均不低于 80% |
| 三节点运行 | 三节点健康、成员和路由收敛、数据复制、leader 故障恢复与重启追平有直接证据 |
| 数据与管理面 | 普通流、跨分片读取、AggregateStore、持久订阅、快照和在线迁移完成闭环 |
| FUSE | 真实挂载、事件 append/read、状态首次创建与覆盖、`esctl` 交叉读回全部通过 |
| 权限 | client 仅有 `/dev/fuse`、`SYS_ADMIN`、`apparmor=unconfined`，不使用 privileged |
| Release Action | 同一提交的四个原生 runner 测试、release 编译、架构冒烟和资产汇总全部成功 |
| 下载产物 | 按 run ID 下载，四包 SHA256 校验成功，只保留宿主原生 Linux 包用于镜像构建 |
| 最终交付 | 新产物重建集群并复验，分支 fast-forward 合入 `main` 后推送；不创建 tag |

测试通过数只能来自本轮命令输出，不能照抄 README 的历史数字。Linux 条件编译测试必须
由 Linux runner 或 Linux 容器执行；macOS 上仅编译成功不能替代真 FUSE 证据。

## 缺陷闭环

每个失败按“观察错误 -> 提出根因假设 -> 最小复现 -> 修复 -> 精准回归 -> 扩大回归”
处理。修复同步更新测试、README 或设计文档。代码提交后触发 `workflow_dispatch`；四平台
成功后下载该 run 的统一资产并校验，再用新二进制重建本地环境。任何本地二进制失败都
回到开发步骤，不能用旧产物继续宣称完成。

核实结果：多进程 `leader_killed_re_elect_data_intact` 已改用真实事件可见性门禁并通过；
`es-server` 真实多进程测试计数已从过期的 12 项修正为源码实际的 14 项。

## 当前本机证据

截至 2026-08-16，本轮实现提交前已取得以下本机证据；这些结果不能替代同一提交的
Action 与正式产物复验门禁：

- Rust 1.88 默认 workspace：634 项通过，16 项按平台设计忽略。
- 真实多进程：`es-server` 14/14、`esctl` 2/2 通过；历史 leader 故障用例已复现为通过。
- 分布式 `$all`：接入节点只承载 shard 1、远端只承载 shard 0 时，正向/反向分页、
  全局 limit、逐分片顺序与消费水位全部通过。
- 覆盖率：nightly LLVM 23 与 profile 版本匹配；行 `90.98%`、分支 `81.40%`、函数
  `82.29%`、区域 `89.37%`。
- 静态检查：`cargo fmt --check`、`git diff --check` 与 `actionlint 1.7.12` 通过；
  workflow 只含 `workflow_dispatch`，全局权限为 `contents: read`。
- 当前源码 Linux ARM64 包：三个 ARM64 ELF 的 `--help` 冒烟通过；Compose 6 shard
  RF=2 集群和最小权限 FUSE client 全部 healthy。
- Docker 数据与管理面：普通流、node1 单入口跨全部 shard 的 `$all`、在线迁移、持久
  订阅 Fetch/Ack、AggregateStore 状态 CAS 与 group Fetch/Ack 均通过。
- Docker FUSE/恢复：事件 append/fsync、follow、状态首次创建与覆盖、group Ack、
  `esctl` 交叉读回均通过；node1 重启后流、状态和挂载保持可用。

## 磁盘约束

- 所有本地 Cargo、ignored 测试和覆盖率使用独立 `CARGO_TARGET_DIR`，shell `trap` 在
  成功或失败后删除；不在 worktree 留下 `target/`。
- 覆盖率 raw profile、HTML、临时数据目录和下载展开目录验收后立即删除，只保留最终
  汇总数字与命令证据。
- Action 下载完成 SHA256 校验后，仅保留 `.docker-artifacts/eventfs-linux-native.tar.gz`；
  四个平台重复包和校验临时目录立即删除。
- Docker 只清理本项目的临时容器、镜像和构建层，不删除无法确认归属的用户镜像或
  volume。每个大步骤前后检查 `df` 与 `docker system df`。
- 当前磁盘可用空间基线为 62 GiB。若低于 20 GiB，先停止新编译并清理已确认归属的
  中间产物；不得通过保留多个 target 目录换取测试速度。

## 恢复与回滚

每个测试套件使用独立临时目录，失败不会污染下一个套件。三节点集群没有持久化 volume，
允许 `docker compose down` 后从已校验产物重建。Action 失败保留 run 日志用于定位，修复
后创建新提交和新 run，不覆盖失败证据。合入 `main` 前可直接删除验收 worktree 回滚；
合入后使用普通 revert 提交，不改写远程历史。
