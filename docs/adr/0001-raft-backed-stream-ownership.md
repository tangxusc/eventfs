# ADR-0001：使用现有 Raft group 提交流归属

- 状态：已接受
- 日期：2026-08-14

## 背景

现有节点各自修改 `routes.json`，再以整表广播和版本号仲裁收敛。两个节点可同时把未知 Stream 从同一版本分配到不同 Shard，并生成相同的新版本；接收方会忽略同版本表，无法自动裁决。

用户确认的约束：Stream 归属强一致；无法确认时拒绝首次写入；已知归属保留本地快路径；允许修改内部协议；旧 `routes.json` 必须可读；首次归属可多一次节点间通信。

## 决策

1. 首次部署以配置中编号最小的 Shard 作为控制 Shard，并将 ID 持久化到 `{data_dir}/ownership-control.json`；后续重启不得因加入更小编号 Shard 改变权威。
2. `routes.json` 降为本地兼容投影，继续写出原有 `version`、`streams`、`shard_stream_counts` 字段。
3. 使用 `StreamOwnership` deep module；外部 interface 为 `for_append`、`known`、`change`。
4. 首次归属、迁移切换、旧文件导入和可分配 Shard 变更都必须通过归属权威提交。
5. Append 使用字段私有的 `AppendTarget` 携带 Shard 与归属代次；数据 Shard 状态机使用 fencing 拒绝旧代次写入。
6. 控制 Shard 无 leader、无 quorum 或结果无法确认时返回可重试错误，禁止回退到本地分配。
7. 内部协议与 Raft 日志格式改变后不支持新旧服务器混跑；升级需一次性停止旧版本并启动新版本。公开客户端协议保持兼容。
8. `PushRouteTable` 仅是刷新通知，接收方必须向控制 Shard 读取权威投影；recount 不推进归属 revision。
9. `CommitOwnership` 与 `InstallOwnershipFence` 只注册到内部 listener；多节点配置必须提供内部地址。

## 原因

固定单节点只能串行化进程内请求，节点故障后无法证明已确认归属是否持久化。复用现有 Raft group 可在多数派提交后再确认结果，不引入新的部署单元。

本地快读若没有 fencing，迁移后持有旧投影的节点仍可能写入源 Shard，因此归属代次是强一致迁移语义的一部分。

## 后果

- 未知 Stream 首次写入需要一次归属共识提交和一次 fencing 安装。
- 控制 Shard 不可用时，未知 Stream 创建不可用；已有 Stream 仍可读取本地投影，但旧代次写入会被拒绝。
- 迁移切换可能出现短暂不可写窗口，但不允许双写。
- 手工编辑 `routes.json` 不再直接改写内存；watcher 必须把修改转换为条件归属变更。
- 持久化控制 Shard 不得被放置表移除；加入更小编号 Shard 不会改变控制 Shard。

## 未采用方案

### 固定协调节点

interface 较小，但节点崩溃时无法保证已确认归属被多数派保存，不能满足强一致。

### 可扩展归属账本

以批量 ChangeSet、审计查询和历史分页为 external interface。扩展性高，但当前没有第二个调用需求，interface 成本超过第一阶段收益。
