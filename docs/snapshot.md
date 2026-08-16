# Raft Snapshot

## 语义

Raft Snapshot 是单个 Shard 状态机在某个已提交日志点的备份，用于日志压缩、落后节点安装
和离线恢复。它包含该 Shard 承载的 AggregateType catalog、事件、状态文档、消费者组进度
及幂等记录；它不是业务状态文档，也不是按聚合实例导出的业务快照。

Aggregate-only 版本的 snapshot 格式不兼容旧数据、旧 Raft 日志或旧 snapshot。恢复时
必须使用同一架构版本生成的文件，不提供格式迁移。

## 配置

```toml
[snapshot]
compression = "zstd"
keep = 3
max_chunk_size = 3145728
# dir = "./data/node1/snapshots"
```

| 字段 | 说明 |
|---|---|
| `compression` | `zstd`、`lz4` 或 `none`；算法记录在文件头 |
| `keep` | 每 Shard 保留数量，包含最新文件 |
| `max_chunk_size` | 安装快照分块；默认 3 MiB，配置上限 6 MiB |
| `dir` | 缺省 `{data_dir}/snapshots` |

压缩算法属于文件元数据，节点可以使用不同默认算法读取已有 snapshot。chunk 必须小于 gRPC
消息上限并留出协议余量，不能依赖 transport 自动拆分超大块。

## 文件与一致性

snapshot 先写临时文件，完成头部、payload 和校验后再原子发布。文件身份包含 Shard 和
Raft snapshot meta；安装时验证 magic、版本、压缩算法、长度、校验、Shard 归属和尾随字节。

创建 snapshot 的状态读取点和 Raft meta 必须一致。安装过程不得部分替换现有状态机；只有
完整校验和导入成功后才切换可见存储。保留清理由 `keep` 控制，不删除当前正在引用的文件。

## 列表

```bash
esctl snapshot list /srv/eventfs/node1
esctl snapshot list /srv/eventfs/node1 --snapshot-dir /srv/snapshots/node1
```

输出包括 Shard、snapshot ID、最后日志点、压缩算法、文件大小和路径。该命令离线扫描文件，
不连接 gRPC 节点。

## 离线恢复

```bash
esctl snapshot restore \
  /srv/eventfs/node1 \
  /backup/eventfs/shard-0.snapshot \
  --yes
```

恢复步骤：

1. 停止使用目标 `data_dir` 的 `eventstored`，确认没有共享该目录的进程。
2. 确认 snapshot 来自 Aggregate-only 同版本、目标节点计划承载对应 Shard。
3. 备份当前目录；恢复是破坏性离线操作。
4. 执行 restore，并检查工具完成文件校验和状态导入。
5. 启动节点，检查 `esctl status`、AggregateType catalog、事件追加、状态和消费者组。

恢复单个副本后仍由 Raft membership 决定其角色。若 quorum 中其他节点拥有更新日志，恢复
节点会继续追平；不能把离线恢复当作绕过 Raft 提交的业务写入工具。

## 灾备验证

定期在隔离目录演练：

```bash
esctl snapshot list /srv/eventfs/node1
esctl snapshot restore /tmp/eventfs-restore-test <SNAPSHOT> --yes
```

恢复验证至少覆盖：AggregateType 数量和状态、多个聚合实例的下一次 OCC、状态 revision
CAS、消费者组 revision/epoch、已提交进度和幂等重试。测试完成后删除隔离目录，绝不能把
生产目录作为演练目标。

## 故障处理

| 错误 | 处理 |
|---|---|
| magic/version 不支持 | 文件不是当前 Aggregate-only 格式，拒绝恢复 |
| checksum/长度错误 | 文件损坏，换用另一保留副本 |
| Shard 不匹配 | 选择正确目标或 snapshot，禁止强制导入 |
| 解压失败 | 检查文件完整性和构建是否包含对应 codec |
| 目录被占用 | 停止进程后重新执行 |
| 启动后落后 | 检查 membership、网络和 leader，等待 Raft 追平 |
