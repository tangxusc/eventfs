# esctl 手册

`esctl` 管理 AggregateStore、Raft 成员和离线快照。全局参数必须位于子命令之前。

```text
esctl [GLOBAL_OPTIONS] <COMMAND>
```

## 全局参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--endpoints <A,B>` | `http://127.0.0.1:50051` | 候选节点，逗号分隔 |
| `--dial-timeout <S>` | 5 | 建连超时 |
| `--timeout <S>` | 10 | 单次 RPC 超时，0 表示不限时 |
| `--cacert <PEM>` | 无 | HTTPS CA，和跳过验证互斥 |
| `--insecure-skip-tls-verify` | false | 跳过 HTTPS 校验 |
| `-w, --write-out <FORMAT>` | `simple` | `simple` / `table` / `json` |
| `--shards <N>` | 自动探测 | 集群 Shard 总数 |

## 命令树

```text
esctl init
esctl member add|remove|list
esctl status
esctl snapshot list|restore
esctl aggregate capabilities
esctl aggregate type register|list|get
esctl aggregate append
esctl aggregate follow
esctl aggregate state list|get|put
esctl aggregate group create|update|delete|list|fetch|settle
esctl aggregate status
esctl aggregate partitions
```

不存在通用 append/read/watch/meta、persistent、route 或 migrate 命令。

## 聚合类型

```bash
esctl aggregate capabilities
esctl aggregate type register orders order [--operation-id UUID]
esctl aggregate type list
esctl aggregate type get orders order
esctl aggregate status
esctl aggregate partitions orders order
```

`register` 等待 256 个虚拟分区激活后返回。自动生成的 operation UUID 会显示在输出中；
若命令结果未知，手工重试必须通过 `--operation-id` 复用原值。

`partitions` 是运维诊断接口，会显示内部 Shard placement 和 generation。业务客户端不能
缓存这些值做路由。

## 追加事件

```bash
esctl aggregate append <BUSINESS_SPACE> <AGGREGATE_TYPE> <AGGREGATE_ID> \
  --event-type <TYPE> \
  (--data <JSON> | --data-file <PATH>) \
  [--metadata <JSON>] \
  [--event-id <UUID>] \
  [--expected-version any|no-aggregate|exists|N]
```

示例：

```bash
esctl aggregate append orders order order-42 \
  --event-type OrderPlaced \
  --data '{"sku":"A-1","quantity":2}' \
  --expected-version no-aggregate
```

成功输出新 `aggregate_version`。同一 event ID 和完整请求可安全重试；复用 ID 但改变事件
类型、data、metadata、聚合身份或期望版本会返回幂等冲突。

## 跟随类型级事件

```bash
esctl aggregate follow orders order
esctl aggregate follow orders order --now
esctl aggregate follow orders order --cursor <HEX>
esctl aggregate follow orders order --once
```

默认从 Beginning 开始。`--now` 从连接时各分区 head 开始；`--cursor` 使用前次 frame 输出
的十六进制 opaque cursor。`--once` 在收到 `caught_up` 后退出。

输出事件包含 `aggregate_id`、`aggregate_version`、event ID、类型、data、metadata、HLC 和
cursor。不同实例之间的输出顺序不是全序。出现 `degraded` 时不应丢弃最后 cursor；恢复后
会出现 `recovered`。

## 状态文档

```bash
esctl aggregate state list orders order [--page-size 100] [--page-token HEX]
esctl aggregate state get orders order order-42
esctl aggregate state put orders order order-42 \
  (--data <JSON> | --data-file <PATH>) \
  [--expected-revision absent|N]
```

首次创建使用 `absent`，覆盖使用当前 revision。成功返回新 revision 和服务端修改时间。
列表 token 只允许原样传回，不能跨聚合类型使用。

## 消费者组

创建和管理：

```bash
esctl aggregate group create orders order projector [--now] [SETTINGS]
esctl aggregate group update orders order projector \
  --expected-revision 1 [--reset-beginning|--reset-now] [SETTINGS]
esctl aggregate group delete orders order projector --expected-revision 2
esctl aggregate group list orders order
```

可选设置：

```text
--max-unacked-per-consumer N
--max-unacked-per-group N
--ack-timeout-ms N
--max-retries N
--retry-min-ms N
--retry-max-ms N
```

消费与结算：

```bash
esctl aggregate group fetch orders order projector \
  --consumer worker-1 --max-events 100 --max-bytes 4194304 --wait-ms 15000

esctl aggregate group settle orders order projector \
  --consumer worker-1 --delivery <HEX> --action ack
```

`--action` 可为 `ack`、`retry`、`park`、`skip`；Retry/Park 可加 `--reason`。delivery token
是不透明且有租约的，必须由 Fetch 使用的同一 consumer 结算。CLI 暂不暴露 renew，长期
处理应使用 Rust SDK 或 protobuf API。

## 集群管理

手工初始化：

```bash
esctl init --shard 0 \
  --member 1@node1:50051 --member 2@node2:50052 --member 3@node3:50053 --yes
esctl --shards 8 init --all-shards --member 1@node1:50051 --yes
```

成员与状态：

```bash
esctl member add --shard 0 --member 4@node4:50054 --promote
esctl member remove --shard 0 --node-id 2
esctl member list
esctl status
```

`member add` 先添加 learner，再按参数提升。成员变更使用当前 voter 集合做 CAS；并发冲突
时命令重读后再决定是否重试。

## 快照

```bash
esctl snapshot list <DATA_DIR> [--snapshot-dir PATH]
esctl snapshot restore <DATA_DIR> <SNAPSHOT_FILE> [--snapshot-dir PATH] [--yes]
```

快照命令直接访问本地文件，restore 必须停机。详见 [snapshot.md](snapshot.md)。

## 输出、重试与退出

- `simple` 面向交互，`table` 面向检查，`json` 面向自动化。
- 官方客户端会尝试 leader hint 和候选端点；Append 依赖 event ID 幂等，catalog 写依赖
  operation ID 幂等。
- OCC/CAS、非法参数和幂等冲突不会自动改写请求重试。
- 非零退出表示参数、连接或服务端错误；脚本应解析 JSON 输出而不是人类文本。
