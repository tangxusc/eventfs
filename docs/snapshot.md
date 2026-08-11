# Raft 快照

## 为什么需要

Raft 日志会无限增长。不做快照的后果:

1. **磁盘被吃满** —— 日志只增不减
2. **新节点加入极慢** —— 需重放全部历史日志,恢复时间随集群运行时长线性增长
3. **落后节点无法追赶** —— 若 leader 已 purge 掉它缺的日志段,它永远追不上

快照把「某个时刻的完整状态机」序列化下来,之后即可安全清理该时刻之前的日志。
落后太多的节点直接装快照,而非重放日志。

## 实现

### 存储层接口(`es-storage/src/state_machine.rs`)

实现了 openraft 的四个快照方法:

| 方法 | 作用 | 实现要点 |
|---|---|---|
| `build_snapshot` | 建快照 | 扫本分片状态机区全部 kv,序列化为 `SnapshotPayload`;落盘到 `snapshot_current` key 供后续读取 |
| `get_current_snapshot` | 读当前快照 | 从 `snapshot_current` key 读回 `(meta, data)` |
| `begin_receiving_snapshot` | 准备接收 | 返回空 `Cursor<Vec<u8>>`,openraft 往里写流式数据 |
| `install_snapshot` | 装快照 | **先清空本分片状态机区**,再灌入快照内容;与 applied 状态同事务提交 |

### 关键设计点

**install_snapshot 必须先清空目标**

不清空会残留快照里已不存在的 key(例如已被 purge 的事件),导致数据多出来。
测试 `装快照会清掉目标原有数据` 专门验证这一点。

**快照与 applied 状态同事务提交**

```rust
// 清旧数据 + 灌新数据 + 写 applied 状态,全在一个 surrealkv 事务内
txn.delete(...);  // 旧 key
txn.set(...);     // 快照内容
txn.set(key::sm_applied_state(shard), applied_bytes);
txn.commit().await?;
```

若分开提交,中途崩溃会留下「数据是新的但 applied 是旧的」的不一致状态,
重启后 openraft 会从错误位置重放。

**snapshot_id 不依赖墙上时钟**

```rust
let snapshot_id = match last_applied {
    Some(l) => format!("{}-{}-{}", shard, l.leader_id, l.index),
    None => format!("{shard}-empty"),
};
```

用 `(shard, leader_id, index)` 拼接而非时间戳:确定性回放时时钟不可用,
且同一状态必须产生同一 id。

### 生产配置(`es-server/src/server.rs`)

```rust
openraft::Config {
    // 每 5000 条日志建一次快照
    snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5000),
    // 快照后只保留 1000 条日志,其余 purge
    max_in_snapshot_log_to_keep: 1000,
    ..Default::default()
}
```

**参数权衡**:
- `LogsSinceLast` 太小 → 频繁建快照,浪费 IO
- 太大 → 日志堆积,落后节点追赶慢
- `max_in_snapshot_log_to_keep` 决定「多落后的节点还能靠日志追赶」;
  超出这个窗口就必须传快照(更慢但更可靠)

## 测试覆盖

### 存储层往返(`es-storage/src/tests/state_machine_test.rs`)

| 测试 | 验证内容 |
|---|---|
| `快照往返后数据一致` | build → get_current → install 到空存储,5 条事件与 applied 状态完整 |
| `装快照会清掉目标原有数据` | 目标原有的流在装快照后必须消失,不能残留 |

### 集群场景(`es-raft/tests/partition_test.rs`)

用进程内集群,配 `snapshot_after_logs: Some(5)` + `keep_logs_after_snapshot: 2`,
让快照与日志清理频繁发生。

| 测试 | 验证内容 |
|---|---|
| `落后节点通过快照追赶而非重放日志` | 隔离一个节点,期间写入 30 条(触发多轮快照与 purge);恢复后它缺的日志大部分已被清理,只能靠快照追赶。断言:追平、31 条事件完整、版本连续无空洞、三节点一致 |
| `快照后日志被清理但数据完整` | 写 20 条触发多轮快照;三节点数据均完整且版本连续;快照后仍能继续追加 |

**为何用进程内集群测**:需要精确控制日志量与节点隔离时机。
多进程下难以确定快照是否真的触发了。

## 已知限制

- **快照是全量的**:每次 `build_snapshot` 序列化整个分片状态机,
  数据量大时耗时明显。openraft 不支持增量快照。
- **快照存在同一个 tree 里**:与业务数据共用 surrealkv,
  没有独立的快照文件目录。好处是事务原子性有保证,
  代价是快照占用与业务数据同一份存储空间。
- **无快照压缩**:序列化用 serde_json,未压缩。大状态机的快照体积偏大。
- **单快照**:只保留最新一个快照,无历史快照可回滚。

## 后续改进

- [ ] 快照压缩(zstd / lz4)减小体积与传输量
- [ ] 快照存独立文件,与业务数据分离
- [ ] 分块传输大快照,避免一次性载入内存
- [ ] 保留多个历史快照,支持时间点恢复
