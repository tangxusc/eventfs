# Raft 快照

## 为什么需要

Raft 日志会无限增长。不做快照的后果:

1. **磁盘被吃满** —— 日志只增不减
2. **新节点加入极慢** —— 需重放全部历史日志,恢复时间随集群运行时长线性增长
3. **落后节点无法追赶** —— 若 leader 已 purge 掉它缺的日志段,它永远追不上

快照把「某个时刻的完整状态机」序列化下来,之后即可安全清理该时刻之前的日志。
落后太多的节点直接装快照,而非重放日志。

## 实现

### 快照独立文件（`es-storage/src/snapshot.rs`）

快照存独立文件,与业务数据（surrealkv tree）分离:

```
{data_dir}/
  snapshots/                                     ← 快照根目录（[snapshot].dir 可覆盖）
    snap-{shard:08}-{term:020}-{index:020}.esnap  ← 正式快照
    incoming/{uuid}.tmp                           ← 传输中临时文件（list 只扫顶层）
```

- **文件命名**:固定宽度补零,同分片内字典序 = (term, index) 数值序。
  空快照（last_applied=None）用哨兵 term=0/index=0（真实 term 从 1 起,不冲突）。
- **与 snapshot_id 解耦**:openraft 的 `SnapshotMeta.snapshot_id`（`{shard}-{term}-{index}`）
  格式不变（分块传输的 snapshot_id 匹配依赖它）,文件名只是本地布局约定。
- **build 原子性**:写 `incoming/{uuid}.tmp` → 刷压缩帧尾 → `rename` 原子转正。
- **启动清理**（`restore_applied_state`）:删除旧版 `snapshot_current` key（树内单 key
  格式已废弃,快照是可重建缓存,不迁移）与 incoming 残留临时文件。

### 文件格式（v1）

```
偏移   大小   字段              说明
0      4     magic            固定 b"ESNP"，读侧第一道校验
4      1     version          固定 0x01，读侧拒绝未知版本
5      1     compression tag   0=none 1=zstd 2=lz4
6      2     reserved          0x0000
8      8     shard_id          u64 LE，显式存分片
16     8     meta_len          u64 LE
24     8     payload_len       u64 LE，未压缩 payload 总字节数
32     meta_len  meta          serde_json(SnapshotMeta)，未压缩（esctl list 只读这段）
32+meta_len    payload         压缩流（解压后为记录流）

记录流（解压后）：
  u64 LE entry_count
  ×entry_count: [u64 LE key_len][key][u64 LE val_len][val]
  u64 LE end_marker = 0xFFFF_FFFF_FFFF_FFFF
```

- **头部与 meta 未压缩**:esctl snapshot list 只需读文件头即可列出快照,无需解压 payload。
- **完整性校验链**（无独立 checksum）:magic → version → 压缩帧自身完整（zstd/lz4 帧尾）
  → payload_len 实读字节数比对 → end_marker → 记录数 = entry_count。
- **压缩算法**:`[snapshot] compression = "zstd" | "lz4" | "none"`,默认 zstd。
  文件头记录算法,读取时自动识别——节点间可混用不同算法。
  zstd 用 `zstd` crate（level 3 固定）;lz4 用 `lz4_flex::frame`（流式帧格式）。

### 存储层接口（`es-storage/src/state_machine.rs`）

实现了 openraft 的四个快照方法:

| 方法 | 作用 | 实现要点 |
|---|---|---|
| `build_snapshot` | 建快照 | 扫本分片状态机区全部 kv → 按配置压缩写入临时文件 → rename 转正 → 保留清理（keep 个） |
| `get_current_snapshot` | 读当前快照 | 扫描快照目录,按 (term, index) 数值取最新文件,读文件头 meta |
| `begin_receiving_snapshot` | 准备接收 | 在 incoming/ 创建唯一临时文件,分块到达时逐块落盘 |
| `install_snapshot` | 装快照 | **先清空本分片状态机区**,流式解压逐条灌入,与 applied 状态同事务提交;成功后临时文件转正 + 保留清理 |

**快照数据类型**（`raft_type.rs`）:`SnapshotData = SnapshotFile`（自定义包装
`tokio::fs::File`,携带路径与临时标记）。openraft 内置 Chunked 分块传输
（默认 3MiB/块）直接流式读文件,不再一次性载入内存。

### 关键设计点

**install_snapshot 必须先清空目标**

不清空会残留快照里已不存在的 key(例如已被 purge 的事件),导致数据多出来。
测试 `装快照会清掉目标原有数据` 专门验证这一点。

**快照与 applied 状态同事务提交**

清旧数据 + 灌新数据 + 写 applied 状态,全在一个 surrealkv 事务内。
若分开提交,中途崩溃会留下「数据是新的但 applied 是旧的」的不一致状态。

**snapshot_id 不依赖墙上时钟**

```rust
let snapshot_id = match last_applied {
    Some(l) => format!("{}-{}-{}", shard, l.leader_id.term, l.index),
    None => format!("{shard}-empty"),
};
```

用 `(shard, leader_id, index)` 拼接而非时间戳:确定性回放时时钟不可用,
且同一状态必须产生同一 id。

**传输完成的 shutdown 语义**

tokio 1.53 的 `File` 写有用户态缓冲,`write_all` 返回不代表落盘。
openraft 的 Chunked 在传输完成时调用 `shutdown()` 刷出缓冲;
`install_snapshot` 内再幂等兜底一次,防未来链路变化丢数据。

### 多快照保留（`[snapshot] keep`,默认 3）

build/install 后按 (term, index) 数值排序,删除超出 keep 个的最旧快照。
**字符串排序不可用**（"9" > "10"）,必须解析文件头 meta 的数值比较。

### 时间点恢复（`esctl snapshot restore`）

`esctl snapshot list <data_dir> [--snapshot-dir <DIR>]` 列出历史快照
（分片/term/index/压缩算法/大小,损坏文件标记「损坏」）;
`esctl snapshot restore <data_dir> <snapshot_file> [--snapshot-dir <DIR>] [--yes]`
离线恢复。`--snapshot-dir` 缺省 `{data_dir}/snapshots`;服务端配置了
`[snapshot].dir` 自定义目录时须显式传入,否则 CLI 与服务器的快照视图不一致。

恢复步骤（**先清理、后提交**——任一失败数据未动）:

1. **清理快照目录中该分片的旧文件**（按文件头分片过滤,不碰其它分片;
   损坏文件无法判断分片,保留;源文件本身除外）
2. **清空**该分片的 Raft 日志区与状态机区（**保留 vote**:选举状态与数据时间点无关,
   清掉后单节点重启无法恢复领导,而日志非空又拒绝重新 initialize）
3. 流式解压装入快照内容 + applied 状态,单事务提交
4. `raft_last_purged` / `raft_committed` 写回快照点——重启后 `get_log_state`
   在日志为空时回落 last_purged,日志基线与状态机一致,openraft 不重放不报错
5. 复制恢复的快照为当前快照;源已在规范名位置时跳过复制
   （`fs::copy` 以 O_TRUNC 打开目标,源==目标会截断同一 inode 清空文件）

**防御性兜底**：`get_current_snapshot` 按已应用状态过滤——跳过领先于
last_applied 的快照文件（restore/崩溃残留的「更新」文件与状态机不一致,
不会作为当前快照发给 follower）。

恢复后:单节点以快照点直接恢复领导（vote 保留）;多节点由 leader 复制快照点
之后的日志或新快照。**需集群停机**（LOCK 安全网,在线执行直接拒绝）。

### 生产配置（`es-server/src/server.rs`）

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

**传输上限**:快照分块默认 3MiB + bincode 头,tonic 0.14 默认 4MB 消息上限
恰好能过但无余量。server 显式 `max_decoding_message_size(8MB)`（tonic 0.14
在服务级配置）,消除 `snapshot_max_chunk_size` 调大时 PayloadTooLarge 直接
失败不重试的隐患。

## 测试覆盖

### 存储层（`es-storage/src/tests/`）

| 测试 | 验证内容 |
|---|---|
| `snapshot_roundtrip_consistent` / `snapshot_overwrite_clears_old` | build → get_current → install 全链路（清旧数据语义） |
| `snapshot_roundtrip_all_compressions` | zstd / lz4 / none 三种压缩往返一致 |
| `snapshot_retention_cleans_oldest` / `snapshot_install_respects_retention` | build 与 install 后保留清理（数值排序） |
| `snapshot_startup_cleanup_legacy` | 启动清理旧 key 与残留临时文件 |
| `snapshot_corrupted_latest_skipped` | 损坏快照被跳过,返回仍有效的快照 |
| `snapshot_restore_to_point_in_time` 等 | 离线恢复:清日志、基线写回、重启后从快照点恢复 |
| `snapshot.rs` 内单测 | 三压缩往返、tag 编解码、magic/version/截断/长度不符检测、SnapshotFile 生命周期、命名与排序 |

### 集群场景（`es-raft/tests/partition_test.rs`）

用进程内集群,配 `snapshot_after_logs: Some(5)` + `keep_logs_after_snapshot: 2`,
让快照与日志清理频繁发生。

| 测试 | 验证内容 |
|---|---|
| `lagging_node_snapshot_catchup` | 落后节点通过快照追赶而非重放日志（文件化快照全链路） |
| `multi_chunk_snapshot_transfer` | 64KiB 小块 + 不可压缩大数据:跨多块传输,分块读写与数据完整性 |
| `logs_purged_after_snapshot_data_intact` | 快照后日志被清理但数据完整,快照之上仍能继续追加 |

### esctl 端到端（`es-ctl/tests/snapshot_test.rs`）

真实 esctl 二进制子进程:list 元数据/损坏标记/空目录、restore 时间点恢复与
恢复后继续写、LOCK 占用与损坏文件拒绝。

## 已知限制

- **快照是全量的**:每次 `build_snapshot` 序列化整个分片状态机,
  数据量大时耗时明显。openraft 不支持增量快照。
- **快照分块与 append 批量共用消息上限**（server 8MB）:快照分块默认 3MiB
  有余量,`[snapshot] max_chunk_size` 上限 6MiB 由启动校验保证不触线
  （openraft 0.9.25 对超限快照块直接放弃传输,无拆小路径）。
  append 批量超限由 es-raft 网络层在发送前映射为 openraft `PayloadTooLarge`
  拆小重试（可自愈,复制不永久停滞）;单事件超限（无拆小路径的死角）由
  `[limits] max_event_bytes`（默认 1MiB）在服务端权威拒绝、客户端本地前置
  校验兜底。
- **install 单事务内存 ≈ 快照未压缩体积**:surrealkv 事务的写入全部在内存缓冲
  到 commit（已核源码 `Transaction.write_set`）。快照体积显著大于可用内存时
  不适用;失败时事务原子,旧数据无损。后续方案:多事务 + installing 标记文件。
- **build/install 同步段阻塞 runtime worker**:与状态机扫描同模式,
  大快照会短暂占用 worker 线程;后续可迁 spawn_blocking。
- **无在线恢复 RPC**:恢复走 esctl 离线命令（LOCK 安全网保证停机约束）。

## 配置参考（config.example.toml）

```toml
[snapshot]
compression = "zstd"   # zstd（压缩率高）/ lz4（速度快）/ none
keep = 3               # 保留历史快照数（含最新），默认 3
max_chunk_size = 3145728  # 快照分块字节数，默认 3MiB；上限 6MiB（8MB 消息上限 - 余量）
# dir = "./data/node1/snapshots"   # 缺省 {data_dir}/snapshots

# 请求大小限制（可选，可整体缺省）
# max_event_bytes：单事件 data+metadata 上限（默认 1MiB）。单条日志超限时
#                  openraft 无拆小路径（复制停滞），必须源头拦截。
# max_append_batch_bytes：单次 append 请求上限（默认 7MiB = 8MB 传输上限 - 余量）。
#                  服务端按 proto 编码精确字节数校验。
[limits]
max_event_bytes = 1048576
max_append_batch_bytes = 7340032
```
