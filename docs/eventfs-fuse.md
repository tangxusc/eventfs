# EventFS FUSE 设计

`eventfs-fuse` 是 AggregateStore 的无状态 Linux FUSE3 适配器。它不保存本地业务数据，不
推导虚拟分区或 Shard，也不引入第二套聚合模型。启动时先协商服务端能力；协议版本、256
分区、状态 CAS、显式 settlement 或修改时间语义缺失时拒绝挂载。

## 平台与启动

真实挂载只支持 Linux，需要 FUSE3、`/dev/fuse` 和挂载权限：

```bash
cargo build -p eventfs-fuse --locked
mkdir -p /data/eventfs
./target/debug/eventfs-fuse mount \
  --config ./eventfs-fuse.example.toml \
  /data/eventfs
```

默认前台运行。`--allow-other` 只有在本机信任边界明确且 `/etc/fuse.conf` 允许时使用。
Darwin 可编译并运行 codec、路径、后端和公共契约测试，但没有 `/dev/fuse`，不能执行真实
mount e2e。

## 路径模型

```text
/
└── {business_space}/
    └── {aggregate_type}/
        ├── events.jsonl
        ├── states/
        │   └── {aggregate_id}.json
        └── groups/
            └── {group_name}/
                └── {consumer_id}.jsonl
```

三个固定入口保持为：

```text
/{business_space}/{aggregate_type}/events.jsonl
/{business_space}/{aggregate_type}/states
/{business_space}/{aggregate_type}/groups
```

标识符必须匹配 `[A-Za-z0-9][A-Za-z0-9._-]{0,127}`；`events.jsonl`、`states` 和
`groups` 是保留名。路径拒绝空段、`.`、`..`、尾随 `/` 和错误扩展名。

根目录和业务空间目录由已激活 AggregateType 推导。`mkdir` 只允许创建聚合类型和消费者
组：创建 `/{space}/{type}` 对应 `RegisterAggregateType`，创建
`/{space}/{type}/groups/{name}` 对应 `CreateAggregateGroup`。固定目录不能手工创建。

## events.jsonl

### 写入

每次打开、写入并提交一个完整 JSON 值，对应一条 `AppendAggregateEvent`：

```json
{
  "spec_version": "1.0",
  "aggregate_id": "order-42",
  "event_type": "OrderPlaced",
  "data": {"sku": "A-1", "quantity": 2},
  "metadata": {"trace_id": "t-1"},
  "event_id": "018f5f56-8e1a-7e35-9f01-111111111111",
  "expected_version": {"kind": "no_aggregate"}
}
```

`metadata` 缺省为空对象，`event_id` 缺省时在第一次准备提交时生成并在失败重试中复用。
`expected_version` 支持：

```json
{"kind":"any"}
{"kind":"no_aggregate"}
{"kind":"exists"}
{"kind":"exact","version":7}
```

输入采用严格 schema：未知字段、多个 JSON 值、非法标识符、非对象 metadata、错误版本或
超限都会拒绝。内核必须按连续 offset 写入；`flush`、`fsync` 或 `release` 触发提交，成功
后重复提交不产生第二条事件，失败后可复用同一准备结果重试。

### 读取

只读打开从 Beginning 调用 `FollowAggregateTypeEvents`，输出非 seek JSONL：

```json
{"kind":"event","aggregate_id":"order-42","aggregate_version":0,"event_id":"...","event_type":"OrderPlaced","data":{"sku":"A-1"},"metadata":{}}
{"kind":"caught_up"}
{"kind":"degraded","unavailable_source_count":1,"retrying":true}
{"kind":"recovered"}
```

该文件是类型级持续 feed，不是单个实例历史文件。事件保证同一 `aggregate_id` 内按
`aggregate_version` 有序，不同实例之间无全序。句柄标记为 direct I/O、non-seekable；
offset 必须等于已消费字节数。

## states

`states/{aggregate_id}.json` 正文就是原始 JSON 状态，不包 envelope。只读打开返回当前
正文。写打开时读取当前 revision：

- 文件不存在时提交条件为 `Absent`；
- 文件存在时提交条件为打开时的 `Exact(revision)`；
- 并发修改导致 CAS 冲突，返回 `EAGAIN`，调用方必须重新打开并合并。

状态目录使用服务端分页 token 增量执行 `readdir`，不会一次把全部状态加载到内存。文件
mtime 使用服务端提交时间；新建但尚未写入的句柄使用 Unix epoch。

## groups

创建 `groups/{group_name}` 建立从 Beginning 开始的消费者组。读取
`groups/{group_name}/{consumer_id}.jsonl` 长轮询 delivery：

```json
{"kind":"delivery","delivery_id":"0a0b...","attempt":1,"deadline_ms":1700000000000,"replayed":false,"aggregate_id":"order-42","aggregate_version":0,"event_id":"...","event_type":"OrderPlaced","data":{},"metadata":{}}
```

同一挂载内，同一 `(AggregateType, group_name, consumer_id)` 只允许一个活跃读句柄；重复
打开返回 `EBUSY`。后端会为尚在处理的 delivery 续租，句柄关闭后停止续租。

向同一路径写入 settlement envelope：

```json
{
  "settlements": [
    {"delivery_id":"0a0b...","action":"ack"},
    {"delivery_id":"0c0d...","action":"retry","reason":"temporary"}
  ]
}
```

动作支持 `ack`、`retry`、`park`、`skip`。delivery ID 为十六进制 opaque token；stale
lease 映射 `ESTALE`，wrong consumer 映射 `EACCES`。RPC 成功但任一逐项结果失败时，整个
本次文件提交按对应 errno 报错。

## 缓冲和背压

流式读使用有界内存缓冲。生产速度超过消费速度时后端等待空间；单个 frame 超过缓冲上限
返回 `EFBIG`。客户端停止读取应及时关闭 fd，使远端 gRPC stream、消费者租约和本地任务
释放。

## 错误映射

| 后端错误 | errno |
|---|---|
| 非法参数/offset | `EINVAL` |
| 不存在 | `ENOENT` |
| 已存在 | `EEXIST` |
| OCC/CAS 冲突 | `EAGAIN` |
| payload 超限 | `EFBIG` |
| stale lease | `ESTALE` |
| 权限/消费成员不匹配 | `EACCES` |
| 超时 | `ETIMEDOUT` |
| 节点不可用 | `EHOSTUNREACH` |
| 句柄冲突 | `EBUSY` |
| 服务端能力不满足 | `ENOTSUP` |
| 内部协议错误 | `EIO` |

## 测试

```bash
cargo test -p eventfs-fuse --locked
```

Linux 真实 mount e2e 另外验证 mkdir、事件读写、状态 CAS、组消费、poll 和卸载清理；它不
能由 Darwin 的 mock 后端测试替代。Linux CI 或具备 `/dev/fuse` 的主机必须显式运行该
用例。
