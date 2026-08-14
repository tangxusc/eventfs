# 跨分片 Stream 聚合订阅

## 公共模型

客户端订阅的对象始终是 stream，而不是 shard。`Subscribe` 支持两种互斥目标：

- 指定一个或多个 stream；
- `$all`，表示当前集群中所有 stream，并在既有 shard 上动态纳入后续新建且写入的 stream。

客户端请求和公开响应均不携带 shard ID、分片 position 或节点地址。订阅从历史起点开始；断点恢复与消费进度持久化属于后续持久化订阅能力。

## 服务端实现

接入节点根据路由表把指定 stream 按内部 shard 分组；`$all` 则从放置表取得全部当前 shard。聚合器只在本机是 Raft leader 时直接使用本地 catch-up 到 live 逻辑；本机为 follower 和远程 shard 均经 `InternalSubscription` RPC 转发到 leader，避免从落后副本读取后错误宣布追平。

`InternalSubscription` 仅在每个节点的 `node.internal_listen_addr` 专用监听端口注册，绝不注册到 `node.listen_addr` 公共端口。各节点必须在 `node.peers` 中为其他节点配置 `internal_addr`，并由防火墙或网络策略限制该端口仅能被集群节点访问。缺少内部地址或内部来源不可用时，公共订阅只发送不含拓扑细节的 `degraded`。

聚合器转发健康子订阅的事件，保证每个 stream 内 version 严格递增，不承诺跨 stream 顺序。全部健康子订阅发送 caught-up 后，公共流发送一次 `caught_up`。任一来源无法建立、断开或落后时，公共流发送不含内部细节的 `degraded`，并持续转发其余健康来源；不自动重连。

## CLI 与 SDK

`esctl watch` 使用可重复的 `--stream <ID>` 或 `--all`，二者互斥；删除 `--shard`、`--from-exclusive` 与 `--from-start`。`--once` 在收到 `caught_up` 后结束；只要出现 `degraded`，即使健康来源均已追平也以非零退出。

SDK 的 `SubscribeTarget` 改为 `Streams(Vec<String>)` 和 `All`，订阅方法不再接收位置或 shard 参数。

## 验证矩阵

- 单节点与跨 shard 的指定 stream：历史与实时事件均按公开 `(stream_id, version)` 投递；
- `$all`：覆盖已有 stream，并在订阅建立后纳入既有 shard 上新建且写入的 stream；
- 双节点：接入节点通过 `InternalSubscription` 聚合远程 shard，客户端不感知该 RPC；`$all` 在本地与远程来源均追平后只发送一次公开 `caught_up`，两侧实时事件持续投递；
- 降级：内部来源不可用只发送无拓扑细节的 `degraded`；CLI `watch --once` 收到它立即以非零退出；
- 配置缩容：保留本地既有 shard 和数据，只收紧后续新 stream 的分配范围；
- catch-up 窗口：历史扫描与广播切换期间写入的事件按 stream version 恰好一次投递。

## 兼容与边界

这是 `Subscribe` 公共协议的破坏性变更。`ReadAll` 的分片游标协议不受影响。`$all` 的 shard 集合在订阅建立时确定，运行期新加 shard 需重新建立订阅。
