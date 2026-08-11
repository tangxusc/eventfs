# 待办：es-client / es-core 代码审查发现（来自 2026-08-11 审查）

以下 4 项来自代码审查，属已提交的 `0bd7f1c`（es-client ReadAll/Subscribe/GetStreamMeta + leader 重定向重试）引入或暴露的问题。
**当前分支（feat/snapshot-files）不处理**，后续另开分支修复。

---

## 1. es-client `read_all` 反向翻页永不干净终止

- **位置**：`eventstore/es-client/src/client.rs:303`（read_all 游标更新逻辑）；
  服务端 `eventstore/es-server/src/service.rs:415`（`(*p > 0).then(...)` 丢弃 position 0 游标）
- **问题**：反向读 $all（`from_position=u64::MAX`）分片消费到 position 0 时，服务端把该分片从
  `next_positions` 丢弃；客户端「next_positions 非空才更新」保留上一页旧游标 →
  尾页事件被无限重复投递；调用方按文档「以空页为终止」永远等不到空页（死循环）。
  单页边界情形则把空游标透传，触发服务端 invalid_argument（shard_ids 与 from_positions 不能同时为空）
- **复现**：10 条事件 `max_count=4` 反向翻页，第 3 页返回 `[2,1,0]` 且 next_positions=[] →
  第 4 页重读 `[2,1,0]`，无限循环
- **修复方向**：客户端把「某分片 position 已达 0」记为终止状态并从游标中移除；
  服务端反向读返回的游标语义需明确（或返回特殊标记）；补反向翻页 e2e 测试
- **现有覆盖**：e2e 只测正向，反向无覆盖

## 2. subscribe catch-up/live 窗口丢事件

- **位置**：`eventstore/es-server/src/service.rs:497`（subscribe 实现）
- **问题**：历史扫描（read_stream_events / read_all_events）与发送 caught_up 都发生在
  `storage.subscribe_events()` 挂上广播通道**之前**——窗口内提交的事件既不在这段历史里、
  也不会广播给未挂载的订阅者，对该订阅者永久丢失。按已读位置重订阅的调用方将永久跳过空洞
- **复现**：繁忙流上大历史 catch-up 耗时数秒，期间提交的事件无人接收；
  客户端收到 caught_up 后从挂载点开始 live，窗口事件丢失
- **修复方向**：先挂广播通道再扫描历史，或扫描完成后比对「扫描终点 vs 当前 applied」
  补投窗口内事件（等价于把广播通道起点前移到扫描开始前，catch-up 从挂载点续读）
- **现有覆盖**：e2e 只在 caught_up 之后才追加写入，测不出该窗口

## 3. es-client `with_any_node` 吞掉流中途错误

- **位置**：`eventstore/es-client/src/client.rs:255`（read_stream / read_all 的 with_any_node 重试）
- **问题**：流建立后的中途错误被静默换节点、以相同 from_version/from_positions **整页重读**——
  与文档「中途错误原样上抛」矛盾。重读可能命中刚恢复网络的落后 follower，返回陈旧/缺失数据
  且无错误信号，调用方按成功处理。另：append 全节点建连失败时报误导性的 `NotLeader(None)`
  （旧实现返回的 ConnectionFailed 详情被丢弃）
- **复现**：节点 A 流中段报错（部分页已收）→ 换节点 B 重读；B 若落后未 apply 最新事件，
  返回缺最新数据的页，无任何错误提示
- **修复方向**：中途错误上抛而非换节点重读（换节点只应发生在建连阶段）；
  append 建连失败返回带原因的 ConnectionFailed
- **现有覆盖**：e2e 只测正常路径，无中断重试场景

## 4. es-core `LeaderRetryPlan` 预算最后槽位的重定向地址被丢弃

- **位置**：`eventstore/es-core/src/redirect.rs:56`（LeaderRetryPlan::next()）
- **问题**：`next()` 先查 `budget == 0` 再出队——最后一个预算槽位上收到的 leader 重定向地址
  被直接丢弃（redirect_to 只入队不退款）；去重跳过同样烧预算，两个已试节点间来回 bounce
  可耗尽预算而队列里未试端点仍在
- **复现**：单节点选举尾期，前 3 次 Unavailable 无提示（各睡 200ms），第 4 次（最后槽位）
  带 leader_addr → redirect_to 入队后 next() 因 budget==0 直接 None → append 返回
  NotLeader(Some(addr))，已知 leader 地址从未被联系
- **修复方向**：出队优先于预算检查（预算应只限制「已尝试次数」而非「是否还能尝试已知地址」）；
  去重跳过不消耗预算或按「有效尝试」计
- **影响面**：es-client append 与 es-ctl with_leader 共用此策略

---

## 处理建议

- 另开分支（如 `fix/es-client-retry`）逐项修复，每项补 e2e 覆盖
- 第 1、2 项涉及服务端与客户端协议语义，修复时同步更新 docs/esctl.md 与 README 的相关描述
