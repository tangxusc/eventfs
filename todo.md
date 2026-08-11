# 待办：es-client / es-core 代码审查发现（来自 2026-08-11 审查）

以下 4 项来自代码审查，属已提交的 `0bd7f1c`（es-client ReadAll/Subscribe/GetStreamMeta + leader 重定向重试）引入或暴露的问题。

**处理状态：全部已关闭**（`0aed6d4` 修复 2、3；`fix/es-retry-cursor` 分支修复 1、4 并补齐 2 的测试覆盖）。

---

## 1. es-client `read_all` 反向翻页永不干净终止 —— ✅ 已修复（fix/es-retry-cursor）

- **位置**：`eventstore/es-server/src/service.rs` read_all（游标生成）；
  客户端 `eventstore/es-client/src/client.rs` read_all（游标更新）
- **问题**：反向读 $all（`from_position=u64::MAX`）分片消费到 position 0 时，服务端把该分片从
  `next_positions` 丢弃；客户端「next_positions 非空才更新」保留上一页旧游标 →
  尾页事件被无限重复投递；调用方按文档「以空页为终止」永远等不到空页（死循环）。
  单页边界情形则把空游标透传，触发服务端 invalid_argument（shard_ids 与 from_positions 不能同时为空）
- **修复**：proto `ShardPosition` 新增 `ended` 标记。反向消费到 position 0 的分片返回
  `(shard_id, 0, ended=true)` 而非丢弃；服务端对 ended 分片返回空页且游标不变；
  「消费到 position 1 → 游标 0（未 ended）」时 from=0 仍能读到 position 0（不丢数据）。
  **空页是正反两向统一的终止条件**，客户端/es-ctl 逻辑零改动（原样透传）。
  同步：es-ctl 反向续读提示剔除 ended 分片；client.rs 文档、docs/esctl.md、README 补反向终止语义
- **测试**：es-server e2e +2（`read_all_backward_paging_terminates` /
  `read_all_backward_last_page_cursor_zero_kept`）；es-client e2e +1
  （`read_all_backward_pages_roundtrip`，走完整 SDK 路径）

## 2. subscribe catch-up/live 窗口丢事件 —— ✅ 已修复（`0aed6d4` 修代码，本次补测试）

- **位置**：`eventstore/es-server/src/service.rs` subscribe 实现
- **问题**：历史扫描与发送 caught_up 都发生在 `storage.subscribe_events()` 挂上广播通道**之前**——
  窗口内提交的事件既不在这段历史里、也不会广播给未挂载的订阅者，对该订阅者永久丢失
- **修复**（`0aed6d4`）：先注册广播接收器再扫描历史（Phase 0），用历史尾水位
  （`last_version` / `last_position`）跳过「快照与广播各一份」的重复窗口，不丢不重
- **测试**（本次补齐）：es-client e2e +2（`subscribe_writes_during_catchup_exactly_once` /
  `subscribe_all_writes_during_catchup_exactly_once`，2000 条大历史拉开窗口 + 窗口内写入，
  断言并集恰好一次；连跑 3 次无 flake）

## 3. es-client `with_any_node` 吞掉流中途错误 —— ✅ 已修复（`0aed6d4`）

- **位置**：`eventstore/es-client/src/client.rs`（read_stream / read_all 的 with_any_node 重试）
- **问题**：流建立后的中途错误被静默换节点、以相同 from_version/from_positions **整页重读**；
  append 全节点建连失败时报误导性的 `NotLeader(None)`
- **修复**：with_any_node 双层错误分类（外层节点级 Status 按码轮换 / 内层调用方定性错误直接上抛）；
  append 建连失败保留错误详情为 `RpcFailed`
- **测试**：client_test.rs 3 项（中途错误不换节点 / 永久错误不轮换 / 建连失败区分）

## 4. es-core `LeaderRetryPlan` 预算最后槽位的重定向地址被丢弃 —— ✅ 已修复（fix/es-retry-cursor）

- **位置**：`eventstore/es-core/src/redirect.rs`（LeaderRetryPlan::next()）
- **问题**：`next()` 先查 `budget == 0` 再出队——最后一个预算槽位上收到的 leader 重定向地址
  被直接丢弃（redirect_to 只入队不退款）；去重跳过同样烧预算
- **复现**：单节点选举尾期，前 3 次 Unavailable 无提示，第 4 次（最后槽位）带 leader_addr →
  redirect_to 入队后 next() 因 budget==0 直接 None → append 返回 NotLeader(Some(addr))，
  已知 leader 地址从未被联系
- **修复**：出队优先于预算检查；去重跳过不消耗预算；预算耗尽后仅重定向目标可继续尝试，
  受新增 `redirect_tail`（初始 = 预算）计数兜底——最后槽位收到的地址必被联系，
  集群抖动（A↔B 互指）仍有界。公共 API 不变，es-client / es-ctl 调用方零改动
- **测试**：redirect.rs 单测 +6（最后槽位重定向不丢弃 / 预算 0 后 Normal 目标终止 /
  去重不烧预算 / tail 计数有界 / tail 耗尽后终止 / 混合队列）
- **影响面**：es-client append 与 es-ctl with_leader 共用此策略
