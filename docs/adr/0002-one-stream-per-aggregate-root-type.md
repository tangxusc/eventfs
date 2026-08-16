---
status: superseded by ADR-0003
---

# 每种聚合根类型共享一个事件序列

EventFS FUSE 曾选择让同一业务空间内某种聚合根类型的全部实例共享一个事件序列，并以事件中的 `aggregate_id` 区分实例。ADR-0003 引入虚拟分区后替代了这一决定。
