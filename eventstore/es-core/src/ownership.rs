//! Stream 强一致归属领域模型。
//!
//! 本 module 只描述可确定重放的归属状态机，不包含网络、文件或 Raft 细节。

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::route::RouteTable;

/// 已提交的 Stream 归属。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Owner {
    shard_id: u64,
    generation: u64,
    revision: u64,
}

impl Owner {
    /// 构造归属记录。
    ///
    /// - `shard_id`：当前承载 Stream 的 Shard。
    /// - `generation`：数据 fencing 使用的归属代次。
    /// - `revision`：本次归属变化对应的全局 revision。
    /// - 返回：不可变的已提交归属值。
    pub fn new(shard_id: u64, generation: u64, revision: u64) -> Self {
        Self {
            shard_id,
            generation,
            revision,
        }
    }

    /// 返回当前承载 Stream 的 Shard ID。
    pub fn shard_id(&self) -> u64 {
        self.shard_id
    }

    /// 返回归属代次；迁移切换时严格递增，用于数据 Shard fencing。
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// 返回该归属最后一次变化的全局 revision。
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// 生成条件变更使用的匹配值。
    ///
    /// 返回值只包含 Shard 与 generation，不暴露全局 revision。
    pub fn match_token(&self) -> OwnerMatch {
        OwnerMatch {
            shard_id: self.shard_id,
            generation: self.generation,
        }
    }
}

/// 条件变更要求匹配的当前归属。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerMatch {
    /// 期望的当前 Shard。
    pub shard_id: u64,
    /// 期望的当前归属代次。
    pub generation: u64,
}

/// 归属状态机命令；所有变更均由控制 Shard 的 Raft 日志串行应用。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnershipCommand {
    /// 首次升级时导入旧 `routes.json`；已有权威状态时只返回当前快照。
    Bootstrap {
        /// 旧格式兼容快照。
        legacy: RouteTable,
        /// 当前配置允许承载新 Stream 的 Shard 集合。
        eligible_shards: BTreeSet<u64>,
    },
    /// 确保 Stream 已有归属；并发调用只会创建一次。
    Ensure {
        /// Stream 名称。
        stream: String,
        /// 权威尚未初始化时使用的初始分配集合。
        eligible_shards: BTreeSet<u64>,
    },
    /// 为迁移准备新代次，但暂不发布新归属。
    PrepareMove {
        /// 重试使用的幂等操作 ID。
        operation_id: Uuid,
        /// 目标 Stream。
        stream: String,
        /// 调用者观察到的当前归属。
        expected: OwnerMatch,
        /// 迁移目标 Shard。
        target_shard: u64,
    },
    /// 数据 Shard fencing 完成后发布准备中的迁移。
    CompleteMove {
        /// PrepareMove 返回或接受的操作 ID。
        operation_id: Uuid,
        /// 目标 Stream。
        stream: String,
    },
    /// 更新后续新 Stream 可使用的 Shard 集合。
    ApplyPlacement {
        /// 新的完整集合。
        eligible_shards: BTreeSet<u64>,
    },
    /// 条件收养尚无权威记录的孤儿 Stream。
    AdoptOrphan {
        /// 孤儿 Stream 名称。
        stream: String,
        /// 已完成复制的目标 Shard。
        target_shard: u64,
    },
}

/// 单条归属命令的可观察结果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OwnershipOutcome {
    /// 返回当前或新建归属。
    Owner {
        /// 已提交归属。
        owner: Owner,
        /// true 表示本命令首次创建该归属。
        created: bool,
    },
    /// 迁移已准备，可安装目标与源 Shard fencing。
    MovePrepared {
        /// 当前已发布归属。
        current: Owner,
        /// 待发布目标 Shard。
        target_shard: u64,
        /// fencing 使用的新代次。
        generation: u64,
        /// 规范化后的操作 ID。
        operation_id: Uuid,
    },
    /// 命令已提交但没有单个 Stream 结果。
    Snapshot,
    /// 条件变更与当前归属冲突；状态不变。
    Conflict {
        /// 当前归属；Stream 不存在时为 None。
        current: Option<Owner>,
    },
    /// 输入违反归属不变量；状态不变。
    Invalid {
        /// 面向调用者的稳定错误原因。
        reason: String,
    },
}

/// 状态机应用结果，包含调用者结果与新的兼容投影。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipApply {
    /// 命令结果。
    pub outcome: OwnershipOutcome,
    /// 命令应用后的 `routes.json` 兼容投影。
    pub table: RouteTable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PendingMove {
    operation_id: Uuid,
    expected: OwnerMatch,
    target_shard: u64,
    generation: u64,
}

/// 控制 Shard 中持久化的归属权威状态。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipCatalog {
    initialized: bool,
    revision: u64,
    owners: BTreeMap<String, Owner>,
    eligible_shards: BTreeSet<u64>,
    pending_moves: BTreeMap<String, PendingMove>,
}

impl OwnershipCatalog {
    /// 返回当前 catalog 的全局归属 revision。
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// 查询 Stream 当前已发布归属。
    ///
    /// - `stream`：待查询的 Stream 名称。
    /// - 返回：已发布归属的借用；未知 Stream 返回 `None`。
    pub fn owner(&self, stream: &str) -> Option<&Owner> {
        self.owners.get(stream)
    }

    /// 返回当前 `routes.json` 兼容投影。
    ///
    /// 返回值是独立快照，调用方修改它不会改变权威 catalog。
    pub fn project(&self) -> RouteTable {
        let mut table = RouteTable {
            version: self.revision,
            streams: BTreeMap::new(),
            shard_stream_counts: BTreeMap::new(),
            stream_generations: BTreeMap::new(),
            stream_revisions: BTreeMap::new(),
        };
        for (stream, owner) in &self.owners {
            table.streams.insert(stream.clone(), owner.shard_id);
            table
                .stream_generations
                .insert(stream.clone(), owner.generation);
            table
                .stream_revisions
                .insert(stream.clone(), owner.revision);
            *table.shard_stream_counts.entry(owner.shard_id).or_insert(0) += 1;
        }
        table
    }

    /// 应用一条归属命令。
    ///
    /// - `command`：由控制 Shard Raft 日志串行提交的归属命令。
    /// - 返回：命令结果及应用后的完整兼容投影。
    ///
    /// 返回值只描述已提交状态；`Conflict` 与 `Invalid` 不改变 catalog。
    pub fn apply(&mut self, command: OwnershipCommand) -> OwnershipApply {
        let outcome = match command {
            OwnershipCommand::Bootstrap {
                legacy,
                eligible_shards,
            } => self.bootstrap(legacy, eligible_shards),
            OwnershipCommand::Ensure {
                stream,
                eligible_shards,
            } => self.ensure(stream, eligible_shards),
            OwnershipCommand::PrepareMove {
                operation_id,
                stream,
                expected,
                target_shard,
            } => self.prepare_move(operation_id, stream, expected, target_shard),
            OwnershipCommand::CompleteMove {
                operation_id,
                stream,
            } => self.complete_move(operation_id, &stream),
            OwnershipCommand::ApplyPlacement { eligible_shards } => {
                self.apply_placement(eligible_shards)
            }
            OwnershipCommand::AdoptOrphan {
                stream,
                target_shard,
            } => self.adopt_orphan(stream, target_shard),
        };
        OwnershipApply {
            outcome,
            table: self.project(),
        }
    }

    fn bootstrap(
        &mut self,
        legacy: RouteTable,
        eligible_shards: BTreeSet<u64>,
    ) -> OwnershipOutcome {
        if self.initialized {
            return OwnershipOutcome::Snapshot;
        }
        self.initialized = true;
        self.revision = legacy.version;
        self.eligible_shards = eligible_shards;
        for (stream, shard_id) in legacy.streams {
            let generation = legacy
                .stream_generations
                .get(&stream)
                .copied()
                .unwrap_or(1)
                .max(1);
            let revision = legacy
                .stream_revisions
                .get(&stream)
                .copied()
                .unwrap_or(legacy.version);
            self.owners
                .insert(stream, Owner::new(shard_id, generation, revision));
        }
        OwnershipOutcome::Snapshot
    }

    fn ensure(&mut self, stream: String, initial_eligible: BTreeSet<u64>) -> OwnershipOutcome {
        if stream.is_empty() {
            return OwnershipOutcome::Invalid {
                reason: "stream 不能为空".to_string(),
            };
        }
        if let Some(owner) = self.owners.get(&stream) {
            return OwnershipOutcome::Owner {
                owner: owner.clone(),
                created: false,
            };
        }
        if !self.initialized {
            self.initialized = true;
            self.eligible_shards = initial_eligible;
        }
        let Some(shard_id) = self.eligible_shards.iter().copied().min_by_key(|shard| {
            let count = self
                .owners
                .values()
                .filter(|owner| owner.shard_id == *shard)
                .count();
            (count, *shard)
        }) else {
            return OwnershipOutcome::Invalid {
                reason: "没有可分配的 Shard".to_string(),
            };
        };
        self.revision = self.revision.saturating_add(1);
        let owner = Owner::new(shard_id, 1, self.revision);
        self.owners.insert(stream, owner.clone());
        OwnershipOutcome::Owner {
            owner,
            created: true,
        }
    }

    fn prepare_move(
        &mut self,
        operation_id: Uuid,
        stream: String,
        expected: OwnerMatch,
        target_shard: u64,
    ) -> OwnershipOutcome {
        let Some(current) = self.owners.get(&stream).cloned() else {
            return OwnershipOutcome::Conflict { current: None };
        };
        if current.shard_id == target_shard {
            return OwnershipOutcome::Owner {
                owner: current,
                created: false,
            };
        }
        if current.match_token() != expected {
            return OwnershipOutcome::Conflict {
                current: Some(current),
            };
        }
        if !self.eligible_shards.contains(&target_shard) {
            return OwnershipOutcome::Invalid {
                reason: format!("目标 Shard {target_shard} 不在可分配集合中"),
            };
        }
        if let Some(pending) = self.pending_moves.get(&stream) {
            if pending.expected == expected && pending.target_shard == target_shard {
                return OwnershipOutcome::MovePrepared {
                    current,
                    target_shard,
                    generation: pending.generation,
                    operation_id: pending.operation_id,
                };
            }
            return OwnershipOutcome::Conflict {
                current: Some(current),
            };
        }
        self.revision = self.revision.saturating_add(1);
        let generation = current.generation.saturating_add(1);
        self.pending_moves.insert(
            stream,
            PendingMove {
                operation_id,
                expected,
                target_shard,
                generation,
            },
        );
        OwnershipOutcome::MovePrepared {
            current,
            target_shard,
            generation,
            operation_id,
        }
    }

    fn complete_move(&mut self, operation_id: Uuid, stream: &str) -> OwnershipOutcome {
        let Some(pending) = self.pending_moves.get(stream).cloned() else {
            return match self.owners.get(stream) {
                Some(owner) => OwnershipOutcome::Owner {
                    owner: owner.clone(),
                    created: false,
                },
                None => OwnershipOutcome::Conflict { current: None },
            };
        };
        if pending.operation_id != operation_id {
            return OwnershipOutcome::Conflict {
                current: self.owners.get(stream).cloned(),
            };
        }
        self.revision = self.revision.saturating_add(1);
        let owner = Owner::new(pending.target_shard, pending.generation, self.revision);
        self.owners.insert(stream.to_string(), owner.clone());
        self.pending_moves.remove(stream);
        OwnershipOutcome::Owner {
            owner,
            created: false,
        }
    }

    fn apply_placement(&mut self, eligible_shards: BTreeSet<u64>) -> OwnershipOutcome {
        if eligible_shards.is_empty() {
            return OwnershipOutcome::Invalid {
                reason: "可分配 Shard 集合不能为空".to_string(),
            };
        }
        if self.eligible_shards != eligible_shards {
            self.initialized = true;
            self.eligible_shards = eligible_shards;
            self.revision = self.revision.saturating_add(1);
        }
        OwnershipOutcome::Snapshot
    }

    fn adopt_orphan(&mut self, stream: String, target_shard: u64) -> OwnershipOutcome {
        if stream.is_empty() || !self.eligible_shards.contains(&target_shard) {
            return OwnershipOutcome::Invalid {
                reason: "孤儿 Stream 或目标 Shard 无效".into(),
            };
        }
        if let Some(current) = self.owners.get(&stream) {
            return if current.shard_id == target_shard {
                OwnershipOutcome::Owner {
                    owner: current.clone(),
                    created: false,
                }
            } else {
                OwnershipOutcome::Conflict {
                    current: Some(current.clone()),
                }
            };
        }
        self.revision = self.revision.saturating_add(1);
        let owner = Owner::new(target_shard, 1, self.revision);
        self.owners.insert(stream, owner.clone());
        OwnershipOutcome::Owner {
            owner,
            created: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn eligible() -> BTreeSet<u64> {
        [0, 1, 2].into_iter().collect()
    }

    #[test]
    fn concurrent_ensure_is_serialized_to_one_owner() {
        let mut catalog = OwnershipCatalog::default();
        let first = catalog.apply(OwnershipCommand::Ensure {
            stream: "orders/1".into(),
            eligible_shards: eligible(),
        });
        let second = catalog.apply(OwnershipCommand::Ensure {
            stream: "orders/1".into(),
            eligible_shards: eligible(),
        });
        assert!(matches!(
            first.outcome,
            OwnershipOutcome::Owner { created: true, .. }
        ));
        assert!(matches!(
            second.outcome,
            OwnershipOutcome::Owner { created: false, .. }
        ));
        assert_eq!(first.table.streams, second.table.streams);
        assert_eq!(catalog.owner("orders/1").map(Owner::shard_id), Some(0));
    }

    #[test]
    fn legacy_table_without_generations_imports_as_generation_one() {
        let legacy: RouteTable = serde_json::from_str(
            r#"{"version":7,"streams":{"orders/1":2},"shard_stream_counts":{"2":1}}"#,
        )
        .expect("读取旧 routes.json");
        let mut catalog = OwnershipCatalog::default();
        catalog.apply(OwnershipCommand::Bootstrap {
            legacy,
            eligible_shards: eligible(),
        });
        let owner = catalog.owner("orders/1").expect("导入归属");
        assert_eq!(owner.shard_id(), 2);
        assert_eq!(owner.generation(), 1);
        assert_eq!(catalog.project().version, 7);
    }

    #[test]
    fn invalid_ensure_inputs_do_not_create_ownership() {
        let mut catalog = OwnershipCatalog::default();
        let empty_stream = catalog.apply(OwnershipCommand::Ensure {
            stream: String::new(),
            eligible_shards: eligible(),
        });
        assert_eq!(
            empty_stream.outcome,
            OwnershipOutcome::Invalid {
                reason: "stream 不能为空".into()
            }
        );
        assert_eq!(catalog.revision(), 0);

        let no_shard = catalog.apply(OwnershipCommand::Ensure {
            stream: "orders/no-shard".into(),
            eligible_shards: BTreeSet::new(),
        });
        assert_eq!(
            no_shard.outcome,
            OwnershipOutcome::Invalid {
                reason: "没有可分配的 Shard".into()
            }
        );
        assert!(catalog.owner("orders/no-shard").is_none());
    }

    #[test]
    fn bootstrap_preserves_explicit_metadata_and_is_idempotent() {
        let legacy = RouteTable {
            version: 9,
            streams: BTreeMap::from([("orders/metadata".into(), 2)]),
            shard_stream_counts: BTreeMap::from([(2, 1)]),
            stream_generations: BTreeMap::from([("orders/metadata".into(), 4)]),
            stream_revisions: BTreeMap::from([("orders/metadata".into(), 6)]),
        };
        let mut catalog = OwnershipCatalog::default();
        catalog.apply(OwnershipCommand::Bootstrap {
            legacy,
            eligible_shards: eligible(),
        });
        let imported = catalog.owner("orders/metadata").expect("导入归属");
        assert_eq!(imported.generation(), 4);
        assert_eq!(imported.revision(), 6);

        let replacement = RouteTable {
            version: 99,
            streams: BTreeMap::from([("unsafe-replacement".into(), 1)]),
            shard_stream_counts: BTreeMap::from([(1, 1)]),
            stream_generations: BTreeMap::new(),
            stream_revisions: BTreeMap::new(),
        };
        catalog.apply(OwnershipCommand::Bootstrap {
            legacy: replacement,
            eligible_shards: BTreeSet::from([1]),
        });
        assert_eq!(catalog.revision(), 9, "重复导入不得替换权威状态");
        assert!(catalog.owner("unsafe-replacement").is_none());
    }

    #[test]
    fn prepare_move_rejects_stale_or_invalid_requests() {
        let mut catalog = OwnershipCatalog::default();
        let missing = catalog.apply(OwnershipCommand::PrepareMove {
            operation_id: Uuid::new_v4(),
            stream: "missing".into(),
            expected: OwnerMatch {
                shard_id: 0,
                generation: 1,
            },
            target_shard: 1,
        });
        assert!(matches!(
            missing.outcome,
            OwnershipOutcome::Conflict { current: None }
        ));

        catalog.apply(OwnershipCommand::Ensure {
            stream: "orders/move".into(),
            eligible_shards: eligible(),
        });
        let current = catalog.owner("orders/move").expect("已有归属").clone();
        let same_target = catalog.apply(OwnershipCommand::PrepareMove {
            operation_id: Uuid::new_v4(),
            stream: "orders/move".into(),
            expected: current.match_token(),
            target_shard: current.shard_id(),
        });
        assert!(matches!(
            same_target.outcome,
            OwnershipOutcome::Owner { created: false, .. }
        ));

        let stale = catalog.apply(OwnershipCommand::PrepareMove {
            operation_id: Uuid::new_v4(),
            stream: "orders/move".into(),
            expected: OwnerMatch {
                shard_id: current.shard_id(),
                generation: current.generation() + 1,
            },
            target_shard: 1,
        });
        assert!(matches!(
            stale.outcome,
            OwnershipOutcome::Conflict { current: Some(_) }
        ));

        let invalid_target = catalog.apply(OwnershipCommand::PrepareMove {
            operation_id: Uuid::new_v4(),
            stream: "orders/move".into(),
            expected: current.match_token(),
            target_shard: 99,
        });
        assert_eq!(
            invalid_target.outcome,
            OwnershipOutcome::Invalid {
                reason: "目标 Shard 99 不在可分配集合中".into()
            }
        );
    }

    #[test]
    fn move_prepare_and_complete_retries_are_idempotent() {
        let mut catalog = OwnershipCatalog::default();
        catalog.apply(OwnershipCommand::Ensure {
            stream: "orders/retry".into(),
            eligible_shards: eligible(),
        });
        let current = catalog.owner("orders/retry").expect("已有归属").clone();
        let operation_id = Uuid::new_v4();
        let command = OwnershipCommand::PrepareMove {
            operation_id,
            stream: "orders/retry".into(),
            expected: current.match_token(),
            target_shard: 1,
        };
        let first = catalog.apply(command.clone());
        let retry = catalog.apply(command);
        assert_eq!(first.outcome, retry.outcome, "PrepareMove 重试必须幂等");

        let competing = catalog.apply(OwnershipCommand::PrepareMove {
            operation_id: Uuid::new_v4(),
            stream: "orders/retry".into(),
            expected: current.match_token(),
            target_shard: 2,
        });
        assert!(matches!(
            competing.outcome,
            OwnershipOutcome::Conflict { current: Some(_) }
        ));

        let wrong_operation = catalog.apply(OwnershipCommand::CompleteMove {
            operation_id: Uuid::new_v4(),
            stream: "orders/retry".into(),
        });
        assert!(matches!(
            wrong_operation.outcome,
            OwnershipOutcome::Conflict { current: Some(_) }
        ));

        let completed = catalog.apply(OwnershipCommand::CompleteMove {
            operation_id,
            stream: "orders/retry".into(),
        });
        assert!(matches!(
            completed.outcome,
            OwnershipOutcome::Owner { created: false, .. }
        ));
        assert_eq!(catalog.owner("orders/retry").map(Owner::shard_id), Some(1));
        let repeated = catalog.apply(OwnershipCommand::CompleteMove {
            operation_id,
            stream: "orders/retry".into(),
        });
        assert!(matches!(
            repeated.outcome,
            OwnershipOutcome::Owner { created: false, .. }
        ));
        let unknown = catalog.apply(OwnershipCommand::CompleteMove {
            operation_id,
            stream: "missing".into(),
        });
        assert!(matches!(
            unknown.outcome,
            OwnershipOutcome::Conflict { current: None }
        ));
    }

    #[test]
    fn placement_rejects_empty_and_only_advances_on_change() {
        let mut catalog = OwnershipCatalog::default();
        let invalid = catalog.apply(OwnershipCommand::ApplyPlacement {
            eligible_shards: BTreeSet::new(),
        });
        assert_eq!(
            invalid.outcome,
            OwnershipOutcome::Invalid {
                reason: "可分配 Shard 集合不能为空".into()
            }
        );
        assert_eq!(catalog.revision(), 0);

        catalog.apply(OwnershipCommand::ApplyPlacement {
            eligible_shards: eligible(),
        });
        let changed_revision = catalog.revision();
        catalog.apply(OwnershipCommand::ApplyPlacement {
            eligible_shards: eligible(),
        });
        assert_eq!(
            catalog.revision(),
            changed_revision,
            "相同放置表不得制造新 revision"
        );
    }

    proptest! {
        #[test]
        fn ensure_sequences_preserve_unique_ownership(
            streams in prop::collection::vec("[a-z]{1,12}", 1..100),
        ) {
            let mut catalog = OwnershipCatalog::default();
            for stream in &streams {
                let before = catalog.owner(stream).cloned();
                let applied = catalog.apply(OwnershipCommand::Ensure {
                    stream: stream.clone(),
                    eligible_shards: eligible(),
                });
                let after = catalog.owner(stream).cloned().expect("Ensure 后必有归属");
                if let Some(before) = before {
                    prop_assert_eq!(before, after, "重复 Ensure 不能改变归属");
                    prop_assert!(
                        matches!(applied.outcome, OwnershipOutcome::Owner { created: false, .. }),
                        "重复 Ensure 必须返回 created=false"
                    );
                }
            }
            let projected = catalog.project();
            prop_assert_eq!(projected.streams.len(), catalog.owners.len());
            let count_sum: u64 = projected.shard_stream_counts.values().sum();
            prop_assert_eq!(count_sum as usize, catalog.owners.len());
        }
    }
}
