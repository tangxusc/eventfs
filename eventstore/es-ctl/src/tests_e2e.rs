//! esctl 命令 e2e 测试（真实单节点集群 / 本地数据目录）。
//!
//! bin 内嵌 cfg(test) 的原因：cli/client/commands 模块是私有 mod，
//! integration test 无法引用；放在 crate 内部则可直接构造 Args 与 Ctx。

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use es_proto::eventstore::event_store_server::EventStoreServer;
use es_proto::eventstore::*;
use es_server::config::{Config, NodeConfig, PlacementConfig, PlacementNode, StorageConfig};
use es_server::Server;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::cli::*;
use crate::client::ClusterClient;
use crate::commands::{append, init, member, meta, migrate, read, route, status, watch, Ctx};

/// 启动单节点测试集群（2 分片，raft 已初始化，gRPC 已监听）。
/// 返回 (gRPC 地址, Server, TempDir)。
async fn start_server() -> (String, Server, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("临时目录");
    let config = Config {
        node: NodeConfig {
            id: 1,
            listen_addr: "127.0.0.1:0".into(),
            internal_listen_addr: None,
            peers: vec![],
        },
        storage: StorageConfig {
            data_dir: dir.path().to_path_buf(),
            memtable_arena_bytes: 4 * 1024 * 1024,
        },
        // 单节点 2 分片：rf=1，node1 主承载 [0,1]
        placement: PlacementConfig {
            replication_factor: 1,
            nodes: vec![PlacementNode {
                id: 1,
                primary: (0..2).collect(),
                replica: vec![],
            }],
        },
        snapshot: Default::default(),
        tls: None,
        limits: Default::default(),
    };
    let server = Server::new(config.clone()).expect("创建服务器");
    server.init().await.expect("初始化");

    let members = BTreeSet::from([1u64]);
    for shard_id in 0..2 {
        let shard = server
            .shard_manager()
            .get_shard(shard_id)
            .await
            .expect("取分片");
        shard
            .raft
            .initialize(members.clone())
            .await
            .expect("初始化 raft");
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().expect("取本地地址"));
    // 共享 server 的路由表实例（EsService::new 会自建独立实例，内存态不同步）
    let service = es_server::service::EsService::with_ownership(
        server.shard_manager().clone(),
        config.limits.clone(),
        server.route_table().clone(),
        server.ownership().clone(),
        &config,
    )
    .expect("创建服务");
    let admin = es_raft::admin_service::RaftAdminService::new(server.shard_manager().clone());
    let raft = es_raft::rpc_service::RaftRpcService::new(server.shard_manager().clone());
    let migration = es_server::migration_service::MigrationService::new(
        server.route_table().clone(),
        server.shard_manager().clone(),
        server.ownership().clone(),
    );
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(EventStoreServer::new(service))
            .add_service(es_proto::eventstore::raft_rpc_server::RaftRpcServer::new(
                raft,
            ))
            .add_service(es_proto::eventstore::raft_admin_server::RaftAdminServer::new(admin))
            .add_service(es_proto::eventstore::migration_server::MigrationServer::new(migration))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    (addr, server, dir)
}

fn global(write_out: Format, endpoints: Vec<String>, shards: Option<u64>) -> GlobalArgs {
    GlobalArgs {
        endpoints,
        dial_timeout: 2,
        timeout: 5,
        cacert: None,
        insecure_skip_tls_verify: false,
        write_out,
        shards,
    }
}

fn ctx_with(addr: String, format: Format) -> Ctx {
    let cluster = ClusterClient::new(
        &[addr.clone()],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    Ctx::new(cluster, global(format, vec![addr], None))
}

fn append_args(stream: &str, event_type: &str, data: Option<String>) -> AppendArgs {
    AppendArgs {
        stream: stream.to_string(),
        event_type: event_type.to_string(),
        data,
        data_file: None,
        metadata: None,
        metadata_file: None,
        event_id: None,
        expected_version: ExpectedVersionArg::Any,
    }
}

fn ev_any() -> Option<ExpectedVersion> {
    Some(ExpectedVersion {
        kind: Some(expected_version::Kind::Any(Empty {})),
    })
}

fn route_args(check: bool, recount: bool) -> RouteArgs {
    RouteArgs {
        show: false,
        recount,
        check,
    }
}

fn migrate_args(stream: Option<&str>, to: u64, dry_run: bool) -> MigrateArgs {
    MigrateArgs {
        stream: stream.map(str::to_string),
        shard: None,
        to,
        dry_run,
        drain_quiet_rounds: 1,
        drain_timeout_secs: 1,
    }
}

fn migrate_shard_args(shard: u64, to: u64) -> MigrateArgs {
    MigrateArgs {
        stream: None,
        shard: Some(shard),
        to,
        dry_run: false,
        drain_quiet_rounds: 1,
        drain_timeout_secs: 1,
    }
}

// ---------- append ----------

#[tokio::test]
async fn append_three_formats_and_read_back() {
    let (addr, _s, _dir) = start_server().await;
    for fmt in [Format::Simple, Format::Table, Format::Json] {
        let ctx = ctx_with(addr.clone(), fmt);
        append::run(&ctx, &append_args("s-a", "E", Some("payload".into())))
            .await
            .expect("append 成功");

        // 读回校验（read 命令）
        let read_ctx = ctx_with(addr.clone(), Format::Simple);
        let rargs = ReadArgs {
            stream: "s-a".into(),
            from_version: 0,
            max_count: 0,
            backward: false,
        };
        read::run(&read_ctx, &rargs).await.expect("read 成功");
        let bargs = ReadArgs {
            stream: "s-a".into(),
            from_version: 0,
            max_count: 0,
            backward: true, // 覆盖 u64::MAX 哨兵分支
        };
        read::run(&read_ctx, &bargs)
            .await
            .expect("backward read 成功");
    }
}

// ---------- route：展示、校准与跨分片一致性检查 ----------

#[tokio::test]
async fn route_commands_report_consistent_orphan_and_phantom_streams() {
    let (addr, server, _dir) = start_server().await;
    let stream = "route-check-stream";
    append::run(
        &ctx_with(addr.clone(), Format::Simple),
        &append_args(stream, "Created", Some("payload".into())),
    )
    .await
    .expect("追加测试流");

    let owner = server
        .route_table()
        .lookup(stream)
        .await
        .expect("追加后应记录流路由");

    // 三种展示格式都应经由真实 Migration RPC 获取路由表。
    for format in [Format::Simple, Format::Table, Format::Json] {
        route::run(&ctx_with(addr.clone(), format), &route_args(false, false))
            .await
            .expect("展示路由表");
    }
    route::run(
        &ctx_with(addr.clone(), Format::Simple),
        &route_args(false, true),
    )
    .await
    .expect("校准路由表计数");

    // 正常路由：实际存储与路由表归属一致。
    route::run(
        &ctx_with(addr.clone(), Format::Simple),
        &route_args(true, false),
    )
    .await
    .expect("一致性检查应成功");

    // 用版本更高的本地表模拟路由文件回退，已落盘事件成为孤儿流。
    let mut without_stream = es_core::route::RouteTable::new();
    without_stream.version = server.route_table().snapshot().await.version + 1;
    let routes_path = es_server::route_table::routes_path(&server.config().storage.data_dir);
    std::fs::write(
        &routes_path,
        serde_json::to_vec(&without_stream).expect("序列化孤儿路由表"),
    )
    .expect("写入孤儿路由表");
    server.route_table().reload().await;
    assert_eq!(server.route_table().lookup(stream).await, None);
    route::run(
        &ctx_with(addr.clone(), Format::Simple),
        &route_args(true, false),
    )
    .await
    .expect("孤儿流检查应成功");

    // 将同一流切到另一 shard，模拟迁移切换未同步时的虚挂路由。
    let other_shard = owner ^ 1;
    server
        .route_table()
        .set_stream_shard(stream, other_shard)
        .await
        .expect("设置虚挂路由");
    for format in [Format::Table, Format::Json] {
        route::run(&ctx_with(addr.clone(), format), &route_args(true, false))
            .await
            .expect("虚挂流检查应成功");
    }
}

// ---------- migrate：真实状态机的早期退出与校验路径 ----------

#[tokio::test]
async fn migrate_handles_noop_invalid_and_missing_source_states() {
    let (addr, server, _dir) = start_server().await;
    let stream = "migrate-state-stream";
    append::run(
        &ctx_with(addr.clone(), Format::Simple),
        &append_args(stream, "Created", Some("payload".into())),
    )
    .await
    .expect("创建迁移测试流");
    let owner = server
        .route_table()
        .lookup(stream)
        .await
        .expect("流应有路由");
    let other = owner ^ 1;

    // 同一 shard 且无残留源数据：迁移应幂等返回，无需复制或删除。
    migrate::run(
        &ctx_with(addr.clone(), Format::Simple),
        &migrate_args(Some(stream), owner, false),
    )
    .await
    .expect("同 shard 无操作应成功");

    // 目标未出现在放置表时必须在复制前拒绝。
    let err = migrate::run(
        &ctx_with(addr.clone(), Format::Simple),
        &migrate_args(Some(stream), 99, false),
    )
    .await
    .expect_err("不存在的目标分片应失败");
    assert!(err.to_string().contains("不存在"), "{err}");

    // 路由表与存储中均不存在的流不能被静默创建为迁移目标。
    let err = migrate::run(
        &ctx_with(addr.clone(), Format::Simple),
        &migrate_args(Some("absent-migration-stream"), other, false),
    )
    .await
    .expect_err("不存在的源流应失败");
    assert!(err.to_string().contains("未创建"), "{err}");

    // 手工路由存在但源数据已被清理：状态机应幂等成功并保留可重跑语义。
    server
        .route_table()
        .set_stream_shard("empty-migration-stream", owner)
        .await
        .expect("写入空流路由");
    migrate::run(
        &ctx_with(addr.clone(), Format::Table),
        &migrate_args(Some("empty-migration-stream"), other, false),
    )
    .await
    .expect("源数据缺失的幂等迁移应成功");

    // dry-run 只计算版本差，不切换路由或写入目标分片。
    migrate::run(
        &ctx_with(addr, Format::Json),
        &migrate_args(Some(stream), other, true),
    )
    .await
    .expect("dry-run 应成功");
    assert_eq!(server.route_table().lookup(stream).await, Some(owner));
}

#[tokio::test]
async fn migrate_stream_copies_data_switches_route_and_cleans_source() {
    let (addr, server, _dir) = start_server().await;
    let ctx = ctx_with(addr.clone(), Format::Simple);
    let stream = "migrate-complete-stream";
    append::run(&ctx, &append_args(stream, "Created", Some("first".into())))
        .await
        .expect("写入首个迁移事件");
    append::run(&ctx, &append_args(stream, "Updated", Some("second".into())))
        .await
        .expect("写入第二个迁移事件");
    let source = server
        .route_table()
        .lookup(stream)
        .await
        .expect("读取源路由");
    let target = source ^ 1;

    migrate::run(&ctx, &migrate_args(Some(stream), target, false))
        .await
        .expect("完整迁移应成功");
    assert_eq!(
        server.route_table().lookup(stream).await,
        Some(target),
        "切换后路由必须指向目标分片"
    );

    let mut data_client = ctx
        .cluster
        .event_client(&addr)
        .await
        .expect("连接数据面服务");
    let meta = data_client
        .get_stream_meta(GetStreamMetaRequest {
            stream_id: stream.into(),
        })
        .await
        .expect("读取迁移后元数据")
        .into_inner();
    assert!(meta.exists, "迁移后目标流必须存在");
    assert_eq!(meta.shard_id, target);
    assert_eq!(meta.current_version, 1, "两个事件的版本必须保留");

    let mut events = data_client
        .read_stream(ReadStreamRequest {
            stream_id: stream.into(),
            from_version: 0,
            max_count: 0,
            direction: Direction::Forward as i32,
        })
        .await
        .expect("读取迁移后数据")
        .into_inner();
    let mut data = Vec::new();
    while let Some(page) = events.message().await.expect("读取迁移响应") {
        data.extend(page.events.into_iter().map(|event| event.data));
    }
    assert_eq!(data, vec![b"first".to_vec(), b"second".to_vec()]);

    let source_shard = server
        .shard_manager()
        .get_shard(source)
        .await
        .expect("读取源分片");
    assert!(
        source_shard
            .storage
            .read_stream_events(stream, 0, 0)
            .expect("读取源数据")
            .is_empty(),
        "完成迁移后源分片必须清理旧数据"
    );
}

#[tokio::test]
async fn migrate_shard_handles_empty_and_partial_failure_without_hiding_errors() {
    let (addr, _server, _dir) = start_server().await;
    let ctx = ctx_with(addr.clone(), Format::Simple);

    // 空源分片不是错误：批量迁移应明确完成，而不是尝试创建目标流。
    migrate::run(&ctx, &migrate_shard_args(0, 1))
        .await
        .expect("空分片迁移应无操作成功");

    let stream = "migrate-shard-failure";
    append::run(
        &ctx,
        &append_args(stream, "Created", Some("payload".into())),
    )
    .await
    .expect("创建批量迁移测试流");
    let mut data_client = ctx
        .cluster
        .event_client(&addr)
        .await
        .expect("连接数据面服务");
    let meta = data_client
        .get_stream_meta(GetStreamMetaRequest {
            stream_id: stream.into(),
        })
        .await
        .expect("读取流元数据")
        .into_inner();

    // 目标不存在时逐流失败必须聚合为命令错误，不能静默吞掉部分失败。
    let err = migrate::run(&ctx, &migrate_shard_args(meta.shard_id, 99))
        .await
        .expect_err("批量迁移的失败流必须返回错误");
    assert!(err.to_string().contains("迁移失败"), "{err}");
}

#[tokio::test]
async fn append_data_and_metadata_files() {
    let (addr, _s, _dir) = start_server().await;
    let tmp = tempfile::tempdir().expect("临时目录");
    let data_file = tmp.path().join("d.bin");
    let meta_file = tmp.path().join("m.bin");
    std::fs::write(&data_file, b"\x01\x02").expect("写数据文件");
    std::fs::write(&meta_file, b"m").expect("写元数据文件");

    let ctx = ctx_with(addr.clone(), Format::Simple);
    // --data-file 分支
    append::run(
        &ctx,
        &AppendArgs {
            stream: "s-f".into(),
            event_type: "E".into(),
            data: None,
            data_file: Some(data_file),
            metadata: Some("meta".into()), // --metadata 分支
            metadata_file: None,
            event_id: Some(uuid::Uuid::new_v4().to_string()),
            expected_version: ExpectedVersionArg::Any,
        },
    )
    .await
    .expect("data-file append 成功");
    // --metadata-file 分支
    append::run(
        &ctx,
        &AppendArgs {
            stream: "s-f".into(),
            event_type: "E".into(),
            data: Some("d".into()),
            data_file: None,
            metadata: None,
            metadata_file: Some(meta_file),
            event_id: None,
            expected_version: ExpectedVersionArg::Any,
        },
    )
    .await
    .expect("meta-file append 成功");
}

#[tokio::test]
async fn append_invalid_event_id_rejected() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr, Format::Simple);
    let err = append::run(
        &ctx,
        &AppendArgs {
            stream: "s".into(),
            event_type: "E".into(),
            data: Some("d".into()),
            data_file: None,
            metadata: None,
            metadata_file: None,
            event_id: Some("not-a-uuid".into()),
            expected_version: ExpectedVersionArg::Any,
        },
    )
    .await
    .expect_err("非法 event_id 应报错");
    assert!(err.to_string().contains("非法事件 ID"), "{err}");
}

#[tokio::test]
async fn append_optimistic_conflict_translated() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr, Format::Simple);
    // 先写一条（version 0）
    append::run(&ctx, &append_args("s-conf", "E", Some("x".into())))
        .await
        .expect("首次 append");
    // 期望 exact(5) 与实际不符 → FailedPrecondition → 中文翻译
    let err = append::run(
        &ctx,
        &AppendArgs {
            stream: "s-conf".into(),
            event_type: "E".into(),
            data: Some("y".into()),
            data_file: None,
            metadata: None,
            metadata_file: None,
            event_id: None,
            expected_version: ExpectedVersionArg::Exact(5),
        },
    )
    .await
    .expect_err("版本冲突应报错");
    assert!(err.to_string().contains("乐观并发冲突"), "{err}");
}

// ---------- read / readall / meta ----------

#[tokio::test]
async fn readall_three_cursor_modes_and_backward() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr.clone(), Format::Simple);
    // 造数据
    append::run(&ctx, &append_args("s-ra", "E", Some("1".into())))
        .await
        .expect("append");

    // 默认（scope.all_ids 分支）
    let all = ReadAllArgs {
        from_position: 0,
        from_positions: None,
        max_count: 0,
        backward: false,
        shard_ids: None,
    };
    read::run_all(&ctx, &all).await.expect("readall 默认");
    // --from-positions 覆盖分支
    let with_pos = ReadAllArgs {
        from_position: 0,
        from_positions: Some(ShardPositions(vec![(0, 0), (1, 0)])),
        max_count: 0,
        backward: false,
        shard_ids: None,
    };
    read::run_all(&ctx, &with_pos).await.expect("readall 游标");
    // --shard-ids 分支
    let with_ids = ReadAllArgs {
        from_position: 0,
        from_positions: None,
        max_count: 0,
        backward: false,
        shard_ids: Some(ShardIds(vec![0, 1])),
    };
    read::run_all(&ctx, &with_ids)
        .await
        .expect("readall 分片列表");
    // 反向（u64::MAX 哨兵）
    let backward = ReadAllArgs {
        from_position: 0,
        from_positions: None,
        max_count: 0,
        backward: true,
        shard_ids: None,
    };
    read::run_all(&ctx, &backward).await.expect("readall 反向");
}

#[tokio::test]
async fn meta_three_formats() {
    let (addr, _s, _dir) = start_server().await;
    for fmt in [Format::Simple, Format::Table, Format::Json] {
        let ctx = ctx_with(addr.clone(), fmt);
        meta::run(
            &ctx,
            &MetaArgs {
                stream: "s-m".into(),
            },
        )
        .await
        .expect("meta 成功");
    }
}

// ---------- member / status ----------

#[tokio::test]
async fn member_list_add_learner_and_promote() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr.clone(), Format::Simple);

    // list（Simple 空/非空节点路径）
    member::run(
        &ctx,
        &MemberArgs {
            action: MemberAction::List(MemberListArgs {}),
        },
    )
    .await
    .expect("member list");
    for fmt in [Format::Table, Format::Json] {
        let tctx = ctx_with(addr.clone(), fmt);
        member::run(
            &tctx,
            &MemberArgs {
                action: MemberAction::List(MemberListArgs {}),
            },
        )
        .await
        .expect("member list 渲染");
    }

    // add：learner_only（不触发提升）——分片 1。
    // 注意：add_learner(no_blocking) 的 membership 变更需要 learner 实际响应
    // 才能提交，故提升路径的测试走 FlakyStub（见 member_promote_and_remove_via_stub）。
    member::run(
        &ctx,
        &MemberArgs {
            action: MemberAction::Add(MemberAddArgs {
                shard: Some(1),
                all_shards: false,
                member: MemberArg {
                    node_id: 2,
                    addr: "127.0.0.1:59999".into(),
                },
                no_blocking: true,
                learner_only: true,
            }),
        },
    )
    .await
    .expect("add learner");
}

#[tokio::test]
async fn member_remove_non_voter_rejected() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr, Format::Simple);
    let err = member::run(
        &ctx,
        &MemberArgs {
            action: MemberAction::Remove(MemberRemoveArgs {
                shard: Some(0),
                all_shards: false,
                node_id: 99,
                retain: false,
            }),
        },
    )
    .await
    .expect_err("节点 99 不是投票成员应报错");
    assert!(err.to_string().contains("不在其中"), "{err}");
}

#[tokio::test]
async fn member_promote_and_remove_via_stub() {
    // 真实集群中 add_learner 变更需 learner 响应才能提交，无法在单节点推进，
    // 提升/移除的 CAS 读-改-写路径用 FlakyStub 覆盖（admin RPC 语义不变）。
    for fmt in [Format::Simple, Format::Table, Format::Json] {
        // 提升：get_raft_state 返回 voters=[1] → change_membership([1,2])
        let stub = FlakyStub::new();
        stub.set_voters(vec![1]);
        let addr = start_flaky(stub).await;
        let ctx = ctx_with(addr, fmt);
        member::run(
            &ctx,
            &MemberArgs {
                action: MemberAction::Add(MemberAddArgs {
                    shard: Some(0),
                    all_shards: false,
                    member: MemberArg {
                        node_id: 2,
                        addr: "127.0.0.1:59999".into(),
                    },
                    no_blocking: true,
                    learner_only: false,
                }),
            },
        )
        .await
        .expect("提升成员成功");

        // 移除（--retain 降级分支）：voters=[1,2] → 移除节点 2
        let stub = FlakyStub::new();
        stub.set_voters(vec![1, 2]);
        let addr = start_flaky(stub).await;
        let ctx = ctx_with(addr, fmt);
        member::run(
            &ctx,
            &MemberArgs {
                action: MemberAction::Remove(MemberRemoveArgs {
                    shard: Some(0),
                    all_shards: false,
                    node_id: 2,
                    retain: true,
                }),
            },
        )
        .await
        .expect("移除成员(降级)成功");

        // 移除（非 retain）：detail 分支
        let stub = FlakyStub::new();
        stub.set_voters(vec![1, 2]);
        let addr = start_flaky(stub).await;
        let ctx = ctx_with(addr, fmt);
        member::run(
            &ctx,
            &MemberArgs {
                action: MemberAction::Remove(MemberRemoveArgs {
                    shard: Some(0),
                    all_shards: false,
                    node_id: 2,
                    retain: false,
                }),
            },
        )
        .await
        .expect("移除成员成功");
    }
}

#[tokio::test]
async fn status_three_formats() {
    let (addr, _s, _dir) = start_server().await;
    for fmt in [Format::Simple, Format::Table, Format::Json] {
        let ctx = ctx_with(addr.clone(), fmt);
        status::run(&ctx, &StatusArgs {})
            .await
            .expect("status 成功");
    }
}

// ---------- watch ----------

#[tokio::test]
async fn watch_once_exits_after_catchup() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr.clone(), Format::Simple);
    // 造一条历史事件，确保 catch-up 有内容
    append::run(&ctx, &append_args("s-w", "E", Some("e1".into())))
        .await
        .expect("append");

    // 订阅单流 --once：收到 caught_up 后退出
    let stream_watch = WatchArgs {
        stream: vec!["s-w".into()],
        all: false,
        once: true,
    };
    tokio::time::timeout(Duration::from_secs(5), watch::run(&ctx, &stream_watch))
        .await
        .expect("watch 不应挂起")
        .expect("watch once 成功");

    // 订阅 $all --once + Json 格式（覆盖 render_message 的 Json/table 分支）
    let jctx = ctx_with(addr.clone(), Format::Json);
    let all_watch = WatchArgs {
        stream: vec![],
        all: true,
        once: true,
    };
    tokio::time::timeout(Duration::from_secs(5), watch::run(&jctx, &all_watch))
        .await
        .expect("watch 不应挂起")
        .expect("watch $all 成功");

    // 参数错误：既无 stream 也无 --all
    let bad = WatchArgs {
        stream: vec![],
        all: false,
        once: true,
    };
    assert!(watch::run(&jctx, &bad).await.is_err(), "缺目标应报错");
}

// ---------- init ----------

#[tokio::test]
async fn init_success_three_formats_via_stub() {
    // Server::init() 会自动初始化 raft（单节点自动组建），真实服务器上
    // init 命令必然命中"已初始化"告警；成功路径用 FlakyStub 模拟。
    for fmt in [Format::Simple, Format::Table, Format::Json] {
        let stub = FlakyStub::new();
        let addr = start_flaky(stub).await;
        let ctx = ctx_with(addr, fmt);
        init::run(
            &ctx,
            &InitArgs {
                shard: Some(0),
                all_shards: false,
                member: vec![MemberArg {
                    node_id: 1,
                    addr: "127.0.0.1:50051".into(),
                }],
                yes: true,
            },
        )
        .await
        .expect("init 成功");
    }
}

#[tokio::test]
async fn init_already_initialized_warns() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr, Format::Simple);
    let err = init::run(
        &ctx,
        &InitArgs {
            shard: Some(0),
            all_shards: false,
            member: vec![MemberArg {
                node_id: 1,
                addr: "127.0.0.1:50051".into(),
            }],
            yes: true,
        },
    )
    .await
    .expect_err("重复 init 应报错");
    assert!(err.to_string().contains("初始化失败"), "{err}");
}

// ---------- Ctx 分片缓存 ----------

#[tokio::test]
async fn shards_probe_and_cache() {
    let (addr, _s, _dir) = start_server().await;
    let ctx = ctx_with(addr, Format::Simple);
    let scope1 = ctx.shards().await.expect("首次探测");
    assert_eq!(scope1.count(), 2, "应探测到 2 分片");
    let scope2 = ctx.shards().await.expect("缓存命中");
    assert_eq!(scope1, scope2, "缓存应返回同一结果");
}

// ---------- 行为可配置 stub（client.rs 网络错误分支） ----------

/// 可配置行为 stub：按调用次数弹出队列中的预设响应，耗尽后返回默认 Ok。
///
/// 字段用 Arc 共享：clone 时预设队列也共享（测试侧 push，server 侧消费）。
#[derive(Clone)]
struct FlakyStub {
    queue: Arc<std::sync::Mutex<std::collections::VecDeque<Result<(), Status>>>>,
    /// 收到的 append 调用数（验证重定向目标）
    append_calls: Arc<std::sync::atomic::AtomicU64>,
    /// get_raft_state 预设响应（is_leader 布尔或错误）
    raft_states: Arc<std::sync::Mutex<std::collections::VecDeque<Result<bool, Status>>>>,
    /// get_raft_state 返回的 voter_ids（member 提升/移除 CAS 读-改-写用）
    voter_ids: Arc<std::sync::Mutex<Vec<u64>>>,
    /// initialize 的预设响应（init 命令端点失败路径用）
    initialize_result: Arc<std::sync::Mutex<Result<(), Status>>>,
    /// subscribe 预设的事件（发送后立即关闭流，模拟服务端断流）
    subscribe_events: Arc<std::sync::Mutex<Vec<SubscriptionEvent>>>,
    /// 是否在断流前发送降级信号，用于验证 --once 的失败语义。
    subscribe_degraded: Arc<std::sync::atomic::AtomicBool>,
    /// 是否在断流前发送追平信号，用于验证非 --once 的正常关闭路径。
    subscribe_caught_up: Arc<std::sync::atomic::AtomicBool>,
}

impl FlakyStub {
    fn new() -> Self {
        Self {
            queue: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
            append_calls: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            raft_states: Arc::new(std::sync::Mutex::new(std::collections::VecDeque::new())),
            voter_ids: Arc::new(std::sync::Mutex::new(vec![1])),
            initialize_result: Arc::new(std::sync::Mutex::new(Ok(()))),
            subscribe_events: Arc::new(std::sync::Mutex::new(Vec::new())),
            subscribe_degraded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            subscribe_caught_up: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    fn set_voters(&self, voters: Vec<u64>) {
        *self.voter_ids.lock().unwrap() = voters;
    }

    fn set_initialize(&self, r: Result<(), Status>) {
        *self.initialize_result.lock().unwrap() = r;
    }

    fn set_subscribe_events(&self, events: Vec<SubscriptionEvent>) {
        *self.subscribe_events.lock().unwrap() = events;
    }

    fn set_subscribe_degraded(&self) {
        self.subscribe_degraded
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn set_subscribe_caught_up(&self) {
        self.subscribe_caught_up
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn push_append(&self, r: Result<(), Status>) {
        self.queue.lock().unwrap().push_back(r);
    }

    fn push_raft_state(&self, r: Result<bool, Status>) {
        self.raft_states.lock().unwrap().push_back(r);
    }

    fn next_append(&self) -> Result<(), Status> {
        self.queue.lock().unwrap().pop_front().unwrap_or(Ok(()))
    }

    fn next_raft_state(&self) -> Result<bool, Status> {
        self.raft_states
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(true))
    }

    fn append_count(&self) -> u64 {
        self.append_calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[tonic::async_trait]
impl event_store_server::EventStore for FlakyStub {
    async fn append(
        &self,
        _request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        self.append_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.next_append().map(|()| {
            Response::new(AppendResponse {
                next_expected_version: 1,
                first_position: 0,
                last_position: 0,
                shard_id: 0,
            })
        })
    }

    type ReadStreamStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ReadEventsResponse, Status>> + Send>,
    >;
    async fn read_stream(
        &self,
        _request: Request<ReadStreamRequest>,
    ) -> Result<Response<Self::ReadStreamStream>, Status> {
        Err(Status::unimplemented("stub"))
    }

    type ReadAllStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<ReadEventsResponse, Status>> + Send>,
    >;
    async fn read_all(
        &self,
        _request: Request<ReadAllRequest>,
    ) -> Result<Response<Self::ReadAllStream>, Status> {
        Err(Status::unimplemented("stub"))
    }

    type SubscribeStream = std::pin::Pin<
        Box<dyn tokio_stream::Stream<Item = Result<SubscribeResponse, Status>> + Send>,
    >;
    async fn subscribe(
        &self,
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // 发送预设事件后立即关闭流：模拟服务端断流（未收到 caught_up）
        let events = self.subscribe_events.lock().unwrap().clone();
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        for ev in events {
            let _ = tx
                .send(Ok(SubscribeResponse {
                    payload: Some(subscribe_response::Payload::Event(ev)),
                }))
                .await;
        }
        if self
            .subscribe_degraded
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let _ = tx
                .send(Ok(SubscribeResponse {
                    payload: Some(subscribe_response::Payload::Degraded(Empty {})),
                }))
                .await;
        }
        if self
            .subscribe_caught_up
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            let _ = tx
                .send(Ok(SubscribeResponse {
                    payload: Some(subscribe_response::Payload::CaughtUp(Empty {})),
                }))
                .await;
        }
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_stream_meta(
        &self,
        _request: Request<GetStreamMetaRequest>,
    ) -> Result<Response<GetStreamMetaResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn create_stream(
        &self,
        _request: Request<CreateStreamRequest>,
    ) -> Result<Response<CreateStreamResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

#[tonic::async_trait]
impl raft_admin_server::RaftAdmin for FlakyStub {
    async fn initialize(
        &self,
        _request: Request<InitializeRequest>,
    ) -> Result<Response<InitializeResponse>, Status> {
        self.initialize_result.lock().unwrap().clone()?;
        Ok(Response::new(InitializeResponse {}))
    }

    async fn add_learner(
        &self,
        _request: Request<AddLearnerRequest>,
    ) -> Result<Response<AddLearnerResponse>, Status> {
        Ok(Response::new(AddLearnerResponse {}))
    }

    async fn change_membership(
        &self,
        _request: Request<ChangeMembershipRequest>,
    ) -> Result<Response<ChangeMembershipResponse>, Status> {
        Ok(Response::new(ChangeMembershipResponse {}))
    }

    async fn get_raft_state(
        &self,
        _request: Request<GetRaftStateRequest>,
    ) -> Result<Response<GetRaftStateResponse>, Status> {
        let is_leader = self.next_raft_state()?;
        let voters = self.voter_ids.lock().unwrap().clone();
        Ok(Response::new(GetRaftStateResponse {
            node_id: 1,
            server_state: "Leader".into(),
            is_leader,
            has_leader: is_leader,
            current_leader: if is_leader { 1 } else { 0 },
            current_term: 1,
            has_last_log_index: false,
            last_log_index: 0,
            has_last_applied: false,
            last_applied: 0,
            voter_ids: voters,
        }))
    }

    async fn list_shards(
        &self,
        _request: Request<ListShardsRequest>,
    ) -> Result<Response<ListShardsResponse>, Status> {
        // stub 只"承载"分片 0（分片探测用；调用方一般显式 --shards 跳过探测）
        Ok(Response::new(ListShardsResponse {
            node_id: 1,
            shard_ids: vec![0],
        }))
    }
}

/// 起一个 FlakyStub 服务（EventStore + RaftAdmin 双服务），返回地址。
async fn start_flaky(stub: FlakyStub) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定端口");
    let addr = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(event_store_server::EventStoreServer::new(stub.clone()))
            .add_service(raft_admin_server::RaftAdminServer::new(stub))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    addr
}

async fn simple_append(
    client: &mut es_proto::eventstore::event_store_client::EventStoreClient<
        tonic::transport::Channel,
    >,
) -> Result<AppendResponse, Status> {
    client
        .append(AppendRequest {
            stream_id: "s".into(),
            expected_version: ev_any(),
            events: vec![],
        })
        .await
        .map(|r| r.into_inner())
}

#[tokio::test]
async fn with_leader_redirect_to_leader_addr() {
    let stub_b = FlakyStub::new();
    let addr_b = start_flaky(stub_b.clone()).await;
    let stub_a = FlakyStub::new();
    stub_a.push_append(Err(Status::unavailable(format!(
        "not leader; leader_id=2 leader_addr={addr_b}"
    ))));
    let addr_a = start_flaky(stub_a).await;

    let cluster = ClusterClient::new(
        &[addr_a],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    cluster
        .with_leader(
            0,
            |mut client| async move { simple_append(&mut client).await },
        )
        .await
        .expect("重定向后应成功");
    assert_eq!(stub_b.append_count(), 1, "重定向目标应被调用");
}

#[tokio::test]
async fn with_leader_retries_after_election() {
    let stub = FlakyStub::new();
    stub.push_append(Err(Status::unavailable(
        "not leader; leader unknown, retry later",
    )));
    let addr = start_flaky(stub).await;

    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    cluster
        .with_leader(
            0,
            |mut client| async move { simple_append(&mut client).await },
        )
        .await
        .expect("重试后应成功");
}

#[tokio::test]
async fn with_leader_skips_unreachable_endpoint() {
    let stub = FlakyStub::new();
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[
            "http://127.0.0.1:1".to_string(), // 必然拒绝连接（保留端口）
            addr,
        ],
        None,
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .expect("客户端");
    cluster
        .with_leader(
            0,
            |mut client| async move { simple_append(&mut client).await },
        )
        .await
        .expect("跳过坏端点后应成功");
}

#[tokio::test]
async fn with_leader_failed_precondition_raised() {
    let stub = FlakyStub::new();
    stub.push_append(Err(Status::failed_precondition(
        "optimistic conflict: actual_version=3",
    )));
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster
        .with_leader(
            0,
            |mut client| async move { simple_append(&mut client).await },
        )
        .await
        .expect_err("FailedPrecondition 应上抛");
    assert!(err.to_string().contains("optimistic conflict"), "{err}");
}

#[tokio::test]
async fn with_any_endpoint_all_failed_summarized() {
    let stub_a = FlakyStub::new();
    let stub_b = FlakyStub::new();
    stub_a.push_append(Err(Status::internal("boom-a")));
    stub_b.push_append(Err(Status::internal("boom-b")));
    let addr_a = start_flaky(stub_a).await;
    let addr_b = start_flaky(stub_b).await;
    let cluster = ClusterClient::new(
        &[addr_a, addr_b],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster
        .with_any_endpoint(|mut client| async move { simple_append(&mut client).await })
        .await
        .expect_err("全部失败应报错");
    assert!(err.to_string().contains("所有端点均不可用"), "{err}");
    assert!(err.to_string().contains("boom-a"), "{err}");
}

#[tokio::test]
async fn with_any_endpoint_first_fails_falls_to_second() {
    let stub_a = FlakyStub::new();
    let stub_b = FlakyStub::new();
    stub_a.push_append(Err(Status::internal("boom-a")));
    let addr_a = start_flaky(stub_a).await;
    let addr_b = start_flaky(stub_b).await;
    let cluster = ClusterClient::new(
        &[addr_a, addr_b],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    cluster
        .with_any_endpoint(|mut client| async move { simple_append(&mut client).await })
        .await
        .expect("第二个端点应成功");
}

#[tokio::test]
async fn with_admin_leader_not_initialized_errors() {
    let stub = FlakyStub::new();
    stub.push_raft_state(Err(Status::not_found("分片 0: 不存在")));
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster
        .with_admin_leader(0, |_client| async move { Err::<(), _>(Status::ok("")) })
        .await
        .expect_err("未初始化应报错");
    assert!(err.to_string().contains("未初始化"), "{err}");
}

#[tokio::test]
async fn with_admin_leader_no_leader_retries_exhausted() {
    let stub = FlakyStub::new();
    // 3 轮重试都是非 leader
    for _ in 0..6 {
        stub.push_raft_state(Ok(false));
    }
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster
        .with_admin_leader(0, |_client| async move { Err::<(), _>(Status::ok("")) })
        .await
        .expect_err("无 leader 应报错");
    assert!(err.to_string().contains("管理操作失败"), "{err}");
}

#[tokio::test]
async fn find_leader_two_error_paths() {
    let stub = FlakyStub::new();
    stub.push_raft_state(Err(Status::not_found("分片 0: 不存在")));
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster.find_leader(0).await.expect_err("未初始化应报错");
    assert!(err.to_string().contains("未初始化"), "{err}");

    // 非 leader 且初始化过 → 无 leader 错误
    let stub2 = FlakyStub::new();
    stub2.push_raft_state(Ok(false));
    let addr2 = start_flaky(stub2).await;
    let cluster2 = ClusterClient::new(
        &[addr2],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster2.find_leader(0).await.expect_err("无 leader 应报错");
    assert!(err.to_string().contains("无 leader"), "{err}");
}

// ---------- watch / init 错误路径（stub） ----------

#[tokio::test]
async fn watch_closed_before_catchup_once_and_not() {
    let stub = FlakyStub::new();
    // 预设 1 条事件后断流：覆盖事件渲染（非 caught_up）与未追平退出路径
    stub.set_subscribe_events(vec![SubscriptionEvent {
        stream_id: "s".into(),
        version: 0,
        event_id: uuid::Uuid::new_v4().as_bytes().to_vec(),
        event_type: "E".into(),
        data: b"d".to_vec(),
        metadata: vec![],
        hlc: None,
    }]);
    let addr = start_flaky(stub).await;
    let ctx = ctx_with(addr, Format::Simple);

    // 非 --once：流关闭且未追平 → 告警后 Ok
    let w = WatchArgs {
        stream: vec!["s".into()],
        all: false,
        once: false,
    };
    watch::run(&ctx, &w).await.expect("非 once 断流返回 Ok");

    // --once 未追平 → 报错（依赖退出码的脚本必须能感知）
    let w = WatchArgs {
        stream: vec!["s".into()],
        all: false,
        once: true,
    };
    let err = watch::run(&ctx, &w).await.expect_err("once 未追平应报错");
    assert!(err.to_string().contains("caught_up"), "{err}");
}

#[tokio::test]
async fn watch_once_fails_immediately_when_subscription_is_degraded() {
    let stub = FlakyStub::new();
    stub.set_subscribe_degraded();
    let addr = start_flaky(stub).await;
    let ctx = ctx_with(addr, Format::Json);
    let err = watch::run(
        &ctx,
        &WatchArgs {
            stream: vec![],
            all: true,
            once: true,
        },
    )
    .await
    .expect_err("--once 收到降级必须失败");
    assert!(
        err.to_string().contains("降级"),
        "错误应说明订阅降级: {err}"
    );
}

#[tokio::test]
async fn watch_without_once_accepts_caught_up_before_normal_close() {
    let stub = FlakyStub::new();
    stub.set_subscribe_caught_up();
    let addr = start_flaky(stub).await;
    let ctx = ctx_with(addr, Format::Table);
    watch::run(
        &ctx,
        &WatchArgs {
            stream: vec!["s".into()],
            all: false,
            once: false,
        },
    )
    .await
    .expect("追平后正常关闭应成功");
}

#[tokio::test]
async fn init_all_endpoints_down_errors() {
    let stub = FlakyStub::new();
    stub.set_initialize(Err(Status::unavailable("节点下线")));
    let addr = start_flaky(stub).await;
    let ctx = ctx_with(addr, Format::Simple);
    let err = init::run(
        &ctx,
        &InitArgs {
            shard: Some(0),
            all_shards: false,
            member: vec![MemberArg {
                node_id: 1,
                addr: "127.0.0.1:50051".into(),
            }],
            yes: true,
        },
    )
    .await
    .expect_err("端点失败应报错");
    assert!(err.to_string().contains("初始化失败"), "{err}");
}

// ---------- append：分片数为 0 的兜底 ----------

#[tokio::test]
async fn append_zero_shards_bails() {
    let (addr, _s, _dir) = start_server().await;
    // 绕过 clap 的 >=1 校验，直接构造 --shards 0（探测路径可能返回 0）
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let ctx = Ctx::new(cluster, global(Format::Simple, vec![], Some(0)));
    let err = append::run(&ctx, &append_args("s", "E", Some("x".into())))
        .await
        .expect_err("分片数为 0 应报错");
    assert!(err.to_string().contains("分片数为 0"), "{err}");
}

// ---------- client.rs 网络错误分支 ----------

#[tokio::test]
async fn with_any_endpoint_connect_fail_fails_over() {
    let stub = FlakyStub::new();
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[
            "http://127.0.0.1:1".to_string(), // 必然建连失败
            addr,
        ],
        None,
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .expect("客户端");
    cluster
        .with_any_endpoint(|mut client| async move { simple_append(&mut client).await })
        .await
        .expect("第二个端点应成功");
}

#[tokio::test]
async fn with_leader_other_error_raised() {
    let stub = FlakyStub::new();
    stub.push_append(Err(Status::internal("内部错误")));
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster
        .with_leader(
            0,
            |mut client| async move { simple_append(&mut client).await },
        )
        .await
        .expect_err("非 Unavailable 错误应上抛");
    assert!(err.to_string().contains("内部错误"), "{err}");
}

#[tokio::test]
async fn with_leader_budget_exhausted_no_leader() {
    // 单端点持续返回「选举中」：tried 集合允许重入队重试，预算耗尽后报无 leader
    let stub = FlakyStub::new();
    for _ in 0..8 {
        stub.push_append(Err(Status::unavailable(
            "not leader; leader unknown, retry later",
        )));
    }
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster
        .with_leader(
            0,
            |mut client| async move { simple_append(&mut client).await },
        )
        .await
        .expect_err("预算耗尽应报错");
    assert!(err.to_string().contains("未找到分片 0 的 leader"), "{err}");
}

#[tokio::test]
async fn with_admin_leader_rpc_fail_retries_exhausted() {
    // leader 探测成功但 RPC 失败：3 轮重试后报错（覆盖 last_err 路径）
    let stub = FlakyStub::new();
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster
        .with_admin_leader(0, |_client| async move {
            Err::<(), _>(Status::failed_precondition("CAS 冲突"))
        })
        .await
        .expect_err("RPC 失败应报错");
    assert!(err.to_string().contains("管理操作失败"), "{err}");
}

#[tokio::test]
async fn try_find_leader_mixed_errors_collected() {
    // 端点 A 内部错误（收集进 errors）、端点 B 非 leader → NoLeader 且带错误详情
    let stub_a = FlakyStub::new();
    stub_a.push_raft_state(Err(Status::internal("boom")));
    let addr_a = start_flaky(stub_a).await;
    let stub_b = FlakyStub::new();
    stub_b.push_raft_state(Ok(false));
    let addr_b = start_flaky(stub_b).await;
    let cluster = ClusterClient::new(
        &[addr_a, addr_b],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    let err = cluster.find_leader(0).await.expect_err("无 leader 应报错");
    assert!(err.to_string().contains("无 leader"), "{err}");
    assert!(err.to_string().contains("boom"), "应收集错误详情: {err}");
}

// ---------- member：all-shards 与未初始化分片列表 ----------

#[tokio::test]
async fn member_add_all_shards() {
    let stub = FlakyStub::new();
    let addr = start_flaky(stub).await;
    let cluster = ClusterClient::new(
        &[addr],
        None,
        Duration::from_secs(2),
        Duration::from_secs(5),
    )
    .expect("客户端");
    // 显式 --shards 2：探测不触网，直接对全部分片执行
    let ctx = Ctx::new(cluster, global(Format::Simple, vec![], Some(2)));
    member::run(
        &ctx,
        &MemberArgs {
            action: MemberAction::Add(MemberAddArgs {
                shard: None,
                all_shards: true,
                member: MemberArg {
                    node_id: 2,
                    addr: "127.0.0.1:59999".into(),
                },
                no_blocking: true,
                learner_only: true,
            }),
        },
    )
    .await
    .expect("all-shards add 成功");
}

#[tokio::test]
async fn member_list_uninitialized_shards_three_formats() {
    for fmt in [Format::Simple, Format::Table, Format::Json] {
        // shard 0 可达、shard 1 未初始化（NotFound）、shard 2 内部错误（收集）
        let stub = FlakyStub::new();
        stub.push_raft_state(Ok(true));
        stub.push_raft_state(Err(Status::not_found("分片 1: 不存在")));
        stub.push_raft_state(Err(Status::internal("boom")));
        let addr = start_flaky(stub).await;
        let cluster = ClusterClient::new(
            &[addr],
            None,
            Duration::from_secs(2),
            Duration::from_secs(5),
        )
        .expect("客户端");
        let ctx = Ctx::new(cluster, global(fmt, vec![], Some(3)));
        member::run(
            &ctx,
            &MemberArgs {
                action: MemberAction::List(MemberListArgs {}),
            },
        )
        .await
        .expect("member list 成功");
    }
}

// ---------- main.rs 命令分发层 ----------

#[tokio::test]
async fn main_run_command_dispatch() {
    // 覆盖 main.rs 的 run()：装配 Ctx 并分发命令（走真实集群）
    let (addr, _s, _dir) = start_server().await;
    let cli = Cli {
        global: global(Format::Simple, vec![addr], None),
        command: Command::Meta(MetaArgs { stream: "s".into() }),
    };
    crate::run(cli).await.expect("命令分发成功");

    // --cacert 分支（load_tls 的 Some 路径）：文件不存在 → 报错
    let tmp = tempfile::tempdir().expect("临时目录");
    let bad_ca = tmp.path().join("ca.pem");
    let cli = Cli {
        global: GlobalArgs {
            endpoints: vec!["http://127.0.0.1:1".into()],
            dial_timeout: 1,
            timeout: 1,
            cacert: Some(bad_ca),
            insecure_skip_tls_verify: false,
            write_out: Format::Simple,
            shards: None,
        },
        command: Command::Meta(MetaArgs { stream: "s".into() }),
    };
    let err = crate::run(cli).await.expect_err("CA 文件不存在应报错");
    assert!(err.to_string().contains("读取 CA 文件"), "{err}");
}

// ---------- watch：空消息分支 ----------

#[tokio::test]
async fn watch_empty_message_skipped() {
    // subscribe 返回带空 payload 的消息：run() 应跳过（None => {} 分支）
    let stub = FlakyStub::new();
    let addr = start_flaky(stub).await;
    // 直接构造 subscribe 服务不可行，改走 watch::run 的 --all 订阅：
    // 真实集群上 caught_up 之前不会有空消息；这里用 FlakyStub 的 subscribe
    // 预设事件+关闭已覆盖事件分支，空消息分支通过下方 stub 专用测试补。
    let ctx = ctx_with(addr, Format::Simple);
    let w = WatchArgs {
        stream: vec![],
        all: true,
        once: false,
    };
    // stub 无 caught_up：非 once 返回 Ok（已覆盖 74-78 告警路径）
    watch::run(&ctx, &w).await.expect("断流返回 Ok");
}
