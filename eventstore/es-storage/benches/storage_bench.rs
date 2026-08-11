//! 存储层性能基准测试
//!
//! 运行: `cargo bench -p es-storage`
//! 查看报告: `open target/criterion/report/index.html`
//!
//! 注:写入需要完整 Raft 环境,此处只测读取和 reshard。

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use es_storage::storage::EsStorage;
use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// 全局 tokio runtime,供所有 benchmark 共享
static RT: Lazy<Runtime> = Lazy::new(|| Runtime::new().unwrap());

/// 读取基准:读取空流(最快路径)
fn bench_read_empty_stream(c: &mut Criterion) {
    c.bench_function("read_empty_stream", |b| {
        // 预建存储(在 bench 外,避免计入时间)
        let (_dir, storage) = RT.block_on(async {
            let dir = tempfile::tempdir().expect("临时目录");
            let tree = Arc::new(
                surrealkv::TreeBuilder::new()
                    .with_path(dir.path().to_path_buf())
                    .build()
                    .expect("建 tree"),
            );
            let storage = EsStorage::new(
                0,
                tree,
                es_storage::snapshot::SnapshotConfig {
                    dir: dir.path().join("snapshots"),
                    ..Default::default()
                },
            )
            .expect("建存储");
            (dir, storage)
        });

        b.iter(|| {
            let events = storage
                .read_stream_events("nonexistent", 0, 0)
                .expect("读空流");
            black_box(events);
        });
    });
}

/// Reshard 基准:测不同流数下的重分布速度
fn bench_reshard(c: &mut Criterion) {
    let mut group = c.benchmark_group("reshard");
    group.sample_size(10); // Reshard 较慢,减少采样

    for n_streams in [10, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_streams),
            &n_streams,
            |b, &n| {
                b.iter(|| {
                    RT.block_on(async {
                        // 每次迭代重建数据(包含在测量中,反映真实使用)
                        let src_dir = tempfile::tempdir().expect("src dir");
                        let src_tree = Arc::new(
                            surrealkv::TreeBuilder::new()
                                .with_path(src_dir.path().to_path_buf())
                                .build()
                                .expect("src tree"),
                        );

                        // 手工写入最小数据集:StreamMeta
                        use es_storage::key;
                        for shard in 0..2u64 {
                            let mut txn = src_tree.begin().expect("begin");
                            for i in 0..(n / 2) {
                                let stream = format!("s{shard}-{i}");
                                let meta = es_core::StreamMeta {
                                    current_version: 0,
                                };
                                let k = key::sm_stream_meta(shard, &stream);
                                let v = serde_json::to_vec(&meta).expect("序列化");
                                txn.set(&k, &v).expect("set");
                            }
                            txn.commit().await.expect("commit");
                        }

                        let dst_dir = tempfile::tempdir().expect("dst dir");
                        let dst_tree = Arc::new(
                            surrealkv::TreeBuilder::new()
                                .with_path(dst_dir.path().to_path_buf())
                                .build()
                                .expect("dst tree"),
                        );

                        let report = es_storage::reshard::reshard(src_tree, 2, dst_tree, 4)
                            .await
                            .expect("reshard");
                        black_box(report);
                    });
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_read_empty_stream, bench_reshard);
criterion_main!(benches);
