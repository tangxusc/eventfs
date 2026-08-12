//! 存储层性能基准测试
//!
//! 运行: `cargo bench -p es-storage`
//! 查看报告: `open target/criterion/report/index.html`
//!
//! 注:写入需要完整 Raft 环境,此处只测读取与快照恢复。

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


criterion_group!(
    benches,
    bench_read_empty_stream,
);
criterion_main!(benches);
