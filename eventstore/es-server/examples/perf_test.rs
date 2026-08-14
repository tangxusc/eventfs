//! 端到端性能压测客户端（3 节点 TLS 集群）。
//!
//! 每规格（事件大小）依次测量四条路径：
//!   1. 单条 append 延迟抽测（p50/p95/p99）
//!   2. 批量 append 写入吞吐（events/s、MB/s）
//!   3. read_stream 全量分页读吞吐 + 页延迟分布
//!   4. subscribe 从 0 追平（catch-up）吞吐
//!
//! 数据量按「每规格固定总量 50MB」设计，保证不同事件大小间吞吐可比。
//! 事件 payload 用确定性伪随机字节（不可压缩），贴近真实负载。
//!
//! 用法（由 scripts/perf_test.sh 编排调用）：
//! ```bash
//! cargo run --release -p es-server --example perf_test -- \
//!   --addrs https://127.0.0.1:50051,https://127.0.0.1:50052,https://127.0.0.1:50053 \
//!   --ca /path/to/all.crt --output /path/to/result.json
//! ```

use std::time::{Duration, Instant};

use clap::Parser;
use es_client::{
    Direction, EventBuilder, EventStoreClient, ExpectedVersionBuilder, SubscribeTarget,
    TlsClientConfig,
};
use futures::StreamExt;

/// 单条 append 延迟抽测次数（每规格）
const SINGLE_SAMPLE: usize = 200;
/// 每规格固定总量（字节）：吞吐对比的公平基准
const TOTAL_BYTES_PER_SIZE: usize = 50 * 1024 * 1024;
/// 全量读分页大小上限（条/页）。服务端把 max_count 条事件装入单条
/// gRPC 消息（8MB 传输上限），实际页大小按规格动态计算（见 run_size）
const PAGE_MAX: u64 = 1000;
/// 批量 append 目标字节数:低于 7MiB 服务端上限,留编码膨胀余量
const BATCH_TARGET_BYTES: usize = 6 * 1024 * 1024;
/// 事件大小下限（字节）：过小则事件总数爆炸——1 字节规格会生成 5240
/// 万条事件，订阅 catch-up 一次物化全流直接打爆节点内存
const MIN_SIZE_BYTES: usize = 256;
/// 事件大小上限（字节）：低于服务端单事件上限（es-core limits，1MiB），
/// 留编码头余量，超过会被服务端按编码后字节权威拒绝
const MAX_SIZE_BYTES: usize = es_core::limits::MAX_EVENT_PAYLOAD_BYTES - 1024;
/// 写路径重试上限：冷启动各分片选举收敛时序不一（warmup 只验证了
/// 1 个分片），写入命中未收敛分片会返回 NotLeader。200ms × 100 = 20s
/// 兜底收敛，超出则 panic（集群真不可用）
const WRITE_RETRY: u32 = 100;
const WRITE_RETRY_DELAY: Duration = Duration::from_millis(200);

/// 确定性伪随机字节生成器（xorshift64），生成不可压缩 payload
struct XorShift(u64);

impl XorShift {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next_u64().to_le_bytes();
            let n = chunk.len().min(8);
            chunk.copy_from_slice(&v[..n]);
        }
    }
}

/// 分位数：p 为 0..=1，取排序后线性插值
fn percentile(sorted: &[f64], p: f64) -> f64 {
    assert!(!sorted.is_empty());
    let pos = p * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = pos.ceil() as usize;
    if lo == hi {
        sorted[lo]
    } else {
        sorted[lo] * (hi as f64 - pos) + sorted[hi] * (pos - lo as f64)
    }
}

fn summarize(latencies: &[Duration]) -> (f64, f64, f64) {
    let mut sorted: Vec<f64> = latencies.iter().map(|d| d.as_secs_f64() * 1000.0).collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (
        percentile(&sorted, 0.50),
        percentile(&sorted, 0.95),
        percentile(&sorted, 0.99),
    )
}

#[derive(Parser, Debug)]
#[command(about = "EventFS 端到端性能压测客户端")]
struct Args {
    /// 集群节点地址（逗号分隔，https）
    #[arg(
        long,
        default_value = "https://127.0.0.1:50051,https://127.0.0.1:50052,https://127.0.0.1:50053"
    )]
    addrs: String,
    /// CA 证书 PEM 路径（严格校验对端证书链）
    #[arg(long)]
    ca: String,
    /// 事件大小列表（字节，逗号分隔）
    #[arg(long, default_value = "1024,10240,102400")]
    sizes: String,
    /// 结果 JSON 输出路径（可选，缺省仅打印表格）
    #[arg(long)]
    output: Option<String>,
    /// 流名前缀
    #[arg(long, default_value = "perf")]
    stream_prefix: String,
    /// 覆盖批量写入批大小(缺省按 6MiB/size 计算)
    #[arg(long)]
    batch_size: Option<usize>,
}

#[derive(serde::Serialize, Default)]
struct SingleAppend {
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    events_per_s: f64,
}

#[derive(serde::Serialize, Default)]
struct BatchAppend {
    batches: usize,
    elapsed_ms: f64,
    events_per_s: f64,
    mb_per_s: f64,
}

#[derive(serde::Serialize, Default)]
struct Read {
    pages: usize,
    elapsed_ms: f64,
    events_per_s: f64,
    mb_per_s: f64,
    page_p50_ms: f64,
    page_p95_ms: f64,
    page_p99_ms: f64,
}

#[derive(serde::Serialize, Default)]
struct Subscribe {
    events: usize,
    elapsed_ms: f64,
    events_per_s: f64,
    mb_per_s: f64,
}

#[derive(serde::Serialize)]
struct SizeResult {
    event_bytes: usize,
    total_events: usize,
    append_single: SingleAppend,
    append_batch: BatchAppend,
    read: Read,
    subscribe: Subscribe,
}

/// 生成一条事件：event_type 按规格区分，data 为不可压缩伪随机字节
fn make_event(mut rng: XorShift, event_type: &str, size: usize) -> es_client::NewEvent {
    let mut data = vec![0u8; size];
    rng.fill(&mut data);
    EventBuilder::new(event_type).data(data).build()
}

/// 单条 append 延迟抽测：逐条追加，返回延迟序列
async fn sample_single_append(
    client: &mut EventStoreClient,
    stream: &str,
    event_type: &str,
    size: usize,
) -> Vec<Duration> {
    let mut latencies = Vec::with_capacity(SINGLE_SAMPLE);
    for i in 0..SINGLE_SAMPLE {
        let t = Instant::now();
        // 选举/leader 转移等临时错误退避重试（同 batch_write），重试
        // 等待计入延迟——集群未收敛阶段测出的延迟本就含等待
        for attempt in 0..=WRITE_RETRY {
            let ev = make_event(XorShift(0x1234 + i as u64), event_type, size);
            match client
                .append(stream.to_string(), ExpectedVersionBuilder::any(), vec![ev])
                .await
            {
                Ok(_) => break,
                Err(_) if attempt < WRITE_RETRY => {
                    tokio::time::sleep(WRITE_RETRY_DELAY).await;
                }
                Err(e) => panic!("单条 append 失败（{WRITE_RETRY} 次重试后）: {e}"),
            }
        }
        latencies.push(t.elapsed());
    }
    latencies
}

/// 批量写入：按 8MB 留余的批大小追加至固定总量，返回批次耗时序列
async fn batch_write(
    client: &mut EventStoreClient,
    stream: &str,
    event_type: &str,
    size: usize,
    total_events: usize,
    batch_override: Option<usize>,
) -> (usize, Vec<Duration>) {
    let batch = batch_override.unwrap_or_else(|| (BATCH_TARGET_BYTES / size).max(1).min(500));
    let mut latencies = Vec::new();
    let mut written = 0usize;
    while written < total_events {
        let n = batch.min(total_events - written);
        let t = Instant::now();
        // 选举/leader 转移等临时错误退避重试：客户端内部预算有限
        // （8 次 + 200ms 退避 ≈ 1.3s），冷启动各分片收敛时序不一，
        // 单次调用可能耗尽预算，外层再兜底 20s
        for attempt in 0..=WRITE_RETRY {
            let events: Vec<_> = (0..n)
                .map(|i| {
                    make_event(
                        XorShift(0x9000 + written as u64 + i as u64),
                        event_type,
                        size,
                    )
                })
                .collect();
            match client
                .append(stream.to_string(), ExpectedVersionBuilder::any(), events)
                .await
            {
                Ok(_) => break,
                Err(_) if attempt < WRITE_RETRY => {
                    tokio::time::sleep(WRITE_RETRY_DELAY).await;
                }
                Err(e) => panic!("批量 append 失败（{WRITE_RETRY} 次重试后）: {e}"),
            }
        }
        latencies.push(t.elapsed());
        written += n;
    }
    (written, latencies)
}

/// 全量分页读：从 version 0 翻页读完整个流（单次尝试）。
async fn read_all_pages_once(
    client: &mut EventStoreClient,
    stream: &str,
    page_size: u64,
) -> (usize, Vec<Duration>, u64) {
    let mut total = 0usize;
    let mut page_times = Vec::new();
    let mut from = 0u64;
    let mut total_bytes = 0u64;
    loop {
        let t = Instant::now();
        let events = client
            .read_stream(stream.to_string(), from, page_size, Direction::Forward)
            .await
            .expect("读取失败");
        page_times.push(t.elapsed());
        if events.is_empty() {
            break;
        }
        let page_bytes: u64 = events.iter().map(|e| e.data.len() as u64).sum();
        total_bytes += page_bytes;
        total += events.len();
        from += events.len() as u64;
    }
    (total, page_times, total_bytes)
}

/// 全量分页读：从 version 0 翻页读完整个流，断言读到预期条数。
///
/// 读可能命中尚未 apply 最后一批已提交事件的 follower（读无线性化
/// 屏障、无 leader 钉扎），此时读到旧前缀、提前空页。按 `expected`
/// 断言，不足则退避重试（follower 追平很快），重试耗尽仍不足则
/// panic——计数静默偏少会让吞吐数据失真。
async fn read_all_pages(
    client: &mut EventStoreClient,
    stream: &str,
    page_size: u64,
    expected: usize,
) -> (usize, Vec<Duration>, u64) {
    let mut last = (0, Vec::new(), 0);
    for _ in 0..3 {
        last = read_all_pages_once(client, stream, page_size).await;
        if last.0 == expected {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    panic!(
        "读计数 {} 与预期 {} 不符（3 次重试后）：可能命中未追平的 follower",
        last.0, expected
    );
}

/// 订阅 catch-up：从 0 追平整个流（单次尝试）。
async fn subscribe_catchup_once(
    client: &mut EventStoreClient,
    stream: &str,
) -> (usize, Duration, u64) {
    let t = Instant::now();
    let mut stream = client
        .subscribe(SubscribeTarget::Streams(vec![stream.to_string()]))
        .await
        .expect("订阅失败");
    let mut count = 0usize;
    let mut total_bytes = 0u64;
    while let Some(item) = stream.next().await {
        let resp = item.expect("订阅流错误");
        match resp.payload {
            Some(es_client::subscribe_response::Payload::Event(ev)) => {
                total_bytes += ev.data.len() as u64;
                count += 1;
            }
            Some(es_client::subscribe_response::Payload::CaughtUp(_)) => break,
            Some(es_client::subscribe_response::Payload::Degraded(_)) => {
                panic!("性能订阅不应降级")
            }
            None => {}
        }
    }
    (count, t.elapsed(), total_bytes)
}

/// 订阅 catch-up：从 0 追平整个流，断言追平条数 == `expected`。
///
/// 与 read_all_pages 同理：可能命中未追平的 follower（CaughtUp 在旧
/// 前缀处即到达），断言不足则退避重试，重试耗尽仍不足则 panic。
async fn subscribe_catchup(
    client: &mut EventStoreClient,
    stream: &str,
    expected: usize,
) -> (usize, Duration, u64) {
    let mut last = (0, Duration::ZERO, 0);
    for _ in 0..3 {
        last = subscribe_catchup_once(client, stream).await;
        if last.0 == expected {
            return last;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    panic!(
        "订阅追平计数 {} 与预期 {} 不符（3 次重试后）：可能命中未追平的 follower",
        last.0, expected
    );
}

async fn run_size(
    client: &mut EventStoreClient,
    size: usize,
    stream: &str,
    batch_override: Option<usize>,
) -> SizeResult {
    let event_type = format!("perf-{size}b");
    let total_events = (TOTAL_BYTES_PER_SIZE / size).max(1);

    // 1. 单条 append 延迟。抽样写到独立流（{stream}-single），不污染
    //    批量流——混合后读/订阅从 0 读出 200 条抽样事件，计数与
    //    总量不符、吞吐虚高，规格间不可比
    let single_stream = format!("{stream}-single");
    let single = sample_single_append(client, &single_stream, &event_type, size).await;
    let single_n = single.len();
    let single_elapsed: Duration = single.iter().sum();
    let (s_p50, s_p95, s_p99) = summarize(&single);

    // 2. 批量写入至固定总量
    let (written, batch_times) = batch_write(
        client,
        stream,
        &event_type,
        size,
        total_events,
        batch_override,
    )
    .await;
    let batch_elapsed: Duration = batch_times.iter().sum();
    let total_bytes = (written as u64) * (size as u64);

    // 3. 全量分页读。页大小按规格动态计算:单条 gRPC 消息 8MB 上限,
    //    页内事件总大小留半(4MB)保证编码头与传输余量
    let page_size = (4u64 * 1024 * 1024 / size as u64).max(1).min(PAGE_MAX);
    let (read_n, page_times, read_bytes) = read_all_pages(client, stream, page_size, written).await;
    let read_elapsed: Duration = page_times.iter().sum();
    let (r_p50, r_p95, r_p99) = summarize(&page_times);

    // 4. 订阅 catch-up
    let (sub_n, sub_elapsed, sub_bytes) = subscribe_catchup(client, stream, written).await;

    let ms = |d: Duration| d.as_secs_f64() * 1000.0;
    let rate = |elapsed: Duration, n: u64| -> f64 { n as f64 / elapsed.as_secs_f64() };

    SizeResult {
        event_bytes: size,
        total_events: written,
        append_single: SingleAppend {
            p50_ms: s_p50,
            p95_ms: s_p95,
            p99_ms: s_p99,
            // 除数用单条抽测自身耗时：旧实现错用批量阶段耗时，1KB 规格
            // 下数值失真约 13 倍（200 条耗时 0.09s 却除以 1.2s 批量耗时）
            events_per_s: rate(single_elapsed, single_n as u64),
        },
        append_batch: BatchAppend {
            batches: batch_times.len(),
            elapsed_ms: ms(batch_elapsed),
            events_per_s: rate(batch_elapsed, written as u64),
            mb_per_s: total_bytes as f64 / batch_elapsed.as_secs_f64() / 1024.0 / 1024.0,
        },
        read: Read {
            pages: page_times.len(),
            elapsed_ms: ms(read_elapsed),
            events_per_s: rate(read_elapsed, read_n as u64),
            mb_per_s: read_bytes as f64 / read_elapsed.as_secs_f64() / 1024.0 / 1024.0,
            page_p50_ms: r_p50,
            page_p95_ms: r_p95,
            page_p99_ms: r_p99,
        },
        subscribe: Subscribe {
            events: sub_n,
            elapsed_ms: ms(sub_elapsed),
            events_per_s: rate(sub_elapsed, sub_n as u64),
            mb_per_s: sub_bytes as f64 / sub_elapsed.as_secs_f64() / 1024.0 / 1024.0,
        },
    }
}

fn print_table(results: &[SizeResult]) {
    println!();
    println!("=== 端到端性能压测结果（3 节点 TLS，单客户端） ===");
    println!(
        "{:<10} | {:<10} | {:<26} | {:<30} | {:<30} | {:<30}",
        "事件大小",
        "事件数",
        "单条 append p50/p95/p99(ms)",
        "批量写入 条/s | MB/s",
        "全量读 条/s | MB/s",
        "订阅追平 条/s | MB/s"
    );
    println!(
        "{:-<10}-+-{:-<10}-+-{:-<26}-+-{:-<30}-+-{:-<30}-+-{:-<30}",
        "", "", "", "", "", ""
    );
    for r in results {
        println!(
            "{:<10} | {:<10} | p50={:.2} p95={:.2} p99={:.2} | {:.0} | {:.1} | {:.0} | {:.1} | {:.0} | {:.1}",
            format!("{} B", r.event_bytes),
            r.total_events,
            r.append_single.p50_ms,
            r.append_single.p95_ms,
            r.append_single.p99_ms,
            r.append_batch.events_per_s,
            r.append_batch.mb_per_s,
            r.read.events_per_s,
            r.read.mb_per_s,
            r.subscribe.events_per_s,
            r.subscribe.mb_per_s,
        );
        println!(
            "{:<10}   {:<10}   批次耗时 {:.0} ms（{} 批）  页延迟 p50/p95/p99 = {:.2}/{:.2}/{:.2} ms   追平 {:.0} ms",
            "",
            "",
            r.append_batch.elapsed_ms,
            r.append_batch.batches,
            r.read.page_p50_ms,
            r.read.page_p95_ms,
            r.read.page_p99_ms,
            r.subscribe.elapsed_ms,
        );
    }
}

/// 结果 JSON 增量写盘：每完成一个规格就落一次盘，中途失败（panic/
/// 中断）时已完成规格的结果不丢失。
fn write_results(
    output: &Option<String>,
    results: &[SizeResult],
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = output else { return Ok(()) };
    let json = serde_json::to_string_pretty(results)?;
    std::fs::write(path, json)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let addrs: Vec<String> = args
        .addrs
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    let sizes: Vec<usize> = args
        .sizes
        .split(',')
        .map(|s| s.trim().parse().expect("sizes 必须是逗号分隔的整数"))
        .collect();
    for &size in &sizes {
        if !(MIN_SIZE_BYTES..MAX_SIZE_BYTES).contains(&size) {
            eprintln!(
                "✗ 事件大小 {size} B 超出支持范围 [{MIN_SIZE_BYTES}, {MAX_SIZE_BYTES})：\
                 过小则事件总数爆炸（订阅物化全流打爆内存），过大超服务端单事件上限"
            );
            std::process::exit(1);
        }
    }

    let ca = std::fs::read(&args.ca).expect("读取 CA 文件失败");
    let mut client =
        EventStoreClient::connect_with_tls(addrs, Some(TlsClientConfig::Ca(ca))).await?;

    // 预热：确认集群可用。冷启动需选举收敛（3 节点 8 分片实测可达数秒，
    // 与脚本端口就绪无必然先后），用重试等待收敛替代固定 sleep；首次
    // append 含选主/建连开销，不计入测量
    let warmup_stream = format!("{}-warmup", args.stream_prefix);
    for attempt in 0..60 {
        let warmup = make_event(XorShift(0xdeadbeef), "perf-warmup", 64);
        match client
            .append(
                warmup_stream.clone(),
                ExpectedVersionBuilder::any(),
                vec![warmup],
            )
            .await
        {
            Ok(_) => break,
            Err(e) if attempt == 59 => {
                return Err(format!("预热 append 失败——60 次重试后集群仍不可用: {e}").into());
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }

    let mut results = Vec::new();
    for size in sizes {
        let stream = format!("{}-{}b", args.stream_prefix, size);
        println!(
            "▶ 规格 {} B（共 {} 条，总量 50MB）...",
            size,
            TOTAL_BYTES_PER_SIZE / size
        );
        let r = run_size(&mut client, size, &stream, args.batch_size).await;
        println!(
            "  ✓ 完成：单条 p50={:.2}ms，批量 {:.1} MB/s，读 {:.1} MB/s，订阅追平 {:.1} MB/s",
            r.append_single.p50_ms, r.append_batch.mb_per_s, r.read.mb_per_s, r.subscribe.mb_per_s
        );
        results.push(r);
        // 增量落盘：中途失败时已完成规格的结果不丢失
        write_results(&args.output, &results)?;
    }

    print_table(&results);

    write_results(&args.output, &results)?;
    if let Some(path) = &args.output {
        println!("\n结果已写入 {path}");
    }
    Ok(())
}
