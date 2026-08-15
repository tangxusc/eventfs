//! EventStore 客户端使用示例
//!
//! 环境变量：
//! - `EVT_ADDR`：节点地址（默认 `http://127.0.0.1:50051`；https 集群写
//!   `https://...`，如 `EVT_ADDR=https://127.0.0.1:50051`）
//! - `EVT_CA`：CA 文件路径（可选）。设置后严格校验对端证书；未设置时
//!   https 地址默认跳过校验（自签友好）

use es_client::{EventBuilder, EventStoreClient, ExpectedVersionBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 连接到 EventStore 集群
    let addr = std::env::var("EVT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
    let mut client = match std::env::var("EVT_CA") {
        Ok(ca) => {
            EventStoreClient::connect_with_tls(
                vec![addr.clone()],
                Some(es_client::TlsClientConfig::Ca(std::fs::read(&ca)?)),
            )
            .await?
        }
        Err(_) => EventStoreClient::connect(vec![addr.clone()]).await?,
    };

    println!("✓ Connected to EventStore ({addr})");

    // 创建事件
    let event = EventBuilder::new("OrderPlaced")
        .data_json(&serde_json::json!({
            "order_id": "order-123",
            "customer_id": "customer-456",
            "amount": 99.99
        }))?
        .build();

    // 追加事件到流
    let response = client
        .append(
            "order-order-123".to_string(),
            ExpectedVersionBuilder::any(),
            vec![event],
        )
        .await?;

    println!(
        "✓ Event appended: shard={}, position={}-{}",
        response.shard_id, response.first_position, response.last_position
    );

    // 读取流事件
    let events = client
        .read_stream(
            "order-order-123".to_string(),
            0,
            100,
            es_client::Direction::Forward,
        )
        .await?;

    println!("✓ Read {} events", events.len());

    for event in events {
        println!(
            "  - {} v{} @ position {}",
            event.event_type, event.version, event.position
        );
    }

    Ok(())
}
