//! EventStore 客户端使用示例

use es_client::{EventStoreClient, ExpectedVersionBuilder, EventBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tracing_subscriber::fmt::init();

    // 连接到 EventStore 集群
    let mut client = EventStoreClient::connect(vec![
        "http://127.0.0.1:50051".to_string(),
    ])
    .await?;

    println!("✓ Connected to EventStore");

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
            es_client::Direction::DirectionForward,
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
