mod stats;

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{Connection, ConnectionProperties, ExchangeKind};
use serde::Deserialize;
use stats::Stats;

#[derive(Debug, Deserialize)]
struct EventPayload {
    id: String,
    sequence: u64,
}

const EXCHANGE_NAME: &str = "platform.events";
const ALTERNATE_EXCHANGE_NAME: &str = "platform.events.unrouted";
const QUEUE_NAME: &str = "q.consumer-rust";
const BINDING_KEYS: [&str; 2] = ["v1.organization.#", "v1.asset.#"];

#[tokio::main]
async fn main() -> Result<()> {
    let amqp_url = std::env::var("AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into());

    let connection = Connection::connect(&amqp_url, ConnectionProperties::default())
        .await
        .with_context(|| format!("failed to connect to {amqp_url}"))?;
    let channel = connection.create_channel().await.context("failed to open a channel")?;

    // Must match the arguments rabbitmq/init-alternate-exchange.sh declares
    // the exchange with, or the broker rejects this as an inequivalent
    // redeclaration.
    let mut exchange_args = FieldTable::default();
    exchange_args.insert("alternate-exchange".into(), AMQPValue::LongString(ALTERNATE_EXCHANGE_NAME.into()));

    channel
        .exchange_declare(
            EXCHANGE_NAME.into(),
            ExchangeKind::Topic,
            ExchangeDeclareOptions { durable: true, ..Default::default() },
            exchange_args,
        )
        .await
        .context("failed to declare the topic exchange")?;

    channel
        .queue_declare(QUEUE_NAME.into(), QueueDeclareOptions { durable: true, ..Default::default() }, FieldTable::default())
        .await
        .context("failed to declare the queue")?;

    for binding_key in BINDING_KEYS {
        channel
            .queue_bind(QUEUE_NAME.into(), EXCHANGE_NAME.into(), binding_key.into(), QueueBindOptions::default(), FieldTable::default())
            .await
            .with_context(|| format!("failed to bind routing key '{binding_key}'"))?;
    }

    println!("bound to {BINDING_KEYS:?}, waiting for messages on queue '{QUEUE_NAME}'");

    let mut consumer = channel
        .basic_consume(QUEUE_NAME.into(), "consumer-rust".into(), BasicConsumeOptions::default(), FieldTable::default())
        .await
        .context("failed to start consuming")?;

    let mut stats = Stats::default();

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery.context("delivery error while consuming")?;
        let routing_key = delivery.routing_key.as_str().to_string();

        match serde_json::from_slice::<EventPayload>(&delivery.data) {
            Ok(event) => println!("received {routing_key} id={} sequence={}", event.id, event.sequence),
            Err(error) => eprintln!("skipping message on {routing_key}: invalid payload ({error})"),
        }

        delivery.ack(BasicAckOptions::default()).await.context("failed to ack message")?;

        stats.record(&routing_key);
        if stats.total() % 100 == 0 {
            println!("received {} messages, breakdown: {stats}", stats.total());
        }
    }

    Ok(())
}
