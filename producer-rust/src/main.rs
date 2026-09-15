mod domain;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use domain::{BusinessEvent, EventPayload};
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, ExchangeDeclareOptions};
use lapin::types::{AMQPValue, FieldTable};
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use rand::RngExt;
use uuid::Uuid;

const EXCHANGE_NAME: &str = "platform.events";
const ALTERNATE_EXCHANGE_NAME: &str = "platform.events.unrouted";
// No binding targets this key on purpose: it demonstrates what happens to a
// message published on a routing key nobody consumes.
const UNROUTABLE_ROUTING_KEY: &str = "v1.unknown.changed";

struct Config {
    amqp_url: String,
    message_count: u64,
    publish_interval: Duration,
    publish_unroutable: bool,
}

impl Config {
    fn from_env() -> Result<Self> {
        let amqp_url = std::env::var("AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672/%2f".into());

        let message_count = std::env::var("MESSAGE_COUNT")
            .unwrap_or_else(|_| "5000".into())
            .parse()
            .context("MESSAGE_COUNT must be a positive integer")?;

        let publish_interval_ms: u64 = std::env::var("PUBLISH_INTERVAL_MS")
            .unwrap_or_else(|_| "5".into())
            .parse()
            .context("PUBLISH_INTERVAL_MS must be a positive integer")?;

        let publish_unroutable = std::env::var("PUBLISH_UNROUTABLE").is_ok_and(|value| value == "1");

        Ok(Self {
            amqp_url,
            message_count,
            publish_interval: Duration::from_millis(publish_interval_ms),
            publish_unroutable,
        })
    }
}

fn random_event() -> BusinessEvent {
    let index = rand::rng().random_range(0..BusinessEvent::ALL.len());
    BusinessEvent::ALL[index]
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::from_env()?;

    let connection = Connection::connect(&config.amqp_url, ConnectionProperties::default())
        .await
        .with_context(|| format!("failed to connect to {}", config.amqp_url))?;
    let channel = connection.create_channel().await.context("failed to open a channel")?;

    // Every process that declares this exchange must agree on its arguments,
    // including the ones set by rabbitmq/init-alternate-exchange.sh — a
    // mismatched "alternate-exchange" is rejected by the broker as an
    // inequivalent redeclaration.
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
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .context("failed to enable publisher confirms")?;

    println!("connected, publishing {} messages to exchange '{}'", config.message_count, EXCHANGE_NAME);

    let mut counts: HashMap<&'static str, u64> = HashMap::new();
    let started_at = Instant::now();

    for sequence in 0..config.message_count {
        let event = random_event();
        let payload = EventPayload { id: Uuid::new_v4().to_string(), routing_key: event.routing_key(), sequence };
        let body = serde_json::to_vec(&payload).context("failed to serialize event payload")?;
        let properties = BasicProperties::default().with_content_type("application/json".into()).with_delivery_mode(2);

        let confirmation = channel
            .basic_publish(EXCHANGE_NAME.into(), event.routing_key().into(), BasicPublishOptions::default(), &body, properties)
            .await
            .context("failed to publish message")?
            .await
            .context("broker did not confirm the message")?;

        if confirmation.is_nack() {
            anyhow::bail!("broker nacked message {sequence} on routing key '{}'", event.routing_key());
        }

        *counts.entry(event.routing_key()).or_insert(0) += 1;

        if (sequence + 1) % 500 == 0 {
            println!("published {} messages so far: {:?}", sequence + 1, counts);
        }

        tokio::time::sleep(config.publish_interval).await;
    }

    println!("done in {:.1}s, final counts per routing key: {:?}", started_at.elapsed().as_secs_f64(), counts);

    if config.publish_unroutable {
        publish_unroutable_demo(&channel).await?;
    }

    Ok(())
}

/// Publishes one message on a routing key with no matching binding, marked
/// `mandatory`, to show what RabbitMQ does with it: without the alternate
/// exchange it comes back to us as a returned message; with it attached
/// (the default in this repo's docker-compose.yml), the broker routes it
/// there instead and this function observes an acknowledged, non-returned
/// publish. See the README section "Ce que donne un message sans binding
/// correspondant".
async fn publish_unroutable_demo(channel: &lapin::Channel) -> Result<()> {
    let payload = EventPayload { id: Uuid::new_v4().to_string(), routing_key: UNROUTABLE_ROUTING_KEY, sequence: 0 };
    let body = serde_json::to_vec(&payload).context("failed to serialize the unroutable demo payload")?;
    let properties = BasicProperties::default().with_content_type("application/json".into()).with_delivery_mode(2);

    let confirmation = channel
        .basic_publish(
            EXCHANGE_NAME.into(),
            UNROUTABLE_ROUTING_KEY.into(),
            BasicPublishOptions { mandatory: true, ..Default::default() },
            &body,
            properties,
        )
        .await
        .context("failed to publish the unroutable demo message")?
        .await
        .context("broker did not confirm the unroutable demo message")?;

    match confirmation.take_message() {
        Some(returned) => println!(
            "PUBLISH_UNROUTABLE: broker returned the message published on '{UNROUTABLE_ROUTING_KEY}' \
             (reply {} '{}') — no queue was bound to that routing key",
            returned.reply_code, returned.reply_text
        ),
        None => println!(
            "PUBLISH_UNROUTABLE: message on '{UNROUTABLE_ROUTING_KEY}' was not returned — it was routed \
             to the '{ALTERNATE_EXCHANGE_NAME}' alternate exchange instead of being dropped"
        ),
    }

    Ok(())
}
