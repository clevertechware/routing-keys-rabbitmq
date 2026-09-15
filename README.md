# Routing keys RabbitMQ

Companion code for the [CTW blog post on RabbitMQ routing
keys](https://www.clevertechware.fr/blog/posts/2026/routing-keys-rabbitmq), on using topic
exchange routing keys so a consumer only receives the messages it needs, instead of a
catch-all `#` binding.

One producer publishes a continuous stream of business events on a single topic exchange.
Three independent consumers, written in three different languages, each bind only the
routing keys they care about and end up processing a fraction of the total stream.

## Scenario

A single topic exchange, `platform.events`, carries change events for a generic B2B SaaS
platform. Every message is published with a routing key shaped as
`v1.<entity>.<event>[.<discriminant>]`:

| Routing key                     | Entity       |
|----------------------------------|--------------|
| `v1.organization.changed`        | organization |
| `v1.asset.changed.book`      | asset        |
| `v1.asset.changed.video`       | asset        |
| `v1.invoice.changed`             | invoice      |
| `v1.document.changed.contract`   | document     |
| `v1.user.changed`                | user         |

Three consumers, each with its own durable queue, bind only what they need:

| Component          | Language / stack                        | Queue                       | Binds                                              |
|---------------------|------------------------------------------|------------------------------|-----------------------------------------------------|
| `consumer-rust`     | Rust, lapin                             | `q.consumer-rust`            | `v1.organization.#`, `v1.asset.#`                   |
| `consumer-go`       | Go, amqp091-go                          | `q.consumer-go`              | `v1.invoice.#`, `v1.document.#`                     |
| `consumer-kotlin`   | Kotlin, Spring Cloud Stream (RabbitMQ binder) | `q.consumer-kotlin`          | `v1.user.changed`, `v1.asset.changed.video`         |

Spring Cloud Stream would name the Kotlin queue `destination.group` by default; the binder's
`queue-name-group-only` property makes it use the group alone, so it lines up with the
`q.<consumer>` convention of the other two in the management UI's Queues tab.

Each consumer logs a running count of messages received per routing key, so the fraction
of the total volume it actually sees is visible in its own output.

## Prerequisites

- Docker and Docker Compose
- Rust 1.88 or newer (`producer-rust` and `consumer-rust` use the 2024 edition)
- Go 1.22 or newer
- A JDK: `consumer-kotlin`'s Gradle wrapper auto-provisions JDK 21 via the Foojay resolver
  if none is found, so a system JDK is not strictly required

## 1. Start RabbitMQ

```bash
docker compose up -d
```

This starts RabbitMQ 4.1 with the management plugin enabled, then runs a one-shot
`rabbitmq-init` service that declares the `platform.events.unrouted` alternate exchange and
its queue (see [Ce que donne un message sans binding correspondant](#ce-que-donne-un-message-sans-binding-correspondant)).
Confirm it finished before starting the consumers or the producer:

```bash
docker compose logs rabbitmq-init
# alternate exchange topology ready: platform.events -> platform.events.unrouted -> q.unrouted
```

The management UI is at <http://localhost:15672> (guest/guest) — use it to inspect the
exchanges, the four queues, and their bindings while the demo runs.

## 2. Start the three consumers

Each consumer declares its own durable queue and its bindings on startup (the Kotlin one
relies on the exchange declared by `rabbitmq-init`), so start all three before the
producer — a message with no matching binding never reaches a consumer queue; here it
lands in `q.unrouted` instead (see [section 4](#4-see-what-happens-to-a-message-with-no-matching-binding)).

```bash
# Rust consumer — binds v1.organization.# and v1.asset.#
cd consumer-rust
cargo run

# Go consumer — binds v1.invoice.# and v1.document.#
cd consumer-go
go run .

# Kotlin consumer — binds v1.user.changed and v1.asset.changed.book
cd consumer-kotlin
./gradlew bootRun
```

All three default to `amqp://guest:guest@localhost:5672`. Override with `AMQP_URL`
(Rust/Go) or `RABBITMQ_HOST` / `RABBITMQ_PORT` / `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD`
(Kotlin, standard Spring Boot RabbitMQ properties).

## 3. Run the producer

```bash
cd producer-rust
cargo run
```

Publishes 5000 messages by default, spread uniformly at random across the six routing keys
above, at one message every 5ms. Override with `MESSAGE_COUNT` and `PUBLISH_INTERVAL_MS`.
Progress and a final per-routing-key count are printed as it runs. Set `PUBLISH_UNROUTABLE=1`
to also publish one message on a routing key no consumer binds — see the next section.

## What you should observe

With the default 5000 messages spread evenly over six routing keys (roughly 833 messages
per key):

- **consumer-rust** logs roughly 2500 received messages (organization + both asset
  discriminants) — half the stream, never the invoice, document or user events.
- **consumer-go** logs roughly 1666 received messages (invoice + document) — a third of
  the stream, never organization, asset or user events.
- **consumer-kotlin** logs roughly 1666 received messages (user + `asset.changed.book`
  only, not `asset.changed.video`) — never invoice, document, organization or the.video
  discriminant of asset.

No consumer's total ever reaches the producer's total: each is bound to a subset of
routing keys and RabbitMQ only delivers what matches. The management UI's Exchanges tab
lets you see the same thing structurally — inspect `platform.events` and its bindings to
see each queue's routing keys next to the others.

## 4. See what happens to a message with no matching binding

The scenario above is silent by design: none of the six routing keys the producer sends
is ever unroutable, so nothing here shows what happens to a message that *is*. Set
`PUBLISH_UNROUTABLE=1` on the producer to publish one extra message on `v1.unknown.changed`
— a routing key no consumer binds — with the AMQP `mandatory` flag set:

```bash
cd producer-rust
PUBLISH_UNROUTABLE=1 cargo run
```

This repository ships with a safety net already attached: `rabbitmq-init` (started by
`docker compose up -d`, see [1. Start RabbitMQ](#1-start-rabbitmq)) configures
`platform.events` with an `alternate-exchange` argument pointing at
`platform.events.unrouted`, a fanout exchange bound to its own queue, `q.unrouted`. With
that in place, the demo message is **not** returned to the producer:

```
PUBLISH_UNROUTABLE: message on 'v1.unknown.changed' was not returned — it was routed
to the 'platform.events.unrouted' alternate exchange instead of being dropped
```

Confirm it in the management UI (Queues tab, `q.unrouted`) or with:

```bash
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/q.unrouted | grep -o '"messages":[0-9]*'
```

Now remove the safety net and republish, to see the loss the article is actually about.
Delete `q.unrouted` from the management UI (Queues tab → q.unrouted → Delete Queue) — this
leaves the alternate exchange itself with nowhere to route to — then run the producer again
with `PUBLISH_UNROUTABLE=1`:

```
PUBLISH_UNROUTABLE: broker returned the message published on 'v1.unknown.changed'
(reply 312 'NO_ROUTE') — no queue was bound to that routing key
```

That `basic.return` only happens because the message was published as `mandatory`, with a
listener that checks `Confirmation::take_message()`. Without both of those — which is the
default `basic_publish` on every other message in this repo — RabbitMQ's behavior is
identical (the message is still routed nowhere) but nothing is reported back: the publisher
receives a plain acknowledgement (the message was durably accepted by the broker) and the
message never reaches a queue. That gap between "the broker acknowledged it" and "a consumer
will see it" is the silent loss. `mandatory` plus a return listener makes it observable per
message; an alternate exchange makes it survivable without either.

Recreate the queue and its binding to restore the default setup:

```bash
curl -s -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/q.unrouted \
  -H "content-type: application/json" -d '{"durable":true}'
curl -s -u guest:guest -X POST \
  http://localhost:15672/api/bindings/%2F/e/platform.events.unrouted/q/q.unrouted \
  -H "content-type: application/json" -d '{"routing_key":""}'
```

**Why the safety net is on by default here:** an alternate exchange with nothing bound to it
is exactly as silent as having none — it only helps once something consumes from it, which
is the operationally realistic setup to hand a reader. The steps above exist to make the
underlying, always-true failure mode (no `mandatory`, no alternate exchange, no
return listener → message vanishes with no trace) visible on demand rather than to bury it.

## Tests

```bash
# Rust (both crates)
cd producer-rust && cargo test && cargo clippy --all-targets -- -D warnings
cd consumer-rust && cargo test && cargo clippy --all-targets -- -D warnings

# Go
cd consumer-go && go test ./... && go vet ./...

# Kotlin
cd consumer-kotlin && ./gradlew test
```

Tests cover routing key construction (producer) and the per-routing-key counting logic
(each consumer) without requiring a running broker.

## Project layout

```
producer-rust/     Rust binary publishing events across 6 routing keys
consumer-rust/     Rust binary binding organization + asset events
consumer-go/       Go binary binding invoice + document events
consumer-kotlin/   Spring Cloud Stream (Kotlin) app binding user + asset.changed.book
rabbitmq/          Alternate-exchange topology, declared by the rabbitmq-init service
docker-compose.yml RabbitMQ with the management plugin, plus the rabbitmq-init service
```

## Cleanup

```bash
docker compose down -v
```
