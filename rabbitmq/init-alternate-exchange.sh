#!/bin/sh
# Declares the alternate-exchange safety net once RabbitMQ is healthy.
# Runs as a one-shot compose service (see docker-compose.yml, service
# "rabbitmq-init"): it calls the management HTTP API instead of the AMQP
# port, so it does not depend on lapin/amqp091-go/Spring being available.
set -eu

RABBIT_URL="http://rabbitmq:15672"
AUTH="guest:guest"

curl -sf -u "$AUTH" -X PUT "$RABBIT_URL/api/exchanges/%2F/platform.events.unrouted" \
  -H "content-type: application/json" \
  -d '{"type":"fanout","durable":true}'

curl -sf -u "$AUTH" -X PUT "$RABBIT_URL/api/queues/%2F/q.unrouted" \
  -H "content-type: application/json" \
  -d '{"durable":true}'

curl -sf -u "$AUTH" -X POST "$RABBIT_URL/api/bindings/%2F/e/platform.events.unrouted/q/q.unrouted" \
  -H "content-type: application/json" \
  -d '{"routing_key":""}'

# Attaching the alternate exchange here means every producer/consumer that
# declares "platform.events" afterwards must pass the same argument, or
# RabbitMQ rejects the redeclaration as inequivalent. That is why the three
# apps also set it explicitly (see their exchange_declare/ExchangeDeclare calls).
curl -sf -u "$AUTH" -X PUT "$RABBIT_URL/api/exchanges/%2F/platform.events" \
  -H "content-type: application/json" \
  -d '{"type":"topic","durable":true,"arguments":{"alternate-exchange":"platform.events.unrouted"}}'

echo "alternate exchange topology ready: platform.events -> platform.events.unrouted -> q.unrouted"
