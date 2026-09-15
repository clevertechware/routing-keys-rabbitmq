package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	exchangeName          = "platform.events"
	alternateExchangeName = "platform.events.unrouted"
	queueName             = "q.consumer-go"
)

var bindingKeys = []string{"v1.invoice.#", "v1.document.#"}

type eventPayload struct {
	ID       string `json:"id"`
	Sequence uint64 `json:"sequence"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		log.Fatalf("consumer-go: %v", err)
	}
}

func run(ctx context.Context) error {
	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("connecting to %s: %w", amqpURL, err)
	}
	defer conn.Close()

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("opening a channel: %w", err)
	}
	defer channel.Close()

	// Must match the arguments rabbitmq/init-alternate-exchange.sh declares the exchange with, or the broker rejects
	// this as an inequivalent redeclaration.
	exchangeArgs := amqp.Table{"alternate-exchange": alternateExchangeName}
	if err = channel.ExchangeDeclare(exchangeName, amqp.ExchangeTopic, true, false, false, false, exchangeArgs); err != nil {
		return fmt.Errorf("declaring exchange %s: %w", exchangeName, err)
	}

	if _, err = channel.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring queue %s: %w", queueName, err)
	}

	for _, bindingKey := range bindingKeys {
		if err = channel.QueueBind(queueName, bindingKey, exchangeName, false, nil); err != nil {
			return fmt.Errorf("binding routing key %q: %w", bindingKey, err)
		}
	}

	deliveries, err := channel.Consume(queueName, "consumer-go", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("starting to consume: %w", err)
	}

	log.Printf("bound to %v, waiting for messages on queue %q", bindingKeys, queueName)

	stats := NewStats()

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down, final counts: %s", stats)
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				log.Printf("delivery channel closed, final counts: %s", stats)
				return nil
			}
			handleDelivery(delivery, stats)
		}
	}
}

func handleDelivery(delivery amqp.Delivery, stats *Stats) {
	var event eventPayload
	if err := json.Unmarshal(delivery.Body, &event); err != nil {
		log.Printf("skipping message on %s: invalid payload (%v)", delivery.RoutingKey, err)
	} else {
		log.Printf("received %s id=%s sequence=%d", delivery.RoutingKey, event.ID, event.Sequence)
	}

	if err := delivery.Ack(false); err != nil {
		log.Printf("failed to ack message on %s: %v", delivery.RoutingKey, err)
		return
	}

	stats.Record(delivery.RoutingKey)
	if stats.Total()%100 == 0 {
		log.Printf("received %d messages, breakdown: %s", stats.Total(), stats)
	}
}
