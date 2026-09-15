package com.clevertechware.consumerkotlin

import org.slf4j.LoggerFactory
import org.springframework.amqp.support.AmqpHeaders
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.messaging.Message
import java.util.function.Consumer

@Configuration
class EventConsumerConfiguration(private val stats: EventStats) {

    private val logger = LoggerFactory.getLogger(EventConsumerConfiguration::class.java)

    @Bean
    fun eventConsumer(): Consumer<Message<EventPayload>> = Consumer { message ->
        val routingKey = message.headers[AmqpHeaders.RECEIVED_ROUTING_KEY, String::class.java] ?: message.payload.routingKey

        stats.record(routingKey)
        logger.info("received {} id={} sequence={}", routingKey, message.payload.id, message.payload.sequence)

        if (stats.total() % 100 == 0L) {
            logger.info("received {} messages, breakdown: {}", stats.total(), stats)
        }
    }
}
