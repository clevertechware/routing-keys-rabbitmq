package com.clevertechware.consumerkotlin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.amqp.support.AmqpHeaders
import org.springframework.messaging.support.MessageBuilder

class EventConsumerConfigurationTest {

    @Test
    fun consumingAMessageIncrementsTheCountForItsReceivedRoutingKey() {
        val stats = EventStats()
        val consumer = EventConsumerConfiguration(stats).eventConsumer()

        consumer.accept(messageOn("v1.user.changed"))

        assertThat(stats.countFor("v1.user.changed")).isEqualTo(1)
    }

    @Test
    fun consumingMessagesOnDifferentRoutingKeysTracksThemSeparately() {
        val stats = EventStats()
        val consumer = EventConsumerConfiguration(stats).eventConsumer()

        consumer.accept(messageOn("v1.user.changed"))
        consumer.accept(messageOn("v1.asset.changed.building"))

        assertThat(stats.countFor("v1.user.changed")).isEqualTo(1)
        assertThat(stats.countFor("v1.asset.changed.building")).isEqualTo(1)
    }

    private fun messageOn(routingKey: String) = MessageBuilder
        .withPayload(EventPayload(id = "evt-1", routingKey = routingKey, sequence = 1))
        .setHeader(AmqpHeaders.RECEIVED_ROUTING_KEY, routingKey)
        .build()
}
