package com.clevertechware.consumerkotlin

import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder

/** Tracks how many messages were received per routing key. */
@Component
class EventStats {

    private val counts = ConcurrentHashMap<String, LongAdder>()

    fun record(routingKey: String) {
        counts.computeIfAbsent(routingKey) { LongAdder() }.increment()
    }

    fun total(): Long = counts.values.sumOf { it.sum() }

    fun countFor(routingKey: String): Long = counts[routingKey]?.sum() ?: 0

    override fun toString(): String {
        val breakdown = counts.entries
            .sortedBy { it.key }
            .joinToString(" ") { "${it.key}=${it.value.sum()}" }
        return "total=${total()} $breakdown"
    }
}
