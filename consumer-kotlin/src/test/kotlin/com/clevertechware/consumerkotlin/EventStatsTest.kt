package com.clevertechware.consumerkotlin

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class EventStatsTest {

    @Test
    fun recordingTheSameRoutingKeyTwiceAccumulatesItsCount() {
        val stats = EventStats()

        stats.record("v1.user.changed")
        stats.record("v1.user.changed")

        assertThat(stats.countFor("v1.user.changed")).isEqualTo(2)
    }

    @Test
    fun recordingDistinctRoutingKeysTracksThemSeparately() {
        val stats = EventStats()

        stats.record("v1.user.changed")
        stats.record("v1.asset.changed.building")

        assertThat(stats.countFor("v1.user.changed")).isEqualTo(1)
        assertThat(stats.countFor("v1.asset.changed.building")).isEqualTo(1)
    }

    @Test
    fun totalSumsEveryRoutingKeyCount() {
        val stats = EventStats()

        stats.record("v1.user.changed")
        stats.record("v1.asset.changed.building")
        stats.record("v1.asset.changed.building")

        assertThat(stats.total()).isEqualTo(3)
    }

    @Test
    fun countForAnUnseenRoutingKeyIsZero() {
        val stats = EventStats()

        assertThat(stats.countFor("v1.invoice.changed")).isEqualTo(0)
    }
}
