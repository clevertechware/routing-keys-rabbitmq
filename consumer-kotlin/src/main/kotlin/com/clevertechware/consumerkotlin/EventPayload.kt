package com.clevertechware.consumerkotlin

import com.fasterxml.jackson.annotation.JsonProperty

data class EventPayload(
    val id: String,
    @JsonProperty("routing_key") val routingKey: String,
    val sequence: Long,
)
