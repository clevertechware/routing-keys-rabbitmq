package com.clevertechware.consumerkotlin

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ConsumerKotlinApplication

fun main(args: Array<String>) {
	runApplication<ConsumerKotlinApplication>(*args)
}
