package net.corda.flow.necromancer

import org.junit.jupiter.api.Test
import kotlin.concurrent.timer

class TestClass {

    @Test
    fun test() {
        val timer = timer(
                daemon = false,
                initialDelay = 0.toLong(),
                period = 1000
        ) {
            println("here")
        }
        Thread.sleep(5000)
    }

}