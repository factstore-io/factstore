package io.factstore.application

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class RetryPolicyTest {

    @Test
    fun `allows three attempts by default`() {
        val policy = RetryPolicy.exponentialBackoff(random = Random(42))

        assertThat(policy.delayAfter(1)).isNotNull()
        assertThat(policy.delayAfter(2)).isNotNull()
        assertThat(policy.delayAfter(3)).isNull()
    }

    @Test
    fun `waits a random time below a bound that doubles up to its maximum`() {
        val policy = RetryPolicy.exponentialBackoff(maxAttempts = 10, random = Random(42))
        val bounds = mapOf(1 to 10.milliseconds, 2 to 20.milliseconds, 3 to 40.milliseconds, 4 to 80.milliseconds, 5 to 100.milliseconds, 9 to 100.milliseconds)

        bounds.forEach { (failedAttempts, bound) ->
            val waits = List(1_000) { policy.delayAfter(failedAttempts)!! }
            assertThat(waits).allSatisfy { assertThat(it).isBetween(Duration.ZERO, bound) }
            assertThat(waits.max()).isGreaterThan(bound / 2)
        }
    }
}
