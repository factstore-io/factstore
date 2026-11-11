package io.factstore.application

import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds

/** Decides whether a request whose append condition was violated gets another attempt. */
fun interface RetryPolicy {

    /** The wait before the next attempt, after [failedAttempts] attempts were violated; `null` to give up. */
    fun delayAfter(failedAttempts: Int): Duration?

    companion object {

        /**
         * At most [maxAttempts] attempts. Each retry waits a random time below a bound that starts at
         * [initialDelay] and doubles up to [maxDelay] ("full jitter"), so requests that collided on
         * one boundary don't collide again in lockstep.
         */
        fun exponentialBackoff(
            maxAttempts: Int = 3,
            initialDelay: Duration = 10.milliseconds,
            maxDelay: Duration = 100.milliseconds,
            random: Random = Random.Default,
        ): RetryPolicy {
            require(maxAttempts >= 1) { "A request needs at least one attempt, but maxAttempts was $maxAttempts." }
            require(initialDelay.isPositive() && maxDelay >= initialDelay) {
                "The delays must be positive and initialDelay ($initialDelay) must not exceed maxDelay ($maxDelay)."
            }
            return RetryPolicy { failedAttempts ->
                if (failedAttempts >= maxAttempts) return@RetryPolicy null
                val bound = (initialDelay * (1 shl (failedAttempts - 1).coerceAtMost(30))).coerceAtMost(maxDelay)
                random.nextLong(bound.inWholeMicroseconds + 1).microseconds
            }
        }
    }
}
