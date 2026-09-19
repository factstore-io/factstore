package io.factstore.server.http

import io.factstore.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.takeWhile
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds

class FactStreamLinesTest {

    private val fact1 = fact()
    private val fact2 = fact()

    @Test
    @DisplayName("A complete stream is a fact line per fact, closed by an end line with their count")
    fun completeStream(): Unit = runBlocking {
        val lines = flowOf(fact1, fact2).toFactStreamLines().toList()

        assertThat(lines).containsExactly(
            FactStreamLineHttp.FactLine(fact1.toFactHttp()),
            FactStreamLineHttp.FactLine(fact2.toFactHttp()),
            FactStreamLineHttp.EndLine(FactStreamEndHttp(count = 2)),
        )
    }

    @Test
    @DisplayName("An empty stream is only an end line")
    fun emptyStream(): Unit = runBlocking {
        val lines = emptyFlow<Fact>().toFactStreamLines().toList()

        assertThat(lines).containsExactly(FactStreamLineHttp.EndLine(FactStreamEndHttp(count = 0)))
    }

    @Test
    @DisplayName("A failing stream is closed by an error line instead of an end line")
    fun failingStream(): Unit = runBlocking {
        val lines = flow {
            emit(fact1)
            throw IllegalStateException("backend failure")
        }.toFactStreamLines().toList()

        assertThat(lines).hasSize(2)
        assertThat(lines[0]).isEqualTo(FactStreamLineHttp.FactLine(fact1.toFactHttp()))
        val error = (lines[1] as FactStreamLineHttp.ErrorLine).error
        assertThat(error.reason).isEqualTo(Reason.InternalError)
        assertThat(error.message).doesNotContain("backend failure")
    }

    // ─── Subscriptions ────────────────────────────────────────────────────────

    @Test
    @DisplayName("A subscription is a fact line per fact, and never an end line")
    fun subscriptionLines(): Unit = runBlocking {
        // A subscription does not end on its own, so it is read by count.
        val lines = flowOf(fact1, fact2).toSubscriptionLines().take(2).toList()

        assertThat(lines).containsExactly(
            FactStreamLineHttp.FactLine(fact1.toFactHttp()),
            FactStreamLineHttp.FactLine(fact2.toFactHttp()),
        )
    }

    @Test
    @DisplayName("An idle subscription sends a heartbeat line, and resumes with the next fact")
    fun idleSubscriptionHeartbeats(): Unit = runBlocking {
        val lines = flow {
            emit(fact1)
            // Longer than the heartbeat interval below, so the wait is filled with heartbeats.
            delay(200.milliseconds)
            emit(fact2)
        }.toSubscriptionLines(heartbeatEvery = 20.milliseconds)
            .takeWhile { it != FactStreamLineHttp.FactLine(fact2.toFactHttp()) }
            .toList()

        assertThat(lines.first()).isEqualTo(FactStreamLineHttp.FactLine(fact1.toFactHttp()))
        assertThat(lines.drop(1)).isNotEmpty().allMatch { it is FactStreamLineHttp.HeartbeatLine }
    }

    @Test
    @DisplayName("A failing subscription is closed by an error line")
    fun failingSubscription(): Unit = runBlocking {
        val lines = flow {
            emit(fact1)
            throw IllegalStateException("backend failure")
        }.toSubscriptionLines().toList()

        assertThat(lines).hasSize(2)
        assertThat(lines[0]).isEqualTo(FactStreamLineHttp.FactLine(fact1.toFactHttp()))
        val error = (lines[1] as FactStreamLineHttp.ErrorLine).error
        assertThat(error.reason).isEqualTo(Reason.InternalError)
        assertThat(error.message).doesNotContain("backend failure")
    }

    @Test
    @DisplayName("A subscription whose facts run out ends without an end line")
    fun exhaustedSubscription(): Unit = runBlocking {
        // The specification's subscriptions never complete; a backend that completes anyway
        // must not look like a complete stream.
        val lines = flowOf(fact1).toSubscriptionLines().toList()

        assertThat(lines).containsExactly(FactStreamLineHttp.FactLine(fact1.toFactHttp()))
    }

    private fun fact() = Fact(
        id = FactId.generate(),
        type = "T".toFactType(),
        payload = "{}".toFactPayload(),
        subject = "s".toSubject(),
        appendedAt = Instant.now(),
    )
}
