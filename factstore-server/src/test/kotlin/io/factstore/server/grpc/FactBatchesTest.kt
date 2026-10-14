package io.factstore.server.grpc

import io.factstore.core.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.time.Instant

class FactBatchesTest {

    @Test
    @DisplayName("Batches keep every fact in order and stay within the size limit")
    fun batchesStayWithinLimit(): Unit = runBlocking {
        // 100 facts of the maximum payload size, about 6.5 MiB in total.
        val facts = (1..100).map { fact(ByteArray(FactPayload.MAX_SIZE)) }

        val batches = facts.asFlow().toProtoFactBatches().toList()

        assertThat(batches).hasSizeGreaterThan(1)
        assertThat(batches).allMatch { it.serializedSize <= MAX_FACT_BATCH_BYTES }
        assertThat(batches.flatMap { it.factsList }.map { it.id }).containsExactlyElementsOf(facts.map { it.id.uuid.toString() })
    }

    @Test
    @DisplayName("A fact larger than the limit is sent in a batch of its own")
    fun factLargerThanLimit(): Unit = runBlocking {
        val facts = (1..3).map { fact(ByteArray(1_000)) }

        val batches = facts.asFlow().toProtoFactBatches(maxBytes = 100).toList()

        assertThat(batches.map { it.factsCount }).containsExactly(1, 1, 1)
    }

    @Test
    @DisplayName("Small facts are sent together in one batch")
    fun smallFactsShareABatch(): Unit = runBlocking {
        val facts = (1..10).map { fact(ByteArray(10)) }

        val batches = facts.asFlow().toProtoFactBatches().toList()

        assertThat(batches.map { it.factsCount }).containsExactly(10)
    }

    @Test
    @DisplayName("No facts result in no batches")
    fun noFacts(): Unit = runBlocking {
        assertThat(emptyFlow<Fact>().toProtoFactBatches().toList()).isEmpty()
    }

    @Test
    @DisplayName("A fact is sent without waiting for a batch to fill")
    fun factIsNotHeldBack(): Unit = runBlocking {
        val more = CompletableDeferred<Unit>()
        val first = fact(ByteArray(10))
        val facts = flow {
            emit(first)
            // A slow read, such as a selective query, finds the next fact only much later.
            more.await()
            emit(fact(ByteArray(10)))
        }

        val batch = withTimeout(5_000) { facts.toProtoFactBatches().first() }

        assertThat(batch.factsList.map { it.id }).containsExactly(first.id.uuid.toString())
    }

    private fun fact(payload: ByteArray) = Fact(
        id = FactId.generate(),
        type = "T".toFactType(),
        payload = FactPayload(payload),
        subject = "s".toSubject(),
        appendedAt = Instant.now(),
    )
}
