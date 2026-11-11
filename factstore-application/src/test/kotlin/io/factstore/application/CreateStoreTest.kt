package io.factstore.application

import io.factstore.application.slices.CreateStoreSlice
import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.FactQueryItem
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.domain.StoreCreated
import io.factstore.domain.StoreCreationRejected
import io.factstore.domain.SystemFact
import io.factstore.domain.createStoreDecider
import io.factstore.storage.FactLog
import io.factstore.storage.LogAppendResult
import io.factstore.storage.LogCondition
import io.factstore.storage.LogFact
import io.factstore.storage.LogQuery
import io.factstore.storage.LogQueryItem
import io.factstore.storage.memory.InMemoryFactLog
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicInteger

class CreateStoreTest {

    private val log = InMemoryFactLog()
    private val orders = StoreName("orders")

    private fun application(log: FactLog, ids: List<StoreId>): FactStoreApplication {
        val next = ids.iterator()
        return FactStoreApplication(CreateStoreSlice(createStoreDecider, SystemFactRepository(log))) { next.next() }
    }

    private fun List<Fact>.decoded(): List<SystemFact> = map(SystemFactCodec::decode)

    private suspend fun DecisionResult<SystemFact, FactId>.recordedFacts(): List<SystemFact> {
        assertThat(this).isInstanceOf(DecisionResult.Recorded::class.java)
        return (this as DecisionResult.Recorded).facts.map { (fact, _) -> fact }.toList()
    }

    private suspend fun FactLog.storeFactsIn(storeId: StoreId): List<SystemFact> {
        val types = SystemFactTypes.run { listOf(STORE_CREATED, STORE_CREATION_REJECTED) }
        return read(LogQuery(types.map { LogQueryItem(storeId, FactQueryItem(types = setOf(it))) })).toList().map { it.fact }.decoded()
    }

    @Test
    fun `records the new store in the system store and returns the stored fact`(): Unit = runBlocking {
        val s1 = StoreId.generate()

        val outcome = application(log, listOf(s1)).createStore(orders)

        assertThat(outcome.recordedFacts()).containsExactly(StoreCreated(s1, orders))
        assertThat(log.storeFactsIn(SystemStore.ID)).containsExactly(StoreCreated(s1, orders))
        assertThat(log.storeFactsIn(s1)).isEmpty()
    }

    @Test
    fun `records a rejection of the new store, naming the holder of the name`(): Unit = runBlocking {
        val s1 = StoreId.generate()
        val s2 = StoreId.generate()
        val application = application(log, listOf(s1, s2))
        application.createStore(orders)

        val outcome = application.createStore(orders)

        val rejection = StoreCreationRejected(s2, orders, StoreCreationRejected.Reason.NameTaken(holder = s1))
        assertThat(outcome.recordedFacts()).containsExactly(rejection)
        assertThat(log.storeFactsIn(SystemStore.ID)).containsExactly(StoreCreated(s1, orders), rejection)
    }

    @Test
    fun `lets exactly one of two concurrent creates take the name, and reports the violated condition to the other`(): Unit = runBlocking {
        val s1 = StoreId.generate()
        val s2 = StoreId.generate()
        val racingLog = BothReadFirst(log)

        val outcomes = withTimeout(5_000) {
            listOf(s1, s2).map { id -> async(Dispatchers.Default) { application(racingLog, listOf(id)).createStore(orders) } }.awaitAll()
        }

        val (recorded, violated) = outcomes.partition { it is DecisionResult.Recorded }
        assertThat(violated).containsExactly(DecisionResult.ConditionViolated)
        assertThat(log.storeFactsIn(SystemStore.ID)).isEqualTo(recorded.single().recordedFacts())
        assertThat(recorded.single().recordedFacts().single()).isInstanceOf(StoreCreated::class.java)
    }

    @Test
    fun `stamps the recorded fact with the application's clock`(): Unit = runBlocking {
        val now = Instant.parse("2026-11-09T10:00:00Z")
        val repository = SystemFactRepository(log, Clock.fixed(now, ZoneOffset.UTC))
        val application = FactStoreApplication(CreateStoreSlice(createStoreDecider, repository))

        application.createStore(orders)

        val query = LogQuery(LogQueryItem(SystemStore.ID, FactQueryItem(types = setOf(SystemFactTypes.STORE_CREATED))))
        assertThat(log.read(query).toList().single().fact.appendedAt).isEqualTo(now)
    }

    @Test
    fun `fails when a fact it read is gone from the log`() {
        val vanishingLog = FactsVanishOnAppend(log)

        assertThatThrownBy { runBlocking { application(vanishingLog, listOf(StoreId.generate())).createStore(orders) } }
            .isInstanceOf(IllegalStateException::class.java)
    }

    /** Holds the first two reads back until both have happened, so both decisions see the name as free. */
    private class BothReadFirst(private val log: FactLog) : FactLog by log {
        private val reads = AtomicInteger()
        private val bothRead = CompletableDeferred<Unit>()

        override fun read(query: LogQuery): Flow<LogFact> = flow {
            val facts = log.read(query).toList()
            val count = reads.incrementAndGet()
            if (count == 2) bothRead.complete(Unit)
            if (count <= 2) bothRead.await()
            emitAll(facts.asFlow())
        }
    }

    /** Answers every append as if a fact the decision read had been erased in the meantime. */
    private class FactsVanishOnAppend(private val log: FactLog) : FactLog by log {

        override suspend fun append(facts: Flow<LogFact>, condition: LogCondition): LogAppendResult =
            LogAppendResult.AfterFactNotFound(FactId.generate())
    }
}
