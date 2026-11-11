package io.factstore.application

import io.factstore.application.slices.CreateStoreRequest
import io.factstore.application.slices.CreateStoreRequestHandler
import io.factstore.application.slices.CreateStoreSlice
import io.factstore.core.FactId
import io.factstore.core.IdempotencyKey
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
import io.factstore.storage.LogIdempotency
import io.factstore.storage.memory.InMemoryFactLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds

class RequestHandlerTest {

    private val log = InMemoryFactLog()
    private val orders = StoreName("orders")

    private val withoutWaits = RetryPolicy { failedAttempts -> if (failedAttempts < 3) Duration.ZERO else null }

    /** A handler whose generator names [first] first, and a new store on every later call. */
    private fun handler(log: FactLog, first: StoreId = StoreId.generate(), retryPolicy: RetryPolicy = withoutWaits): CreateStoreRequestHandler {
        val ids = generateSequence(first) { StoreId.generate() }.iterator()
        return CreateStoreRequestHandler(CreateStoreSlice(createStoreDecider, SystemFactRepository(log)), ids::next, retryPolicy)
    }

    private fun envelope(storeName: StoreName = orders) = Envelope(CreateStoreRequest(storeName), IdempotencyKey.generate())

    private suspend fun DecisionResult<SystemFact, FactId>.recorded(): List<SystemFact> =
        (this as DecisionResult.Recorded).facts.toList().map { it.fact }

    @Test
    fun `attempts a decision again when its condition was violated, about the same store`(): Unit = runBlocking {
        val s1 = StoreId.generate()
        val s2 = StoreId.generate()
        val racing = BothReadFirst(log)

        val outcomes = withTimeout(5_000) {
            listOf(s1, s2).map { id -> async(Dispatchers.Default) { handler(racing, id).handle(envelope()).recorded().single() } }.awaitAll()
        }

        val winner = outcomes.filterIsInstance<StoreCreated>().single().storeId
        val loser = listOf(s1, s2).single { it != winner }
        assertThat(outcomes).contains(StoreCreationRejected(loser, orders, StoreCreationRejected.Reason.NameTaken(holder = winner)))
    }

    @Test
    fun `answers a violated condition once the policy gives up`(): Unit = runBlocking {
        val violating = AlwaysViolated(log)

        val result = handler(violating).handle(envelope())

        assertThat(result).isEqualTo(DecisionResult.ConditionViolated)
        assertThat(violating.appends.get()).isEqualTo(3)
    }

    @Test
    fun `does not attempt again what was recorded, replayed or rejected for a reused key`(): Unit = runBlocking {
        val counting = CountingAppends(log)
        val handler = handler(counting)
        val first = envelope()

        assertThat(handler.handle(first)).isInstanceOf(DecisionResult.Recorded::class.java)
        assertThat(handler.handle(first)).isInstanceOf(DecisionResult.AlreadyRecorded::class.java)
        assertThat(handler.handle(first.copy(request = CreateStoreRequest(StoreName("invoices")))))
            .isEqualTo(DecisionResult.IdempotencyKeyReused(first.idempotencyKey))
        assertThat(counting.appends.get()).isEqualTo(3)
    }

    @Test
    fun `waits as long as the policy says before each new attempt`() = runTest {
        val waits = listOf(5.seconds, 7.seconds)
        val policy = RetryPolicy { failedAttempts -> waits.getOrNull(failedAttempts - 1) }

        handler(AlwaysViolated(log), retryPolicy = policy).handle(envelope())

        assertThat(currentTime).isEqualTo(12_000)
    }

    @Test
    fun `stops attempting when it is cancelled`() = runTest {
        val violating = AlwaysViolated(log)
        val handling = launch { handler(violating, retryPolicy = { 1.hours }).handle(envelope()) }
        advanceTimeBy(2.5.hours)

        handling.cancelAndJoin()
        val attemptsBeforeCancel = violating.appends.get()
        advanceTimeBy(10.hours)

        assertThat(attemptsBeforeCancel).isEqualTo(3)
        assertThat(violating.appends.get()).isEqualTo(attemptsBeforeCancel)
    }


    /** Counts every append, and passes it on to [log]. */
    private open class CountingAppends(private val log: FactLog) : FactLog by log {
        val appends = AtomicInteger()

        override suspend fun append(facts: Flow<LogFact>, condition: LogCondition, idempotency: LogIdempotency): LogAppendResult {
            appends.incrementAndGet()
            return answer(facts, condition, idempotency)
        }

        protected open suspend fun answer(facts: Flow<LogFact>, condition: LogCondition, idempotency: LogIdempotency) =
            log.append(facts, condition, idempotency)
    }

    /** Answers every append as if a matching fact had been appended in the meantime. */
    private class AlwaysViolated(log: FactLog) : CountingAppends(log) {
        override suspend fun answer(facts: Flow<LogFact>, condition: LogCondition, idempotency: LogIdempotency) =
            LogAppendResult.ConditionViolated
    }
}
