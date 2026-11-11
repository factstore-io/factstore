package io.factstore.application

import io.factstore.application.slices.CreateStoreRequest
import io.factstore.application.slices.CreateStoreRequestEncoding
import io.factstore.application.slices.CreateStoreSlice
import io.factstore.application.slices.toCommand
import io.factstore.core.FactId
import io.factstore.core.FactQueryItem
import io.factstore.core.Fingerprint
import io.factstore.core.IdempotencyKey
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.domain.CreateStore
import io.factstore.domain.StoreCreated
import io.factstore.domain.StoreCreationRejected
import io.factstore.domain.SystemFact
import io.factstore.domain.createStoreDecider
import io.factstore.storage.FactLog
import io.factstore.storage.LogQuery
import io.factstore.storage.LogQueryItem
import io.factstore.storage.memory.InMemoryFactLog
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class CreateStoreIdempotencyTest {

    private val log = InMemoryFactLog()
    private val slice = CreateStoreSlice(createStoreDecider, SystemFactRepository(log))
    private val orders = StoreName("orders")

    private val requestA = Fingerprint("request-a")
    private val requestB = Fingerprint("request-b")

    private fun key(fingerprint: Fingerprint = requestA) = Idempotency(IdempotencyKey.generate(), fingerprint)

    private suspend fun DecisionResult<SystemFact, FactId>.recorded(): List<SystemFact> =
        (this as DecisionResult.Recorded).facts.toList().map { it.fact }

    private suspend fun DecisionResult<SystemFact, FactId>.alreadyRecorded(): List<SystemFact> =
        (this as DecisionResult.AlreadyRecorded).facts.toList().map { it.fact }

    private suspend fun FactLog.systemFacts(): List<SystemFact> {
        val types = SystemFactTypes.run { setOf(STORE_CREATED, STORE_CREATION_REJECTED) }
        return read(LogQuery(LogQueryItem(SystemStore.ID, FactQueryItem(types = types)))).toList()
            .map { SystemFactCodec.decode(it.fact) }
    }

    @Test
    fun `replays the decision first recorded under a key, even though a retry would decide otherwise`(): Unit = runBlocking {
        val s1 = StoreId.generate()
        val s2 = StoreId.generate()
        val key = key()
        slice.handle(CreateStore(s1, orders), key)

        val retry = slice.handle(CreateStore(s2, orders), key)

        assertThat(retry.alreadyRecorded()).containsExactly(StoreCreated(s1, orders))
        assertThat(log.systemFacts()).containsExactly(StoreCreated(s1, orders))
    }

    @Test
    fun `replays a recorded rejection`(): Unit = runBlocking {
        val s1 = StoreId.generate()
        val s2 = StoreId.generate()
        slice.handle(CreateStore(s1, orders), key())
        val key = key()
        val rejection = slice.handle(CreateStore(s2, orders), key).recorded()

        val retry = slice.handle(CreateStore(s2, orders), key)

        assertThat(retry.alreadyRecorded()).isEqualTo(rejection)
        assertThat(log.systemFacts()).containsExactly(StoreCreated(s1, orders), *rejection.toTypedArray())
    }

    @Test
    fun `rejects a used key for another request, and records nothing`(): Unit = runBlocking {
        val s1 = StoreId.generate()
        val key = key(requestA)
        slice.handle(CreateStore(s1, orders), key)

        val result = slice.handle(CreateStore(StoreId.generate(), StoreName("invoices")), key.copy(fingerprint = requestB))

        assertThat(result).isEqualTo(DecisionResult.IdempotencyKeyReused(key.key))
        assertThat(log.systemFacts()).containsExactly(StoreCreated(s1, orders))
    }

    @Test
    fun `leaves a key free when the condition is violated, so the next attempt records its decision`(): Unit = runBlocking {
        val s1 = StoreId.generate()
        val s2 = StoreId.generate()
        val racing = CreateStoreSlice(createStoreDecider, SystemFactRepository(BothReadFirst(log)))
        val keys = mapOf(s1 to key(), s2 to key())

        val outcomes = withTimeout(5_000.milliseconds) {
            keys.map { (id, key) -> async(Dispatchers.Default) { id to racing.handle(CreateStore(id, orders), key) } }.awaitAll()
        }
        val (loser, _) = outcomes.single { (_, outcome) -> outcome == DecisionResult.ConditionViolated }
        val winner = keys.keys.single { it != loser }

        val nextAttempt = slice.handle(CreateStore(loser, orders), keys.getValue(loser))

        assertThat(nextAttempt.recorded())
            .containsExactly(StoreCreationRejected(loser, orders, StoreCreationRejected.Reason.NameTaken(holder = winner)))
    }

    @Test
    fun `replays a retried request, although its command is for another store`(): Unit = runBlocking {
        val request = CreateStoreRequest(orders)
        val key = Idempotency(IdempotencyKey.generate(), CreateStoreRequestEncoding.fingerprint(request))
        val first = slice.handle(request.toCommand(StoreIdGenerator(StoreId::generate)), key).recorded()

        val retry = slice.handle(request.toCommand(StoreIdGenerator(StoreId::generate)), key)

        assertThat(retry.alreadyRecorded()).isEqualTo(first)
        assertThat(log.systemFacts()).isEqualTo(first)
    }

    @Test
    fun `rejects a key used for a request for another name`(): Unit = runBlocking {
        val first = CreateStoreRequest(orders)
        val key = Idempotency(IdempotencyKey.generate(), CreateStoreRequestEncoding.fingerprint(first))
        slice.handle(first.toCommand(StoreIdGenerator(StoreId::generate)), key)

        val other = CreateStoreRequest(StoreName("invoices"))
        val result = slice.handle(
            other.toCommand(StoreIdGenerator(StoreId::generate)),
            key.copy(fingerprint = CreateStoreRequestEncoding.fingerprint(other)),
        )

        assertThat(result).isEqualTo(DecisionResult.IdempotencyKeyReused(key.key))
    }
}
