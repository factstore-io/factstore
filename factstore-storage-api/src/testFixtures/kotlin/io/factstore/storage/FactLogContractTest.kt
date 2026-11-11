package io.factstore.storage

import io.factstore.core.FactId
import io.factstore.core.FactInput
import io.factstore.core.FactPayload
import io.factstore.core.FactQueryItem
import io.factstore.core.FactType
import io.factstore.core.StoreId
import io.factstore.core.Subject
import io.factstore.core.toFact
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Instant

abstract class FactLogContractTest {

    protected abstract fun createLog(): FactLog

    private val log: FactLog by lazy { createLog() }
    private val s1 = StoreId.generate()
    private val s2 = StoreId.generate()

    private val orderPlaced = FactType("OrderPlaced")
    private val orderShipped = FactType("OrderShipped")

    private fun fact(storeId: StoreId, type: FactType) =
        LogFact(storeId, FactInput(type, Subject("order/1"), FactPayload(ByteArray(0))).toFact(FactId.generate(), Instant.now()))

    private fun ofType(type: FactType, storeIds: List<StoreId>) =
        LogQuery(storeIds.map { LogQueryItem(it, FactQueryItem(types = setOf(type))) })

    private suspend fun LogAppendResult.appended(): List<LogFact> = (this as LogAppendResult.Appended).facts.toList()

    private suspend fun appendUnconditionally(storeId: StoreId, type: FactType): LogFact =
        (log.append(flowOf(fact(storeId, type)), LogCondition.None) as LogAppendResult.Appended).facts.single()

    @Test
    fun `stores facts exactly as given and reads them back in order`(): Unit = runBlocking {
        val given = listOf(fact(s1, orderPlaced), fact(s1, orderPlaced))

        val appended = log.append(given.asFlow(), LogCondition.None).appended()

        assertThat(appended).isEqualTo(given)
        assertThat(log.read(ofType(orderPlaced, listOf(s1))).toList()).isEqualTo(given)
    }

    @Test
    fun `appends nothing for no facts`(): Unit = runBlocking {
        val appended = log.append(emptyFlow(), LogCondition.None).appended()

        assertThat(appended).isEmpty()
        assertThat(log.read(ofType(orderPlaced, listOf(s1))).toList()).isEmpty()
    }

    @Test
    fun `appends nothing when the facts fail while they are collected`(): Unit = runBlocking {
        val failing = flow {
            emit(fact(s1, orderPlaced))
            error("The facts could not be produced.")
        }

        assertThatThrownBy { runBlocking { log.append(failing, LogCondition.None) } }
            .isInstanceOf(IllegalStateException::class.java)
        assertThat(log.read(ofType(orderPlaced, listOf(s1))).toList()).isEmpty()
    }

    @Test
    fun `reads nothing until the read is collected`(): Unit = runBlocking {
        val read = log.read(ofType(orderPlaced, listOf(s1)))
        val appended = appendUnconditionally(s1, orderPlaced)

        assertThat(read.toList()).containsExactly(appended)
    }

    @Test
    fun `reads anew on every collection`(): Unit = runBlocking {
        val read = log.read(ofType(orderPlaced, listOf(s1)))
        val first = appendUnconditionally(s1, orderPlaced)
        val beforeSecond = read.toList()
        val second = appendUnconditionally(s1, orderPlaced)

        assertThat(beforeSecond).containsExactly(first)
        assertThat(read.toList()).containsExactly(first, second)
    }

    @Test
    fun `reads only the stores the query names, in one order`(): Unit = runBlocking {
        val inS1 = appendUnconditionally(s1, orderPlaced)
        val inS2 = appendUnconditionally(s2, orderPlaced)

        assertThat(log.read(ofType(orderPlaced, listOf(s1))).toList()).containsExactly(inS1)
        assertThat(log.read(ofType(orderPlaced, listOf(s2, s1))).toList()).containsExactly(inS1, inS2)
    }

    @Test
    fun `reads every fact with the store it belongs to`(): Unit = runBlocking {
        appendUnconditionally(s1, orderPlaced)
        appendUnconditionally(s2, orderPlaced)

        assertThat(log.read(ofType(orderPlaced, listOf(s1, s2))).toList().map { it.storeId }).containsExactly(s1, s2)
    }

    @Test
    fun `appends to several stores together`(): Unit = runBlocking {
        val appended = log.append(flowOf(fact(s1, orderPlaced), fact(s2, orderShipped)), LogCondition.None).appended()

        assertThat(log.read(ofType(orderPlaced, listOf(s1))).toList()).containsExactly(appended[0])
        assertThat(log.read(ofType(orderShipped, listOf(s2))).toList()).containsExactly(appended[1])
    }

    @Test
    fun `rejects an append when a matching fact was appended after the one read`(): Unit = runBlocking {
        val last = appendUnconditionally(s1, orderPlaced)
        appendUnconditionally(s1, orderPlaced)

        val result = log.append(flowOf(fact(s1, orderPlaced)), LogCondition.NoneMatch(ofType(orderPlaced, listOf(s1)), last.fact.id))

        assertThat(result).isEqualTo(LogAppendResult.ConditionViolated)
    }

    @Test
    fun `rejects an append that expects no matching fact when there is one`(): Unit = runBlocking {
        appendUnconditionally(s1, orderPlaced)

        val result = log.append(flowOf(fact(s1, orderPlaced)), LogCondition.NoneMatch(ofType(orderPlaced, listOf(s1)), after = null))

        assertThat(result).isEqualTo(LogAppendResult.ConditionViolated)
    }

    @Test
    fun `applies an append when only facts outside the query were appended since`(): Unit = runBlocking {
        val first = appendUnconditionally(s1, orderPlaced)
        appendUnconditionally(s1, orderShipped)
        appendUnconditionally(s2, orderPlaced)

        val result = log.append(flowOf(fact(s1, orderPlaced)), LogCondition.NoneMatch(ofType(orderPlaced, listOf(s1)), first.fact.id))

        assertThat(result).isInstanceOf(LogAppendResult.Appended::class.java)
    }

    @Test
    fun `reports a fact to read after that the log does not hold`(): Unit = runBlocking {
        val unknown = FactId.generate()

        val result = log.append(flowOf(fact(s1, orderPlaced)), LogCondition.NoneMatch(ofType(orderPlaced, listOf(s1)), unknown))

        assertThat(result).isEqualTo(LogAppendResult.AfterFactNotFound(unknown))
    }
}
