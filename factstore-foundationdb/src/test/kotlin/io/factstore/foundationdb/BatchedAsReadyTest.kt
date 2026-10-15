package io.factstore.foundationdb

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

class BatchedAsReadyTest {

    @Test
    fun `elements that are ready are grouped, in order and within the size`(): Unit = runBlocking {
        val batches = (1..10).asFlow().batchedAsReady(maxSize = 4).toList()

        // Where later batches are cut depends on how far reading has got, but the elements read
        // ahead before the first batch is built fill it.
        assertThat(batches.first()).containsExactly(1, 2, 3, 4)
        assertThat(batches).allMatch { it.size in 1..4 }
        assertThat(batches.flatten()).containsExactlyElementsOf(1..10)
    }

    @Test
    fun `an element is not held back while the next one is slow to arrive`(): Unit = runBlocking {
        val more = CompletableDeferred<Unit>()
        val elements = flow {
            emit(1)
            more.await()
            emit(2)
        }

        // Waiting for a batch to fill, the first batch would never come: the second element
        // arrives only once the first batch is out.
        val first = withTimeout(5_000.milliseconds) { elements.batchedAsReady(maxSize = 256).first() }

        assertThat(first).containsExactly(1)
    }

    @Test
    fun `every element arrives, however slowly they come`(): Unit = runBlocking {
        val gates = List(3) { CompletableDeferred<Unit>() }
        val elements = flow {
            gates.forEachIndexed { i, gate ->
                gate.await()
                emit(i)
            }
        }
        launch { gates.forEach { it.complete(Unit) } }

        val batches = withTimeout(5_000.milliseconds) { elements.batchedAsReady(maxSize = 256).toList() }

        assertThat(batches.flatten()).containsExactly(0, 1, 2)
    }

    @Test
    fun `a failure reading the elements is rethrown`(): Unit = runBlocking {
        val elements = flow {
            emit(1)
            throw IllegalStateException("broken")
        }

        assertThatThrownBy { runBlocking { elements.batchedAsReady(maxSize = 2).toList() } }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessage("broken")
    }
}
