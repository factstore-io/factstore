package io.factstore.foundationdb

import io.factstore.core.Fact
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*

/**
 * Groups the elements into batches of at most [maxSize], never waiting for a batch to fill.
 *
 * The elements are read ahead in a coroutine of their own, and a batch holds those that are
 * ready when it is built. Elements that arrive faster than the batches are consumed queue up,
 * and come out as full batches; elements that trickle in come out in small ones, without being
 * held back for others that may take long to arrive.
 */
internal fun <T> Flow<T>.batchedAsReady(maxSize: Int): Flow<List<T>> {
    require(maxSize > 0) { "The batch size must be positive, but was $maxSize." }
    return flow {
        coroutineScope {
            val ready = buffer(maxSize).produceIn(this)
            while (true) {
                val received = ready.receiveCatching()
                received.exceptionOrNull()?.let { throw it }
                val first = received.getOrNull() ?: break
                val batch = mutableListOf(first)
                while (batch.size < maxSize) {
                    // Takes what is already there, and never waits for more.
                    batch += ready.tryReceive().getOrNull() ?: break
                }
                emit(batch)
            }
        }
    }
}

/**
 * Turns batches of serialized facts into facts.
 *
 * The batches are read in their own coroutine, up to [readAhead] of them ahead of the consumer,
 * so reading the next batches overlaps with processing the current one.
 *
 * Batches are how a store is read, not how its facts are consumed: each fact is emitted on its
 * own, and batching is left to whoever transports it.
 */
internal fun Flow<List<ByteArray>>.toFacts(readAhead: Int): Flow<Fact> =
    map { batch -> batch.map { it.toSerializableFdbFact().toFact() } }
        .flowOn(Dispatchers.Default)
        .buffer(readAhead)
        .transform { facts -> facts.forEach { emit(it) } }
