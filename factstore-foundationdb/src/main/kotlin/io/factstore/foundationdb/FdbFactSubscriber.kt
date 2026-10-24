package io.factstore.foundationdb

import com.apple.foundationdb.*
import io.factstore.core.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

/**
 * The number of facts a subscription reads per transaction, at most.
 *
 * A subscription reads whatever is there: large batches while it catches up with a long history,
 * and once it follows the tail, the facts appended since its last read. Facts are not sized by
 * bytes yet, so at the largest fact size a full batch is large.
 */
private const val SUBSCRIPTION_BATCH_SIZE = 10_000

/**
 * The number of batches a subscription reads ahead of its consumer.
 *
 * Reading ahead lets the next batch be read while the consumer processes the current one.
 */
private const val SUBSCRIPTION_PREFETCH_BATCHES = 4

/**
 * Follows a store's facts as they are appended: a [FactSubscriber] on FoundationDB.
 *
 * A subscription reads the store's facts in their own order, which is the order they are stored
 * in, so it needs no index. When it has read them all, it waits for the next append by watching
 * the store's head, and it never ends on its own.
 */
internal class FdbFactSubscriber(
    storage: FdbStorage,
) : FactSubscriber, FdbAccess by storage {

    override suspend fun subscribe(request: SubscribeRequest): SubscribeResult {
        val start = when (val start = start(request.storeName, request.startPosition)) {
            Start.StoreMissing -> return SubscribeResult.StoreNotFound(request.storeName)
            is Start.ContinuationMissing -> return SubscribeResult.ContinuationNotFound(start.factId)
            is Start.Resolved -> start
        }
        val facts = follow(start.storeId, start.afterKey)
            .toFacts(SUBSCRIPTION_PREFETCH_BATCHES)
        return SubscribeResult.FactStream(facts)
    }

    /** Where a subscription starts: the store, and the key of the fact it continues after. */
    private sealed interface Start {
        data object StoreMissing : Start
        data class ContinuationMissing(val factId: FactId) : Start

        /** [afterKey] is the key of the fact to continue after, or `null` for the store's beginning. */
        class Resolved(val storeId: StoreId, val afterKey: ByteArray?) : Start
    }

    /**
     * Resolves the store and the fact a subscription continues after in one read, as a stream
     * pins its start, so that both come from the same moment.
     */
    private suspend fun start(storeName: StoreName, position: StartPosition): Start = inReadTransaction {
        val storeId = storeIdOf(storeName) ?: return@inReadTransaction Start.StoreMissing
        val after = when (position) {
            StartPosition.Beginning -> null
            // An empty store has no head, and its end is its beginning.
            StartPosition.End -> headOf(storeId)
            is StartPosition.ContinueAfter -> positionOf(storeId, position.factId)
                ?: return@inReadTransaction Start.ContinuationMissing(position.factId)
        }
        Start.Resolved(storeId, after?.let { keyspace.factSubspace.getFactKey(storeId, it) })
    }

    /**
     * The serialized facts of the store after [afterKey], batch by batch, for ever.
     *
     * Each batch is read in its own transaction. When nothing new is there, the transaction
     * watches the store's head instead, and the next batch is read once an append moves it. A
     * watch only takes effect when its transaction commits, which is why this is a transaction
     * that may write, although it never does.
     */
    private fun follow(storeId: StoreId, afterKey: ByteArray?): Flow<List<ByteArray>> = flow {
        val facts = keyspace.factSubspace.getRange(storeId)
        var cursor = afterKey
        while (true) {
            val next = inTransaction {
                val entries = readAfter(facts, cursor)
                if (entries.isEmpty()) Next.Wait(watch(keyspace.headSubspace.headKey(storeId)))
                else Next.Batch(entries)
            }
            when (next) {
                is Next.Batch -> {
                    cursor = next.entries.last().key
                    emit(next.entries.map { it.value })
                }
                is Next.Wait -> next.headMoved.await()
            }
        }
    }

    /** What a subscription does next: emit a batch, or wait for the head to move. */
    private sealed interface Next {
        class Batch(val entries: List<KeyValue>) : Next
        class Wait(val headMoved: CompletableFuture<Void>) : Next
    }

    /**
     * Up to [SUBSCRIPTION_BATCH_SIZE] entries of [range] after [cursor], or from its start without one.
     *
     * The read is a snapshot: a subscription only follows the store, and must never make an
     * append conflict.
     */
    context(tr: ReadTransaction)
    private suspend fun readAfter(range: Range, cursor: ByteArray?): List<KeyValue> =
        tr.snapshot().getRange(
            cursor?.let { KeySelector.firstGreaterThan(it) } ?: KeySelector.firstGreaterOrEqual(range.begin),
            KeySelector.firstGreaterOrEqual(range.end),
            SUBSCRIPTION_BATCH_SIZE,
            false,
            StreamingMode.WANT_ALL,
        ).asList().await()
}
