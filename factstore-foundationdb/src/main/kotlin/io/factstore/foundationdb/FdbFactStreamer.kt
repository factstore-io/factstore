package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.StreamingMode
import com.apple.foundationdb.Transaction
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import io.factstore.core.*
import io.factstore.core.StreamResult.*
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.BufferOverflow.SUSPEND
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import java.util.concurrent.CompletableFuture

const val DEFAULT_BATCH_SIZE = 10_000
const val RAW_CHANNEL_CAPACITY = 4

class FdbFactStreamer(
    private val store: FdbFactStore,
    private val deserializationDispatcher: CoroutineDispatcher = Dispatchers.Default,
) : FactStreamer {

    override suspend fun stream(request: StreamFactsRequest): StreamResult {
        val storeName = request.storeName

        val storeId = read { tr ->
            with(tr) { store.context.lookUpStoreIdByName(storeName) }
        }

        if (storeId == null) {
            return StoreNotFound(storeName)
        }

        val cursorResult = with(storeId) {
            resolveInitialCursor(request.startPosition)
        }

        val initialCursor = when (cursorResult) {
            is CursorResult.Found -> cursorResult.key
            is CursorResult.FactNotFound -> return FactIdNotFound(cursorResult.factId)
            CursorResult.Beginning -> null
        }

        val globalRange = store.context.factSubspace.getRange(storeId)

        return FactStream(streamFacts(initialCursor, globalRange, storeId))
    }

    private fun streamFacts(
        initialCursor: ByteArray?,
        globalRange: Range,
        storeId: StoreId
    ): Flow<List<Fact>> =
        launchStreamer(initialCursor, globalRange, storeId)
            .buffer(
                capacity = RAW_CHANNEL_CAPACITY,
                onBufferOverflow = SUSPEND
            )
            .map { keyValues ->
                withContext(deserializationDispatcher) {
                    keyValues.map { it.value.toSerializableFdbFact().toFact() }
                }
            }

    private fun launchStreamer(
        initialCursor: ByteArray?,
        globalRange: Range,
        storeId: StoreId
    ): Flow<List<KeyValue>> = flow {
        var lastSeenKey = initialCursor
        while (true) {
            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, tr)
                    .thenApply { batch -> batch.toReadResult(tr, storeId) }
            }.await()
            when (readResult) {
                is ReadResult.BatchResult -> {
                    lastSeenKey = readResult.batch.last().key
                    emit(readResult.batch)
                }

                is ReadResult.WatchResult -> readResult.waitForFacts()
            }
        }
    }

    // -------------------------------------------------------------------------
    // Cursor handling
    // -------------------------------------------------------------------------

    sealed interface CursorResult {
        data object Beginning : CursorResult

        @JvmInline
        value class Found(val key: ByteArray) : CursorResult

        @JvmInline
        value class FactNotFound(val factId: FactId) : CursorResult
    }

    context(storeId: StoreId)
    private suspend fun resolveInitialCursor(startPosition: StartPosition): CursorResult =
        when (startPosition) {
            StartPosition.Beginning -> CursorResult.Beginning
            StartPosition.End -> {
                val key = getCurrentEndKey()
                if (key == null) CursorResult.Beginning else CursorResult.Found(key)
            }

            is StartPosition.After -> {
                val key = getKeyForFactOrNull(startPosition.factId)
                if (key == null) CursorResult.FactNotFound(startPosition.factId)
                else CursorResult.Found(key)
            }
        }

    context(storeId: StoreId)
    private suspend fun getKeyForFactOrNull(factId: FactId): ByteArray? =
        read { tr ->
            with(tr) {
                store.context.factPositionIndexSubspace.getPosition(storeId, factId)
                    .thenApply { position ->
                        position?.let { store.context.factSubspace.getFactKey(storeId, it) }
                    }
            }
        }

    context(storeId: StoreId)
    private suspend fun getCurrentEndKey(): ByteArray? =
        read { tr ->
            store.getHead(storeId, tr)
                .thenApply { position ->
                    position?.let { store.context.factSubspace.getFactKey(storeId, it) }
                }
        }

    // -------------------------------------------------------------------------
    // Streaming internals
    // -------------------------------------------------------------------------

    private fun readNextBatch(
        lastSeenKey: ByteArray?,
        globalRange: Range,
        tr: ReadTransaction
    ): CompletableFuture<List<KeyValue>> {
        val beginSelector =
            if (lastSeenKey == null)
                KeySelector.firstGreaterOrEqual(globalRange.begin)
            else
                KeySelector.firstGreaterThan(lastSeenKey)

        val endSelector = KeySelector.firstGreaterOrEqual(globalRange.end)

        return tr.snapshot().getRange(
            beginSelector,
            endSelector,
            DEFAULT_BATCH_SIZE,
            false,
            StreamingMode.WANT_ALL
        ).asList()
    }

    private fun List<KeyValue>.toReadResult(tr: Transaction, storeId: StoreId): ReadResult =
        if (isEmpty()) {
            val watchFuture = tr.watch(store.context.headSubspace.headKey(storeId))
            ReadResult.WatchResult { watchFuture.await() }
        } else {
            ReadResult.BatchResult(this)
        }

    // -------------------------------------------------------------------------
    // Internal types
    // -------------------------------------------------------------------------

    private sealed interface ReadResult {
        data class BatchResult(val batch: List<KeyValue>) : ReadResult
        data class WatchResult(val waitForFacts: suspend () -> Unit) : ReadResult
    }

    private suspend fun <T> read(trBlock: (ReadTransaction) -> CompletableFuture<T>): T =
        store.db.readAsync(trBlock).await()
}
