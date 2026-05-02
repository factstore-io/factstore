package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import io.factstore.core.*
import io.factstore.core.StreamResult.*
import java.util.concurrent.CompletableFuture

const val DEFAULT_BATCH_SIZE = 5000

class FdbFactStreamer(
    private val store: FdbFactStore
) : FactStreamer {

    override suspend fun stream(
        storeName: StoreName,
        streamingOptions: StreamingOptions
    ): StreamResult {

        // Check existence
        val storeId = read { tr ->
            with(tr) {
                store.context.lookUpStoreIdByName(storeName)
            }
        }

        if (storeId == null) {
            return StoreNotFound
        }

        // Resolve cursor safely
        val cursorResult = with(storeId) {
            resolveInitialCursor(streamingOptions.startPosition)
        }

        val initialCursor = when (cursorResult) {
            is CursorResult.Found -> cursorResult.key
            is CursorResult.FactNotFound -> return InvalidStartPosition(cursorResult.factId)
            CursorResult.Beginning -> null
        }

        val globalRange = store.context.factSubspace.getRange(storeId)
        val flow = streamFacts(initialCursor, globalRange, storeId)

        return FactStream(flow)
    }

    private fun streamFacts(
        initialCursor: ByteArray?,
        globalRange: Range,
        storeId: StoreId
    ): Flow<Fact> = flow {

        var lastSeenKey = initialCursor

        while (true) {

            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, tr)
                    .thenApply { batch -> batch.toReadResult(tr, storeId) }
            }.await()

            when (readResult) {
                is ReadResult.BatchResult -> {
                    val lastFact = readResult.batch.last()
                    lastSeenKey = with(storeId) {
                        lastFact.getFactPositionKey()
                    }

                    for (fdbFact in readResult.batch) {
                        emit(fdbFact.fact)
                    }
                }

                is ReadResult.WatchResult -> {
                    readResult.watch.await()
                }
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
    private suspend fun resolveInitialCursor(
        startPosition: StartPosition,
    ): CursorResult =
        when (startPosition) {
            StartPosition.Beginning -> CursorResult.Beginning
            StartPosition.End -> {
                val key = getCurrentEndKey()
                if (key == null) CursorResult.Beginning
                else CursorResult.Found(key)
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
                store.context.factPositionIndexSubspace.getPosition(storeId, factId).thenApply { position ->
                    position?.getFactPositionKey()
                }
            }
        }

    // -------------------------------------------------------------------------
    // Streaming internals
    // -------------------------------------------------------------------------

    private fun readNextBatch(
        lastSeenKey: ByteArray?,
        globalRange: Range,
        tr: ReadTransaction
    ): CompletableFuture<List<FdbFact>> {

        val beginSelector =
            if (lastSeenKey == null)
                KeySelector.firstGreaterOrEqual(globalRange.begin)
            else
                KeySelector.firstGreaterThan(lastSeenKey)

        val endSelector = KeySelector.firstGreaterOrEqual(globalRange.end)

        return tr.getRange(beginSelector, endSelector, DEFAULT_BATCH_SIZE)
            .asList()
            .thenApply { keyValues ->
                keyValues.map { kv ->
                    val keyTuple = Tuple.fromBytes(kv.key)
                    val factPosition = keyTuple.getLastAsFactPosition()
                    val fact = kv.value.toSerializableFdbFact().toFact()

                    FdbFact(
                        fact = fact,
                        factPosition = factPosition
                    )
                }
            }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    context(storeId: StoreId)
    private fun FdbFact.getFactPositionKey(): ByteArray =
        factPosition.getFactPositionKey()

    context(storeId: StoreId)
    private fun FactPosition.getFactPositionKey(): ByteArray =
        store.context.factSubspace.getFactKey(storeId, this)

    context(storeId: StoreId)
    private suspend fun getCurrentEndKey(): ByteArray? =
        read { tr ->
            store.getHead(storeId, tr)
                .thenApply { factPosition ->
                    factPosition?.getFactPositionKey()
                }
        }

    private suspend fun <T> read(trBlock: (ReadTransaction) -> CompletableFuture<T>): T =
        store.db.readAsync(trBlock).await()

    private fun List<FdbFact>.toReadResult(tr: Transaction, storeId: StoreId): ReadResult =
        if (isEmpty()) {
            ReadResult.WatchResult(
                tr.watch(store.context.headSubspace.headKey(storeId))
            )
        } else {
            ReadResult.BatchResult(this)
        }

    // -------------------------------------------------------------------------
    // Internal types
    // -------------------------------------------------------------------------

    private sealed interface ReadResult {
        data class BatchResult(val batch: List<FdbFact>) : ReadResult
        data class WatchResult(val watch: CompletableFuture<Void>) : ReadResult
    }
}
