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
        factStoreId: FactStoreId,
        streamingOptions: StreamingOptions
    ): StreamResult {

        // Check existence
        val factStoreExists = read { tr ->
            store.context.getMetadata(factStoreId, tr)
                .thenApply { it != null }
        }

        if (!factStoreExists) {
            return FactStoreNotFound
        }

        // Resolve cursor safely
        val cursorResult = with(factStoreId) {
            resolveInitialCursor(streamingOptions.startPosition)
        }

        val initialCursor = when (cursorResult) {
            is CursorResult.Found -> cursorResult.key
            is CursorResult.FactNotFound -> return InvalidStartPosition(cursorResult.factId)
            CursorResult.Beginning -> null
        }

        val globalRange = store.context.globalFactPositionSubspace
            .range(Tuple.from(factStoreId.uuid))
        val flow = streamFacts(initialCursor, globalRange, factStoreId)

        return FactStream(flow)
    }

    private fun streamFacts(
        initialCursor: ByteArray?,
        globalRange: Range,
        factStoreId: FactStoreId
    ): Flow<Fact> = flow {

        var lastSeenKey = initialCursor

        while (true) {

            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, tr)
                    .thenApply { batch -> batch.toReadResult(tr, factStoreId) }
            }.await()

            when (readResult) {
                is ReadResult.BatchResult -> {
                    val lastFact = readResult.batch.last()
                    lastSeenKey = with(factStoreId) {
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

    context(factStoreId: FactStoreId)
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

    context(factStoreId: FactStoreId)
    private suspend fun getKeyForFactOrNull(factId: FactId): ByteArray? =
        read { tr ->
            with(tr) {
                factId.loadFact().thenApply { fdbFact ->
                    fdbFact?.getFactPositionKey()
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

    context(factStoreId: FactStoreId)
    private fun FdbFact.getFactPositionKey(): ByteArray =
        factPosition.getFactPositionKey()

    context(factStoreId: FactStoreId)
    private fun FactPosition.getFactPositionKey(): ByteArray =
        store.context.globalFactPositionSubspace.pack(
            Tuple.from(factStoreId.uuid, this)
        )

    context(factStoreId: FactStoreId)
    private suspend fun getCurrentEndKey(): ByteArray? =
        read { tr ->
            store.getHead(factStoreId, tr)
                .thenApply { factPosition ->
                    factPosition?.getFactPositionKey()
                }
        }

    context(transaction: ReadTransaction, factStoreId: FactStoreId)
    private fun FactId.loadFact(): CompletableFuture<FdbFact?> =
        with(store) {
            this@loadFact.loadFactById()
        }

    private suspend fun <T> read(trBlock: (ReadTransaction) -> CompletableFuture<T>): T =
        store.db.readAsync(trBlock).await()

    private fun List<FdbFact>.toReadResult(tr: Transaction, factStoreId: FactStoreId): ReadResult =
        if (isEmpty()) {
            ReadResult.WatchResult(
                tr.watch(store.context.headSubspace.pack(factStoreId.uuid))
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
