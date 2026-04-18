package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
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

        val globalRange = store.context.globalFactPositionSubspace
            .range(Tuple.from(factStoreId.uuid))

        // Check existence
        val exists = store.db.runAsync { tr ->
            store.context.getMetadata(factStoreId, tr)
                .thenApply { metadata -> metadata != null }
        }.await()

        if (!exists) {
            return FactStoreNotFound
        }

        // Resolve cursor safely
        val cursorResult = with(factStoreId) {
            resolveInitialCursor(streamingOptions.startPosition)
        }

        val initialCursor = when (cursorResult) {
            is CursorResult.Found -> cursorResult.key
            is CursorResult.Invalid -> return InvalidStartPosition(cursorResult.nonExistingFact)
            CursorResult.Beginning -> null
        }

        val flow = streamFacts(initialCursor, globalRange, factStoreId)

        return Success(flow)
    }

    private fun streamFacts(
        initialCursor: ByteArray?,
        globalRange: Range,
        factStoreId: FactStoreId
    ): Flow<Fact> = flow {
        var lastSeenKey = initialCursor

        while (currentCoroutineContext().isActive) {

            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, tr)
                    .thenApply { batch ->
                        if (batch.isEmpty()) {
                            val watchFuture = tr.watch(
                                store.context.headSubspace.pack(factStoreId.uuid)
                            )
                            ReadResult.WatchResult(watchFuture)
                        } else {
                            ReadResult.BatchResult(batch)
                        }
                    }
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
        value class Invalid(val nonExistingFact: FactId) : CursorResult
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
                if (key == null) CursorResult.Invalid(startPosition.factId)
                else CursorResult.Found(key)
            }
        }

    context(factStoreId: FactStoreId)
    private suspend fun getKeyForFactOrNull(factId: FactId): ByteArray? =
        store.db.readAsync { tr ->
            with(tr) {
                factId.loadFact().thenApply { fdbFact ->
                    fdbFact?.getFactPositionKey()
                }
            }
        }.await()

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
        store.db.readAsync { tr ->
            store.getHead(factStoreId, tr)
                .thenApply { factPosition ->
                    factPosition?.getFactPositionKey()
                }
        }.await()

    context(transaction: ReadTransaction, factStoreId: FactStoreId)
    private fun FactId.loadFact(): CompletableFuture<FdbFact?> =
        with(store) {
            this@loadFact.loadFactById()
        }

    // -------------------------------------------------------------------------
    // Internal types
    // -------------------------------------------------------------------------

    private sealed interface ReadResult {
        data class BatchResult(val batch: List<FdbFact>) : ReadResult
        data class WatchResult(val watch: CompletableFuture<Void>) : ReadResult
    }
}
