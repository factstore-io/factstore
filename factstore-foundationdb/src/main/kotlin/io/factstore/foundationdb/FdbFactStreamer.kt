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
import java.util.concurrent.CompletableFuture

const val DEFAULT_BATCH_SIZE = 5000

class FdbFactStreamer(
    private val store: FdbFactStore
) : FactStreamer {

    override fun stream(factStoreId: FactStoreId, streamingOptions: StreamingOptions): Flow<Fact> = flow {
        val globalRange = store.context.globalFactPositionSubspace.range(Tuple.from(factStoreId.uuid))

        var lastSeenKey: ByteArray? =
            factStoreId.run { resolveInitialCursor(streamingOptions.startPosition) }

        while (currentCoroutineContext().isActive) {

            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, tr).thenApply { batch ->
                    if (batch.isEmpty()) {
                        val watchFuture = tr.watch(store.context.headSubspace.pack(factStoreId.uuid))
                        ReadResult.WatchResult(watchFuture)
                    } else {
                        ReadResult.BatchResult(batch)
                    }
                }
            }.await()

            when (readResult) {
                is ReadResult.BatchResult -> {
                    val lastFact = readResult.batch.last()
                    lastSeenKey = with(factStoreId) { lastFact.getFactPositionKey() }
                    for (fdbFact in readResult.batch) {
                        emit(fdbFact.fact)
                    }
                }
                is ReadResult.WatchResult -> readResult.watch.await()
            }
        }
    }

    context(factStoreId: FactStoreId)
    private suspend fun resolveInitialCursor(
        startPosition: StartPosition,
    ): ByteArray? =
        when (startPosition) {
            StartPosition.Beginning -> null
            StartPosition.End -> getCurrentEndKey()
            is StartPosition.After -> getKeyForFactOrThrow(startPosition.factId)
        }

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

    context(factStoreId: FactStoreId)
    private suspend fun getKeyForFactOrThrow(factId: FactId): ByteArray =
        store.db.readAsync { tr -> with(tr) {
            factId.loadFact().thenApply { fdbFact ->
                fdbFact?.getFactPositionKey() ?: throw FactIdNotFoundException(factId)
            }
        }
        }.await()

    context(factStoreId: FactStoreId)
    private fun FdbFact.getFactPositionKey(): ByteArray =
        factPosition.getFactPositionKey()

    context(factStoreId: FactStoreId)
    private fun FactPosition.getFactPositionKey(): ByteArray =
        store.context.globalFactPositionSubspace.pack(Tuple.from(factStoreId.uuid, this))

    context(factStoreId: FactStoreId)
    private suspend fun getCurrentEndKey(): ByteArray? =
        store.db.readAsync { tr ->
            store.getHead(factStoreId, tr).thenApply { factPosition ->
                factPosition?.getFactPositionKey()
            }
        }.await()

    context(transaction: ReadTransaction, factStoreId: FactStoreId)
    private fun FactId.loadFact(): CompletableFuture<FdbFact?> = with(store) {
        this@loadFact.loadFactById()
    }

    private sealed interface ReadResult {
        data class BatchResult(val batch: List<FdbFact>) : ReadResult
        data class WatchResult(val watch: CompletableFuture<Void>) : ReadResult
    }

}
