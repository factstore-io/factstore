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

    override fun stream(streamingOptions: StreamingOptions): Flow<Fact> = flow {
        val globalRange = store.context.globalFactPositionSubspace.range()

        var lastSeenKey: ByteArray? =
            resolveInitialCursor(streamingOptions.startPosition)

        while (currentCoroutineContext().isActive) {

            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, tr).thenApply { batch ->
                    if (batch.isEmpty()) {
                        val watchFuture = tr.watch(store.context.headSubspace.pack())
                        ReadResult.WatchResult(watchFuture)
                    } else {
                        ReadResult.BatchResult(batch)
                    }
                }
            }.await()

            when (readResult) {
                is ReadResult.BatchResult -> {
                    lastSeenKey = readResult.batch.last().getFactPositionKey()
                    for (fdbFact in readResult.batch) {
                        emit(fdbFact.fact)
                    }
                }
                is ReadResult.WatchResult -> readResult.watch.await()
            }
        }
    }

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

    private suspend fun getKeyForFactOrThrow(factId: FactId): ByteArray =
        store.db.readAsync { tr ->
            tr.loadFact(factId).thenApply { fdbFact ->
                fdbFact?.getFactPositionKey() ?: throw FactIdNotFoundException(factId)
            }
        }.await()

    private fun FdbFact.getFactPositionKey(): ByteArray =
        factPosition.getFactPositionKey()

    private fun FactPosition.getFactPositionKey(): ByteArray =
        store.context.globalFactPositionSubspace.pack(this)

    private suspend fun getCurrentEndKey(): ByteArray? =
        store.db.readAsync { tr ->
            store.getHead(tr).thenApply { factPosition ->
                factPosition?.getFactPositionKey()
            }
        }.await()

    private fun ReadTransaction.loadFact(factId: FactId): CompletableFuture<FdbFact?> = with(store) {
        this@loadFact.loadFactById(factId)
    }

    private sealed interface ReadResult {
        data class BatchResult(val batch: List<FdbFact>) : ReadResult
        data class WatchResult(val watch: CompletableFuture<Void>) : ReadResult
    }

}
