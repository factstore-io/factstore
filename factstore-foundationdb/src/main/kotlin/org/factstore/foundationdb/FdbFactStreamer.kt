package org.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import org.factstore.core.*
import java.util.concurrent.CompletableFuture

class FdbFactStreamer(private val store: FdbFactStore) : FactStreamer {

    override fun streamAll(streamingOptionSet: StreamingOptionSet): Flow<Fact> = flow {
        // stream over /fact-store/global/{versionstamp}/{index}/{factId}

        // find position of last seen fact if supplied
        var lastSeenFactKey: ByteArray? =
            streamingOptionSet.lastSeenId?.let { getLastSeenKeyForFact(it) }

        val globalRange = store.globalFactPositionSubspace.range()

        while (currentCoroutineContext().isActive) {
            // 1) Read the next batch of global positions after the cursor
            val factBatch: List<Fact> = store.db.readAsync { tr ->
                streamNextBatch(lastSeenFactKey, globalRange, streamingOptionSet, tr)
            }.await()

            if (factBatch.isEmpty()) {
                delay(streamingOptionSet.pollDelayMs)
                continue
            }

            // we update the lastSeenFactKey for the next read operation
            lastSeenFactKey = getLastSeenKeyForFact(factBatch.last().id)

            emitAll(factBatch.asFlow())
        }
    }

    private fun streamNextBatch(
        lastSeenKey: ByteArray?,
        globalRange: Range,
        streamingOptionSet: StreamingOptionSet,
        tr: ReadTransaction
    ): CompletableFuture<List<Fact>> {
        val beginSel =
            if (lastSeenKey == null)
                KeySelector.firstGreaterOrEqual(globalRange.begin)
            else
                KeySelector.firstGreaterThan(lastSeenKey)

        val endSel = KeySelector.firstGreaterOrEqual(globalRange.end)
        val batchSize = streamingOptionSet.batchSize
        return tr.getRange(beginSel, endSel, batchSize).asList().thenCompose { keyValues ->
            val factFutures: List<CompletableFuture<FdbFact?>> = keyValues.mapNotNull { keyValue ->
                val k = store.globalFactPositionSubspace.unpack(keyValue.key)
                val factId = k.getUUID(k.size() - 1).toFactId()
                tr.loadFact(factId)
            }

            CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                factFutures.mapNotNull { it.getNow(null)?.fact }
            }
        }
    }

    private suspend fun getLastSeenKeyForFact(factId: FactId): ByteArray {
        return store.db.readAsync { tr ->
            tr.loadFact(factId).thenApply { internalFact ->
                if (internalFact == null) {
                    error("Fact with ID $factId not found!")
                }
                val positionTuple = internalFact.positionTuple
                val byteArray = store.globalFactPositionSubspace.pack(positionTuple.add(factId.uuid))
                byteArray
            }
        }.await()
    }


    private fun ReadTransaction.loadFact(factId: FactId): CompletableFuture<FdbFact?> =
        with(store) {
            this@loadFact.loadFactById(factId)
        }

}