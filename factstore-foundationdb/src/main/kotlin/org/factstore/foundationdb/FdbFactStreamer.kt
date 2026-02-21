package org.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import org.factstore.core.*
import java.util.concurrent.CompletableFuture
import kotlin.time.Duration.Companion.milliseconds

val DEFAULT_POLL_DELAY = 250.milliseconds
const val DEFAULT_BATCH_SIZE = 1024
const val REVERSE_TRUE = true

class FdbFactStreamer(
    private val store: FdbFactStore
) : FactStreamer {

    override fun stream(streamingOptions: StreamingOptions): Flow<Fact> = flow {
        val globalRange = store.globalFactPositionSubspace.range()

        var lastSeenKey: ByteArray? =
            resolveInitialCursor(streamingOptions.startPosition, globalRange)

        while (currentCoroutineContext().isActive) {

            val batch = store.db.readAsync { tr ->
                readNextBatch(
                    lastSeenKey = lastSeenKey,
                    globalRange = globalRange,
                    tr = tr
                )
            }.await()

            if (batch.isEmpty()) {
                delay(DEFAULT_POLL_DELAY)
                continue
            }

            // we move the cursor
            lastSeenKey = keyOf(batch.last())

            for (fdbFact in batch) {
                emit(fdbFact.fact)
            }
        }
    }

    private suspend fun resolveInitialCursor(
        startPosition: StartPosition,
        globalRange: Range
    ): ByteArray? =
        when (startPosition) {
            StartPosition.Beginning -> null
            StartPosition.End -> getCurrentEndKey(globalRange)
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
            .thenCompose { keyValues ->

                val futures = keyValues.mapNotNull { kv ->
                    val factId = store.globalFactPositionSubspace
                        .unpack(kv.key)
                        .getLastAsFactId()

                    tr.loadFact(factId)
                }

                CompletableFuture
                    .allOf(*futures.toTypedArray())
                    .thenApply {
                        futures.mapNotNull { it.resultNow() }
                    }
            }
    }

    private suspend fun getKeyForFactOrThrow(factId: FactId): ByteArray =
        store.db.readAsync { tr ->
            tr.loadFact(factId).thenApply { internal ->
                internal ?: throw FactIdNotFoundException(factId)
                keyOf(internal)
            }
        }.await()

    private fun keyOf(fdbFact: FdbFact): ByteArray =
        store.globalFactPositionSubspace.pack(
            fdbFact.positionTuple.add(fdbFact.fact.id.uuid)
        )

    private suspend fun getCurrentEndKey(globalRange: Range): ByteArray? =
        store.db.readAsync { tr ->
            tr.getRange(
                KeySelector.lastLessThan(globalRange.end),
                KeySelector.firstGreaterOrEqual(globalRange.end),
                LIMIT_ONE,
                REVERSE_TRUE
            ).asList().thenApply { it.firstOrNull()?.key }
        }.await()

    private fun ReadTransaction.loadFact(factId: FactId): CompletableFuture<FdbFact?> = with(store) {
        this@loadFact.loadFactById(factId)
    }
}
