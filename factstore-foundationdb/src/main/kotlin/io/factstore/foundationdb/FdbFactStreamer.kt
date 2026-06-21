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
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.channels.BufferOverflow.SUSPEND
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import java.util.concurrent.CompletableFuture

const val DEFAULT_BATCH_SIZE = 10_000
const val RAW_CHANNEL_CAPACITY = 4

/**
 * FoundationDB streaming engine. Backs both the live [FactSubscriber] and the
 * bounded [FactReplayer]: each entry point resolves a start cursor and an
 * [EndBoundary], then delegates to a single shared [scan] pipeline.
 */
class FdbFactStreamer(
    private val store: FdbFactStore,
    private val deserializationDispatcher: CoroutineDispatcher = Dispatchers.Default,
) : FactSubscriber, FactReplayer {

    // -------------------------------------------------------------------------
    // Subscribe (live tail)
    // -------------------------------------------------------------------------

    override suspend fun subscribe(request: SubscribeRequest): SubscribeResult {
        val storeName = request.storeName

        val storeId = read { tr ->
            with(tr) { store.context.lookUpStoreIdByName(storeName) }
        } ?: return SubscribeResult.StoreNotFound(storeName)

        val cursorResult = with(storeId) { resolveInitialCursor(request.startPosition) }
        val initialCursor = when (cursorResult) {
            is CursorResult.Found -> cursorResult.key
            is CursorResult.FactNotFound -> return SubscribeResult.FactIdNotFound(cursorResult.factId)
            CursorResult.Beginning -> null
        }

        return SubscribeResult.FactStream(scan(storeId, initialCursor, EndBoundary.Follow))
    }

    // -------------------------------------------------------------------------
    // Replay (bounded)
    // -------------------------------------------------------------------------

    override suspend fun replay(request: ReplayRequest): ReplayResult {
        val storeName = request.storeName

        // Resolve the store, the begin cursor and the pinned head in a single
        // consistent read, so the replay window cannot be skewed by concurrent
        // writes or deletes between separate transactions.
        val resolution = read { tr ->
            with(tr) {
                store.context.lookUpStoreIdByName(storeName).thenCompose { storeId ->
                    if (storeId == null) CompletableFuture.completedFuture(ReplayResolution.StoreMissing)
                    else resolveReplayBounds(tr, storeId, request.start)
                }
            }
        }

        return when (resolution) {
            ReplayResolution.StoreMissing -> ReplayResult.StoreNotFound(storeName)
            is ReplayResolution.CursorMissing -> ReplayResult.FactIdNotFound(resolution.factId)
            is ReplayResolution.Resolved -> {
                // No head => empty store => nothing to replay.
                val pinnedEndKey = resolution.pinnedEndKey ?: return ReplayResult.FactStream(emptyFlow())
                ReplayResult.FactStream(scan(resolution.storeId, resolution.beginCursor, EndBoundary.StopAt(pinnedEndKey)))
            }
        }
    }

    private fun resolveReplayBounds(
        tr: ReadTransaction,
        storeId: StoreId,
        start: ReplayStart,
    ): CompletableFuture<ReplayResolution> {
        val beginFuture: CompletableFuture<BeginResolution> = when (start) {
            ReplayStart.Beginning -> CompletableFuture.completedFuture(BeginResolution.From(null))
            is ReplayStart.After -> with(tr) {
                store.context.factPositionIndexSubspace.getPosition(storeId, start.factId).thenApply { position ->
                    if (position == null) BeginResolution.Missing(start.factId)
                    else BeginResolution.From(store.context.factSubspace.getFactKey(storeId, position))
                }
            }
        }

        return beginFuture.thenCompose { begin ->
            when (begin) {
                is BeginResolution.Missing -> CompletableFuture.completedFuture(ReplayResolution.CursorMissing(begin.factId))
                is BeginResolution.From -> store.getHead(storeId, tr).thenApply { headPosition ->
                    val pinnedEndKey = headPosition?.let { store.context.factSubspace.getFactKey(storeId, it) }
                    ReplayResolution.Resolved(storeId, begin.cursor, pinnedEndKey)
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // Shared scan pipeline
    // -------------------------------------------------------------------------

    private fun scan(
        storeId: StoreId,
        beginCursor: ByteArray?,
        endBoundary: EndBoundary,
    ): Flow<List<Fact>> =
        readBatches(storeId, beginCursor, endBoundary)
            .buffer(
                capacity = RAW_CHANNEL_CAPACITY,
                onBufferOverflow = SUSPEND
            )
            .map { keyValues ->
                withContext(deserializationDispatcher) {
                    keyValues.map { it.value.toSerializableFdbFact().toFact() }
                }
            }

    private fun readBatches(
        storeId: StoreId,
        beginCursor: ByteArray?,
        endBoundary: EndBoundary,
    ): Flow<List<KeyValue>> = flow {
        val globalRange = store.context.factSubspace.getRange(storeId)
        var lastSeenKey = beginCursor
        while (true) {
            val readResult = store.db.runAsync { tr ->
                readNextBatch(lastSeenKey, globalRange, endBoundary, tr)
                    .thenApply { batch -> batch.toReadResult(tr, storeId, endBoundary) }
            }.await()
            when (readResult) {
                is ReadResult.BatchResult -> {
                    lastSeenKey = readResult.batch.last().key
                    emit(readResult.batch)
                }

                is ReadResult.WatchResult -> readResult.waitForFacts()

                ReadResult.Complete -> return@flow
            }
        }
    }

    private fun readNextBatch(
        lastSeenKey: ByteArray?,
        globalRange: Range,
        endBoundary: EndBoundary,
        tr: ReadTransaction
    ): CompletableFuture<List<KeyValue>> {
        val beginSelector =
            if (lastSeenKey == null)
                KeySelector.firstGreaterOrEqual(globalRange.begin)
            else
                KeySelector.firstGreaterThan(lastSeenKey)

        return tr.snapshot().getRange(
            beginSelector,
            endBoundary.endSelector(globalRange),
            DEFAULT_BATCH_SIZE,
            false,
            StreamingMode.WANT_ALL
        ).asList()
    }

    private fun List<KeyValue>.toReadResult(
        tr: Transaction,
        storeId: StoreId,
        endBoundary: EndBoundary
    ): ReadResult =
        if (isNotEmpty()) {
            ReadResult.BatchResult(this)
        } else when (endBoundary) {
            // Live tail: wait for the next append, then resume.
            EndBoundary.Follow -> {
                val watchFuture = tr.watch(store.context.headSubspace.headKey(storeId))
                ReadResult.WatchResult { watchFuture.await() }
            }
            // Bounded: pinned head reached, the replay is done.
            is EndBoundary.StopAt -> ReadResult.Complete
        }

    // -------------------------------------------------------------------------
    // Cursor handling (subscribe)
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
    // Internal types
    // -------------------------------------------------------------------------

    private sealed interface ReadResult {
        data class BatchResult(val batch: List<KeyValue>) : ReadResult
        data class WatchResult(val waitForFacts: suspend () -> Unit) : ReadResult
        data object Complete : ReadResult
    }

    /**
     * Where the scan ends. [Follow] tails the live head indefinitely; [StopAt]
     * reads up to and including the head pinned at replay start, then completes.
     */
    private sealed interface EndBoundary {
        fun endSelector(globalRange: Range): KeySelector

        data object Follow : EndBoundary {
            override fun endSelector(globalRange: Range): KeySelector =
                KeySelector.firstGreaterOrEqual(globalRange.end)
        }

        // Plain class (not data class): equals/hashCode over a ByteArray would be
        // identity-based and misleading.
        class StopAt(val pinnedEndKey: ByteArray) : EndBoundary {
            // The pinned end is the head fact's key; include it with firstGreaterThan.
            override fun endSelector(globalRange: Range): KeySelector =
                KeySelector.firstGreaterThan(pinnedEndKey)
        }
    }

    private sealed interface ReplayResolution {
        data object StoreMissing : ReplayResolution
        data class CursorMissing(val factId: FactId) : ReplayResolution
        class Resolved(
            val storeId: StoreId,
            val beginCursor: ByteArray?,
            val pinnedEndKey: ByteArray?,
        ) : ReplayResolution
    }

    private sealed interface BeginResolution {
        class From(val cursor: ByteArray?) : BeginResolution
        data class Missing(val factId: FactId) : BeginResolution
    }

    private suspend fun <T> read(trBlock: (ReadTransaction) -> CompletableFuture<T>): T =
        store.db.readAsync(trBlock).await()
}
