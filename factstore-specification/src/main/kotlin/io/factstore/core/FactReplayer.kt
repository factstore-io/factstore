package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * Provides a bounded replay over the facts in a store.
 *
 * A replay drains the existing facts from the requested [ReplayStart] up to the
 * head pinned at the moment the replay is opened, then completes. Facts appended
 * while the replay is draining are excluded and will be observed by a subsequent
 * replay; the [Flow] completing is the signal that the consumer has caught up.
 *
 * Combined with [ReplayStart.After] and a persisted checkpoint, a replay is
 * restart-safe: a crashed run resumes from the last processed fact. This makes
 * it the natural fit for exports, projection rebuilds and incremental batch jobs.
 *
 * For an unbounded live subscription see [FactSubscriber].
 *
 * @author Domenic Cassisi
 */
fun interface FactReplayer {

    /**
     * Opens a bounded replay according to the given request.
     *
     * @param request the store and start position to replay from
     * @return a [ReplayResult.FactStream] emitting facts incrementally and
     * completing once the pinned head is reached, or an error result if the
     * store does not exist or the start cursor is invalid
     */
    suspend fun replay(request: ReplayRequest): ReplayResult

}

/**
 * Where a replay starts reading from.
 *
 * Unlike [StartPosition] there is no `End`: starting and stopping at the end
 * would always yield nothing, so that combination is deliberately not expressible.
 */
sealed interface ReplayStart {

    /** Start from the very first fact in the store. */
    data object Beginning : ReplayStart

    /** Start immediately after the given fact (the delta since a checkpoint). */
    @JvmInline
    value class After(val factId: FactId) : ReplayStart

}

sealed interface ReplayResult {
    /** A stream of fact batches that completes once the pinned head is reached. */
    @JvmInline
    value class FactStream(val stream: Flow<List<Fact>>) : ReplayResult

    @JvmInline
    value class StoreNotFound(val storeName: StoreName) : ReplayResult

    @JvmInline
    value class FactIdNotFound(val id: FactId) : ReplayResult
}
