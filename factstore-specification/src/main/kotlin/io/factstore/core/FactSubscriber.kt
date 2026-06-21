package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * Provides a live catch-up subscription over the facts in a store.
 *
 * A subscription drains the existing facts from the requested [StartPosition]
 * and then continues to emit new facts as they are appended. The returned
 * [Flow] never completes on its own; the consumer cancels it when no longer
 * interested. This is the classic catch-up subscription used for live
 * projections, read models and integrations.
 *
 * For a bounded, terminating read see [FactReplayer].
 *
 * @author Domenic Cassisi
 */
fun interface FactSubscriber {

    /**
     * Opens a live subscription according to the given request.
     *
     * @param request the store and start position to subscribe from
     * @return a [SubscribeResult.FactStream] emitting facts incrementally (never
     * completing on its own), or an error result if the store does not exist or
     * the start cursor is invalid
     */
    suspend fun subscribe(request: SubscribeRequest): SubscribeResult

}

/**
 * Where a subscription starts reading from.
 */
sealed interface StartPosition {

    /** Start from the very first fact in the store. */
    data object Beginning : StartPosition

    /** Start from the current end: only facts appended after the subscription is opened. */
    data object End : StartPosition

    /** Start immediately after the given fact. */
    @JvmInline
    value class After(val factId: FactId) : StartPosition

}

sealed interface SubscribeResult {
    /** A live stream of fact batches; never completes on its own. */
    @JvmInline
    value class FactStream(val stream: Flow<List<Fact>>) : SubscribeResult

    @JvmInline
    value class StoreNotFound(val storeName: StoreName) : SubscribeResult

    @JvmInline
    value class FactIdNotFound(val id: FactId) : SubscribeResult
}
