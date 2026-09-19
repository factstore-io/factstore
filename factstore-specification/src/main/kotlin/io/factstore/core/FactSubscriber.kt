package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * Provides a live catch-up subscription over the facts in a store.
 *
 * A subscription drains the facts the store already holds, beginning at the requested
 * [StartPosition], and then keeps emitting facts as they are appended. It is the unbounded
 * counterpart of a [FactStreamer] stream: a stream stops at the head pinned when it was
 * opened, a subscription follows the head for as long as the consumer collects it. The
 * returned [Flow] therefore never completes on its own — the consumer cancels it when it is
 * no longer interested.
 *
 * This is the catch-up subscription used for live projections, read models and integrations.
 *
 * @author Domenic Cassisi
 */
fun interface FactSubscriber {

    /**
     * Opens a live subscription according to the given request.
     *
     * @param request the store to subscribe to and the position to start from
     * @return a [SubscribeResult.FactStream] emitting facts as they are read and appended, or
     * the outcome describing why no subscription could be opened
     */
    suspend fun subscribe(request: SubscribeRequest): SubscribeResult

}

/**
 * Where a subscription starts reading.
 */
sealed interface StartPosition {

    /** Start at the store's first fact, so the whole history is drained first. */
    data object Beginning : StartPosition

    /** Start at the store's current end: only facts appended after the subscription is opened. */
    data object End : StartPosition

    /**
     * Continue after the given fact, exclusive — how a subscription that was cut off resumes.
     */
    @JvmInline
    value class ContinueAfter(val factId: FactId) : StartPosition

}

/**
 * The outcome of [FactSubscriber.subscribe].
 */
sealed interface SubscribeResult {

    /**
     * The subscribed facts, read when collected and emitted in the order the store holds them.
     *
     * The flow never completes on its own: once the store's facts are drained it stays open and
     * emits what is appended next.
     */
    class FactStream(val facts: Flow<Fact>) : SubscribeResult

    /** The requested store does not exist. */
    data class StoreNotFound(val storeName: StoreName) : SubscribeResult

    /** The store holds no fact with the id the subscription was to continue after. */
    data class ContinuationNotFound(val factId: FactId) : SubscribeResult
}
