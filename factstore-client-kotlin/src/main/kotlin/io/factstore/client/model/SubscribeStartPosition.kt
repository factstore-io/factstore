package io.factstore.client.model

/**
 * Where a live subscription starts reading from.
 */
sealed class SubscribeStartPosition {

    /** Start at the store's first fact, so the whole history is drained first. */
    data object Beginning : SubscribeStartPosition()

    /** Start at the store's current end: only facts appended after the subscription is opened. */
    data object End : SubscribeStartPosition()

    /** Continue after the given fact, exclusive — how a subscription that was cut off resumes. */
    data class ContinueAfter(val factId: String) : SubscribeStartPosition()
}
