package io.factstore.client.model

/**
 * Where a live subscription starts reading from.
 */
sealed class SubscribeStartPosition {
    data object Beginning : SubscribeStartPosition()
    data object End : SubscribeStartPosition()
    data class AfterFact(val factId: String) : SubscribeStartPosition()
}
