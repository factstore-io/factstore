package io.factstore.client.model

/**
 * Where a bounded replay starts reading from.
 *
 * Unlike [SubscribeStartPosition] there is no `End`: replaying from the end would
 * always yield nothing.
 */
sealed class ReplayStartPosition {
    data object Beginning : ReplayStartPosition()
    data class AfterFact(val factId: String) : ReplayStartPosition()
}
