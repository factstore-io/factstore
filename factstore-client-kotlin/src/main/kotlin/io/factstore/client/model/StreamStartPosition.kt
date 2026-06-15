package io.factstore.client.model

sealed class StreamStartPosition {
    data object Beginning : StreamStartPosition()
    data object End : StreamStartPosition()
    data class AfterFact(val factId: String) : StreamStartPosition()
}
