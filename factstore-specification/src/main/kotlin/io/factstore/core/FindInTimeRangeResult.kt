package io.factstore.core

sealed interface FindInTimeRangeResult {
    data class Found(val facts: List<Fact>): FindInTimeRangeResult
    data object StoreNotFound: FindInTimeRangeResult
}
