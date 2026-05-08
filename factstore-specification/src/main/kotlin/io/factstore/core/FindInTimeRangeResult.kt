package io.factstore.core

sealed interface FindInTimeRangeResult {
    data class Found(val facts: List<Fact>): FindInTimeRangeResult
    data class StoreNotFound(val storeName: StoreName): FindInTimeRangeResult
}
