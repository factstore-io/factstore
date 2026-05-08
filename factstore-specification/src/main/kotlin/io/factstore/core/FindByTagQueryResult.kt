package io.factstore.core

sealed interface FindByTagQueryResult {
    data class Found(val facts: List<Fact>): FindByTagQueryResult
    data class StoreNotFound(val storeName: StoreName): FindByTagQueryResult
}
