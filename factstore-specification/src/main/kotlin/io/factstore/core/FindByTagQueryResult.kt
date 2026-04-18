package io.factstore.core

sealed interface FindByTagQueryResult {
    data class Found(val facts: List<Fact>): FindByTagQueryResult
    data object FactstoreNotFound: FindByTagQueryResult
}
