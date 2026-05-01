package io.factstore.core

sealed interface FindByTagsResult {
    data class Found(val facts: List<Fact>): FindByTagsResult
    data object StoreNotFound: FindByTagsResult
}
