package io.factstore.core

sealed interface FindBySubjectResult {
    data class Found(val facts: List<Fact>): FindBySubjectResult
    data object StoreNotFound: FindBySubjectResult
}
