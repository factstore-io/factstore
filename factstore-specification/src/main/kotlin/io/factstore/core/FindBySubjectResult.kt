package io.factstore.core

sealed interface FindBySubjectResult {
    data class Found(val facts: List<Fact>): FindBySubjectResult
    data class StoreNotFound(val storeName: StoreName): FindBySubjectResult
}
