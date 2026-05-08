package io.factstore.core

sealed interface FindByIdResult {
    data class Found(val fact: Fact): FindByIdResult
    data class NotFound(val id: FactId): FindByIdResult
    data class StoreNotFound(val storeName: StoreName): FindByIdResult
}
