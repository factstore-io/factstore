package io.factstore.core

sealed interface CreateFactStoreResult {

    data class Created(val id: FactStoreId) : CreateFactStoreResult
    data class NameAlreadyExists(val factStoreName: FactStoreName) : CreateFactStoreResult

}
