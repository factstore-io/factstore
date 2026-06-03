package io.factstore.core

data class FindByIdRequest(
    val storeName: StoreName,
    val factId: FactId,
)
