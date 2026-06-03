package io.factstore.core

data class ExistsByIdRequest(
    val storeName: StoreName,
    val factId: FactId,
)
