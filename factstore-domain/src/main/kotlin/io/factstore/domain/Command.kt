package io.factstore.domain

import io.factstore.core.StoreId
import io.factstore.core.StoreName

sealed interface Command

data class CreateStore(
    val storeId: StoreId,
    val storeName: StoreName,
) : Command
