package io.factstore.domain

import io.factstore.core.StoreId
import io.factstore.core.StoreName

sealed interface SystemFact

data class StoreCreated(
    val storeId: StoreId,
    val storeName: StoreName,
) : SystemFact

data class StoreCreationRejected(
    val storeId: StoreId,
    val storeName: StoreName,
    val reason: Reason,
) : SystemFact {

    sealed interface Reason {
        data class NameTaken(val holder: StoreId) : Reason
    }
}
