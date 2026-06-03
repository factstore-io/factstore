package io.factstore.core

sealed interface ExistsStoreByNameResult {

    data object StoreExists : ExistsStoreByNameResult
    data object StoreAbsent : ExistsStoreByNameResult

}
