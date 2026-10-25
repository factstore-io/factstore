package io.factstore.core

/**
 * Removes the stores of a FactStore.
 */
fun interface StoreRemover {

    /**
     * Removes a store together with everything in it: its facts, their indexes and its
     * idempotency keys. Removal cannot be undone, and the name becomes free for a new store.
     *
     * @return [RemoveStoreResult.Removed], or [RemoveStoreResult.NotFound]
     */
    suspend fun removeStore(request: RemoveStoreRequest): RemoveStoreResult

}

/**
 * Requests the removal of a store.
 *
 * @property storeName the name of the store to remove
 */
data class RemoveStoreRequest(
    val storeName: StoreName
)

/**
 * The outcome of [StoreRemover.removeStore].
 */
sealed interface RemoveStoreResult {

    /** The store and everything in it were removed. */
    data class Removed(val storeName: StoreName) : RemoveStoreResult

    /** No store has that name; nothing was removed. */
    data class NotFound(val storeName: StoreName) : RemoveStoreResult

}
