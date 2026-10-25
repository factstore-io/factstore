package io.factstore.core

/**
 * Creates the stores of a FactStore.
 *
 * A store is an isolated log of facts: facts, their indexes and their idempotency keys all belong
 * to exactly one store, and nothing is shared between them.
 */
fun interface StoreFactory {

    /**
     * Creates a store under the requested name.
     *
     * @return [CreateStoreResult.Created], or [CreateStoreResult.NameAlreadyExists]
     */
    suspend fun createStore(request: CreateStoreRequest): CreateStoreResult

}

/**
 * Requests a new store.
 *
 * @property storeName the name of the store; must not be taken by another store
 */
data class CreateStoreRequest(
    val storeName: StoreName,
)

/**
 * The outcome of [StoreFactory.createStore].
 */
sealed interface CreateStoreResult {

    /** The store was created, with the id it is known by from now on. */
    data class Created(val id: StoreId) : CreateStoreResult

    /** Another store already has the requested name; nothing was created. */
    data class NameAlreadyExists(val storeName: StoreName) : CreateStoreResult

}
