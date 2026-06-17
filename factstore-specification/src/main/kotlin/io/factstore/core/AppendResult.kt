package io.factstore.core

import java.time.Instant

/**
 * Represents the outcome of an append operation.
 *
 * Append results describe the observable result of a valid append request.
 * They do not represent invalid requests or invariant violations, which are
 * reported via exceptions.
 *
 * @author Domenic Cassisi
 */
sealed interface AppendResult {

    /**
     * Indicates that the append operation was successfully applied and the
     * provided facts were persisted.
     *
     * @property factIds the identifiers assigned by the store to the appended
     *         facts, in the same order as the [AppendRequest.facts] they were
     *         materialized from (`factIds[i]` corresponds to the i-th input fact)
     * @property appendedAt the ingestion timestamp assigned to the appended
     *         facts; all facts in a single append share this instant
     */
    data class Appended(
        val factIds: List<FactId>,
        val appendedAt: Instant,
    ) : AppendResult

    /**
     * Indicates that an append request with the same idempotency key was already
     * processed and the facts were not appended again.
     */
    data object AlreadyApplied : AppendResult

    /**
     * Indicates that the append operation was not applied because the specified
     * append condition was not satisfied.
     */
    data object AppendConditionViolated : AppendResult

    /**
     * Indicates that the append operation failed because the specified fact store does not
     * exist. This result is returned when the fact store identified in the append request cannot be found.
     */
    data class StoreNotFound(val storeName: StoreName) : AppendResult

    /**
     * Indicates that the append operation failed because at least one fact ID in the request
     * is already used by another fact.
     */
    data class DuplicateFactIds(val factIds: List<FactId>) : AppendResult

}
