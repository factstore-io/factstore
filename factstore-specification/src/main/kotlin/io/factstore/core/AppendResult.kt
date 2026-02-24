package io.factstore.core

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
     */
    data object Appended : AppendResult

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

}
