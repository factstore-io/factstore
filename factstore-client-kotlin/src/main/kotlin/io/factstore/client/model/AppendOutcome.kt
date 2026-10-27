package io.factstore.client.model

/**
 * The outcome of a successful (non-error) append.
 *
 * Error outcomes — store not found, condition violated, idempotency key reused — are
 * surfaced as exceptions, not as variants of this type.
 */
sealed interface AppendOutcome {

    /** The stored facts, in the same order as the submitted facts; they share one appendedAt. */
    val facts: List<Fact>

    /** The facts were appended. */
    data class Appended(override val facts: List<Fact>) : AppendOutcome

    /**
     * The same append was already applied under its idempotency key; nothing was appended
     * again. [facts] are the facts the first attempt stored.
     */
    data class AlreadyApplied(override val facts: List<Fact>) : AppendOutcome
}
