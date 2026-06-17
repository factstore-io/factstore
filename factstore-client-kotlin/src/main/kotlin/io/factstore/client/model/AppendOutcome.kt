package io.factstore.client.model

import java.time.Instant

/**
 * The outcome of a successful (non-error) append.
 *
 * Error outcomes — store not found, condition violated, duplicate ids — are
 * surfaced as exceptions, not as variants of this type.
 */
sealed interface AppendOutcome {

    /**
     * The facts were appended. [factIds] are the server-assigned identifiers in
     * the same order as the submitted facts; [appendedAt] is the shared ingestion
     * timestamp.
     */
    data class Appended(
        val factIds: List<String>,
        val appendedAt: Instant,
    ) : AppendOutcome

    /**
     * An append with the same idempotency key was already processed; nothing was
     * appended again.
     */
    data object AlreadyApplied : AppendOutcome
}
