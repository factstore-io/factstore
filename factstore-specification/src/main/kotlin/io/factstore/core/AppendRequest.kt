package io.factstore.core

import java.util.UUID

/**
 * Identifies an append request for idempotent processing.
 *
 * Idempotency keys allow append operations to be safely retried. If an append
 * request with the same idempotency key is processed more than once, the
 * FactStore guarantees that the facts are appended at most once.
 *
 * @property value the underlying UUID value
 *
 * @author Domenic Cassisi
 */
@JvmInline
value class IdempotencyKey(val value: UUID = UUID.randomUUID())

/**
 * Represents an append operation to the FactStore.
 *
 * An append request groups one or more facts together with optional idempotency
 * and conditional append semantics. All facts contained in the request are
 * processed atomically.
 *
 * @property facts the facts to append
 * @property idempotencyKey the key used to ensure idempotent processing
 * @property condition an optional condition that must be satisfied for the
 *         append operation to be applied
 *
 * @throws IllegalArgumentException if the request contains duplicate [FactId]s
 *
 * @author Domenic Cassisi
 */
data class AppendRequest(
    val facts: List<Fact>,
    val idempotencyKey: IdempotencyKey,
    val condition: AppendCondition = AppendCondition.None
) {

    init {
        require(facts.map { it.id }.distinct().size == facts.size) {
            "Duplicated FactId detected!"
        }
    }

}

/**
 * Defines conditions that must be satisfied for an append request to be applied.
 *
 * Append conditions allow callers to express consistency expectations explicitly
 * and enable different event-sourcing strategies within the same store.
 *
 * @author Domenic Cassisi
 */
sealed interface AppendCondition {

    /**
     * No condition is applied.
     */
    data object None : AppendCondition

    /**
     * Requires that the last fact associated with the given subject matches
     * the expected fact identifier.
     *
     * @property subjectRef the subject whose last fact is checked
     * @property expectedLastFactId the expected identifier of the last fact,
     *         or `null` if no fact is expected to exist
     */
    data class ExpectedLastFact(
        val subjectRef: SubjectRef,
        val expectedLastFactId: FactId?
    ) : AppendCondition

    /**
     * Requires that the last facts associated with multiple subjects match
     * the provided expectations.
     *
     * @property expectations a mapping from subject references to their expected
     *         last fact identifiers
     */
    data class ExpectedMultiSubjectLastFact(
        val expectations: Map<SubjectRef, FactId?>
    ) : AppendCondition

    /**
     * Requires that no facts matching the given tag query exist after the
     * specified fact identifier.
     *
     * @property failIfEventsMatch the tag query that must not match any facts
     * @property after an optional fact identifier defining the lower bound
     *         for the query
     */
    data class TagQueryBased(
        val failIfEventsMatch: TagQuery,
        val after: FactId?
    ) : AppendCondition

}