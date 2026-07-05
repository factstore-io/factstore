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
 * Facts are submitted as [FactInput]s: their [Fact.id] and [Fact.appendedAt] are
 * assigned by the store on append, not by the client.
 *
 * @property storeName the store to which to append the facts to
 * @property facts the facts to append
 * @property idempotencyKey the key used to ensure idempotent processing
 * @property condition an optional condition that must be satisfied for the
 *         append operation to be applied
 *
 * @author Domenic Cassisi
 */
data class AppendRequest(
    val storeName: StoreName,
    val facts: List<FactInput>,
    val idempotencyKey: IdempotencyKey,
    val condition: AppendCondition = AppendCondition.None
)

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
     * @property subject the subject whose last fact is checked
     * @property expectedLastFactId the expected identifier of the last fact,
     *         or `null` if no fact is expected to exist
     */
    data class ExpectedLastFact(
        val subject: Subject,
        val expectedLastFactId: FactId?
    ) : AppendCondition

    /**
     * Composite condition requiring that **all** nested conditions are
     * satisfied (logical AND).
     *
     * Conditions may be nested arbitrarily, allowing callers to compose complex
     * consistency expectations from the primitive conditions. For example, a
     * multi-subject expectation is expressed as an [All] of several
     * [ExpectedLastFact] conditions.
     *
     * @property conditions the conditions that must all be satisfied; must not
     *         be empty
     */
    data class All(
        val conditions: List<AppendCondition>
    ) : AppendCondition {
        init {
            require(conditions.isNotEmpty()) {
                "All condition must contain at least one condition"
            }
        }
    }

    /**
     * Fails the append if any fact matching [filter] exists after [after].
     *
     * If [after] is `null`, the entire store history is checked. This is the
     * preferred way to express DCB-style consistency boundaries — the full
     * expressiveness of [FactFilter.Predicate] is available as the guard.
     *
     * @property filter the predicate that must not match any existing fact
     * @property after an optional lower bound (exclusive); only facts appended
     *         after this fact are checked
     */
    data class IfNoneMatch(
        val filter: FactFilter.Predicate,
        val after: FactId? = null,
    ) : AppendCondition

    /**
     * Requires that no facts matching the given tag query exist after the
     * specified fact identifier.
     *
     * @property failIfEventsMatch the tag query that must not match any facts
     * @property after an optional fact identifier defining the lower bound
     *         for the query
     */
    @Deprecated(
        message = "Use IfNoneMatch with a FactFilter.Predicate instead",
        replaceWith = ReplaceWith(
            expression = "AppendCondition.IfNoneMatch(filter = TODO(\"convert TagQuery to FactFilter.Predicate\"), after = after)",
            imports = ["io.factstore.core.AppendCondition"],
        ),
    )
    data class TagQueryBased(
        val failIfEventsMatch: TagQuery,
        val after: FactId?
    ) : AppendCondition

}