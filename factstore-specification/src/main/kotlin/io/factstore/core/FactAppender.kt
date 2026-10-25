package io.factstore.core

import java.time.Instant
import java.util.UUID

/**
 * Appends facts to the FactStore.
 *
 * Implementations are responsible for persisting facts atomically and
 * enforcing all store invariants such as uniqueness, idempotency, and
 * conditional append semantics.
 *
 * An append completes with an [AppendResult] describing its outcome.
 *
 * @author Domenic Cassisi
 */
interface FactAppender {

    /**
     * Appends facts using an explicit append request.
     *
     * This variant allows callers to specify idempotency keys and conditional
     * append rules and provides a structured [AppendResult] describing the
     * outcome of the operation.
     *
     * @param request the append request
     * @return the outcome of the append operation
     */
    suspend fun append(request: AppendRequest): AppendResult

}

/**
 * Appends a single fact to the store.
 *
 * This convenience generates a fresh idempotency key on every call and is therefore **not**
 * idempotent across retries. Use [FactAppender.append] with an explicit [AppendRequest] when
 * idempotency or conditional appends are needed.
 *
 * @param fact the fact to append
 * @throws IllegalArgumentException if the fact does not form a valid [AppendRequest]
 */
suspend fun FactAppender.append(storeName: StoreName, fact: FactInput): AppendResult =
    append(storeName, listOf(fact))

/**
 * Appends multiple facts to the store atomically.
 *
 * Either all facts are appended successfully, or none are. Like the single-fact convenience, this
 * generates a fresh idempotency key per call and is not idempotent across retries.
 *
 * @param facts the facts to append
 * @throws IllegalArgumentException if the facts do not form a valid [AppendRequest]
 */
suspend fun FactAppender.append(storeName: StoreName, facts: List<FactInput>): AppendResult =
    append(AppendRequest(storeName = storeName, facts = facts, idempotencyKey = IdempotencyKey()))

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
 * @property facts the facts to append; at least one and at most [MAX_FACTS],
 *         together no larger than [MAX_SIZE] bytes
 * @property idempotencyKey the key used to ensure idempotent processing
 * @property condition an optional condition that must be satisfied for the
 *         append operation to be applied
 *
 * @throws IllegalArgumentException if [facts] is empty, contains more than
 *         [MAX_FACTS] facts, or exceeds [MAX_SIZE] bytes in total
 *
 * @author Domenic Cassisi
 */
data class AppendRequest(
    val storeName: StoreName,
    val facts: List<FactInput>,
    val idempotencyKey: IdempotencyKey,
    val condition: AppendCondition = AppendCondition.None
) {

    companion object {

        /**
         * The maximum number of facts in a single append.
         */
        const val MAX_FACTS = 512

        /**
         * The maximum total size of the facts in a single append, in bytes: 1 MiB.
         */
        const val MAX_SIZE = 1_048_576
    }

    init {
        require(facts.isNotEmpty()) { "An append must contain at least one fact." }
        require(facts.size <= MAX_FACTS) {
            "An append must not contain more than $MAX_FACTS facts, but contained ${facts.size}."
        }
        val size = facts.sumOf { it.byteSize.toLong() }
        require(size <= MAX_SIZE) {
            "An append must not exceed $MAX_SIZE bytes, but its facts total $size bytes."
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
     * Requires that no fact matching [query] was appended after [after].
     *
     * This is the condition of a decision that read the facts matching [query]: [after] is the
     * last fact it read, and the append is applied only if nothing it would have read has been
     * appended since. Without [after], no matching fact may exist at all.
     *
     * An [after] that is not a fact of the store fails the append with
     * [AppendResult.AfterFactNotFound]: a decision that names a fact it cannot have read knows
     * nothing about the store's state.
     *
     * @property query the facts that must not have been appended
     * @property after the last fact the decision read, exclusive; `null` to check the whole store
     */
    data class NoneMatch(
        val query: FactQuery,
        val after: FactId?
    ) : AppendCondition

}

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
     * Indicates that the append operation was not applied because a
     * [AppendCondition.NoneMatch] condition names a fact to check after that is not a
     * fact of the store.
     *
     * It takes precedence over [AppendConditionViolated]: whether the other conditions
     * hold says nothing about a request that refers to a fact the store does not have.
     *
     * @property factId the fact that was not found
     */
    data class AfterFactNotFound(val factId: FactId) : AppendResult

    /**
     * Indicates that the append operation failed because the specified fact store does not
     * exist. This result is returned when the fact store identified in the append request cannot be found.
     */
    data class StoreNotFound(val storeName: StoreName) : AppendResult

}
