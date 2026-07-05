package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * A composable query that streams matching facts from a named store.
 *
 * Construct instances via the [factQuery] DSL, which validates structural
 * constraints and normalises bounded selectors before building this object.
 *
 * ### Three-stage pipeline
 *
 * The query engine processes a [FactQuery] in three distinct stages:
 *
 * 1. **Selection** — [FactFilter.Predicate.First]/[FactFilter.Predicate.Last]
 *    select facts per predicate branch before any merging occurs.
 * 2. **Merge** — results from all branches are merge-sorted by versionstamp into
 *    a single, globally ordered stream.
 * 3. **Delivery** — [direction] and [limit] are applied to the merged stream.
 *
 * These three concerns are orthogonal: [direction] and [limit] do not influence
 * the per-branch selection performed by `First`/`Last`.
 *
 * @property storeName the store to query
 * @property filter the filter describing which facts to include
 * @property limit an optional cap on the total number of facts delivered
 * @property direction the order in which the merged result is delivered
 * @property cursor an optional exclusive lower (Forward) or upper (Backward) bound,
 *   identified by [FactId]; the cursor fact itself is never included in results
 *
 * @author Domenic Cassisi
 */
data class FactQuery(
    val storeName: StoreName,
    val filter: FactFilter = FactFilter.All,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
    val cursor: FactId? = null,
)

/**
 * The result of executing a [FactQuery].
 *
 * Pre-stream errors ([StoreNotFound], [CursorNotFound]) are returned before the
 * stream starts. Errors that occur mid-stream are surfaced as terminal failures on
 * the [FactStream].
 *
 * @author Domenic Cassisi
 */
sealed interface FactQueryResult {

    /**
     * A stream of fact batches satisfying the query.
     *
     * Batches are always non-empty. The stream completing is the authoritative
     * signal that no more results are available.
     */
    @JvmInline
    value class FactStream(val stream: Flow<List<Fact>>) : FactQueryResult

    /** The requested store does not exist. */
    @JvmInline
    value class StoreNotFound(val storeName: StoreName) : FactQueryResult

    /** The cursor [FactId] does not exist in the store. */
    @JvmInline
    value class CursorNotFound(val factId: FactId) : FactQueryResult
}
