package io.factstore.core

import kotlinx.coroutines.flow.Flow

/**
 * The outcome of a [FactQueryRequest].
 *
 * A successful query yields a [FactStream] that emits matching facts incrementally
 * and completes once all matches have been drained, keeping memory usage bounded
 * regardless of store size. Error outcomes are modelled as distinct subtypes.
 *
 * @author Domenic Cassisi
 */
sealed interface FactQueryResult {

    /**
     * A stream of matching facts, emitted in the requested read direction and
     * completing once the result set is exhausted.
     *
     * Facts are emitted one at a time; any batching is an internal I/O and
     * transport concern, not part of this contract.
     *
     * @property stream the incremental stream of matching facts
     */
    data class FactStream(val stream: Flow<Fact>) : FactQueryResult

    /**
     * The requested store does not exist.
     *
     * @property storeName the name of the store that could not be found
     */
    data class StoreNotFound(val storeName: StoreName) : FactQueryResult

    /**
     * The [FactQueryRequest.after] cursor does not resolve to a known fact.
     *
     * @property id the fact id that could not be resolved
     */
    data class FactIdNotFound(val id: FactId) : FactQueryResult
}
