package io.factstore.storage

import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.FactQuery
import io.factstore.core.FactQueryItem
import io.factstore.core.Fingerprint
import io.factstore.core.IdempotencyKey
import io.factstore.core.StoreId
import kotlinx.coroutines.flow.Flow

/**
 * The log of facts that FactStore's decisions run on, implemented by every storage engine.
 *
 * Every fact belongs to one store, and the log keeps one order over the facts of all stores.
 * [read] is cold: nothing is read until the flow is collected, and every collection reads anew.
 * [append] is atomic: it checks the idempotency key, then the condition, then writes all facts
 * and the key, or nothing. It stores the facts as given; their ids and times are assigned before,
 * and the log only gives them their order.
 * An idempotency key is used up by the facts appended under it: a later append under the same key
 * appends nothing and replays them, or is rejected if its fingerprint differs. An append that
 * writes no facts leaves its key free.
 * `FactLogContractTest` holds every engine to these guarantees.
 */
interface FactLog {

    fun read(query: LogQuery): Flow<LogFact>

    suspend fun append(facts: Flow<LogFact>, condition: LogCondition, idempotency: LogIdempotency): LogAppendResult
}

data class LogFact(val storeId: StoreId, val fact: Fact)

data class LogQuery(val items: List<LogQueryItem>) {

    constructor(vararg items: LogQueryItem) : this(items.toList())

    init {
        require(items.isNotEmpty()) { "A log query must contain at least one item." }
        require(items.size <= FactQuery.MAX_ITEMS) {
            "A log query must not contain more than ${FactQuery.MAX_ITEMS} items, but contained ${items.size}."
        }
    }

    fun matches(fact: LogFact): Boolean = items.any { it.matches(fact) }
}

data class LogQueryItem(val storeId: StoreId, val item: FactQueryItem) {

    fun matches(fact: LogFact): Boolean = storeId == fact.storeId && item.matches(fact.fact)
}

sealed interface LogCondition {

    data object None : LogCondition

    data class NoneMatch(val query: LogQuery, val after: FactId?) : LogCondition
}

/**
 * The idempotency key of an append, with the fingerprint of the request it was sent for.
 *
 * Keys are unique within a scope; today a scope is a store.
 */
data class LogIdempotency(val scope: StoreId, val key: IdempotencyKey, val fingerprint: Fingerprint)

sealed interface LogAppendResult {

    class Appended(val facts: Flow<LogFact>) : LogAppendResult

    class AlreadyAppended(val facts: Flow<LogFact>) : LogAppendResult

    data class IdempotencyKeyReused(val key: IdempotencyKey) : LogAppendResult

    data object ConditionViolated : LogAppendResult

    data class AfterFactNotFound(val factId: FactId) : LogAppendResult
}
