package io.factstore.storage.memory

import io.factstore.storage.FactLog
import io.factstore.storage.LogAppendResult
import io.factstore.storage.LogCondition
import io.factstore.storage.LogFact
import io.factstore.storage.LogQuery
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class InMemoryFactLog : FactLog {

    private val mutex = Mutex()
    private val stored = mutableListOf<LogFact>()

    override fun read(query: LogQuery): Flow<LogFact> = flow {
        val matching = mutex.withLock { stored.filter(query::matches) }
        emitAll(matching.asFlow())
    }

    override suspend fun append(facts: Flow<LogFact>, condition: LogCondition): LogAppendResult = mutex.withLock {
        violation(condition)?.let { return it }

        val appended = facts.toList()
        stored += appended
        LogAppendResult.Appended(appended.asFlow())
    }

    private fun violation(condition: LogCondition): LogAppendResult? = when (condition) {
        LogCondition.None -> null
        is LogCondition.NoneMatch -> {
            val after = condition.after
            val start = if (after == null) 0 else stored.indexOfFirst { it.fact.id == after } + 1
            when {
                after != null && start == 0 -> LogAppendResult.AfterFactNotFound(after)
                stored.drop(start).any(condition.query::matches) -> LogAppendResult.ConditionViolated
                else -> null
            }
        }
    }
}
