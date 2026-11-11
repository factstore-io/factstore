package io.factstore.application

import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.toFact
import io.factstore.domain.SystemFact
import io.factstore.storage.FactLog
import io.factstore.storage.LogAppendResult
import io.factstore.storage.LogCondition
import io.factstore.storage.LogFact
import io.factstore.storage.LogQuery
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import java.time.Clock

class SystemFactRepository(
    private val log: FactLog,
    private val clock: Clock = Clock.systemUTC(),
    private val factIdGenerator: FactIdGenerator = FactIdGenerator(FactId::generate),
) : FactRepository<SystemFact, LogQuery, LogCondition, FactId> {

    override fun fetch(query: LogQuery): Flow<Pair<SystemFact, FactId>> = log.read(query).map { it.fact.decoded() }

    override suspend fun save(facts: Flow<SystemFact>, condition: LogCondition): SaveResult<SystemFact, FactId> {
        val appendedAt = clock.instant()
        val stored = facts.map { LogFact(SystemStore.ID, SystemFactCodec.encode(it).toFact(factIdGenerator.generate(), appendedAt)) }
        return when (val result = log.append(stored, condition)) {
            is LogAppendResult.Appended -> SaveResult.Saved(result.facts.map { it.fact.decoded() })
            LogAppendResult.ConditionViolated -> SaveResult.ConditionViolated
            is LogAppendResult.AfterFactNotFound -> error("The fact ${result.factId} named by $condition is gone from the log.")
        }
    }

    private fun Fact.decoded(): Pair<SystemFact, FactId> = SystemFactCodec.decode(this) to id
}
