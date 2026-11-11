package io.factstore.application

import io.factstore.storage.FactLog
import io.factstore.storage.LogFact
import io.factstore.storage.LogQuery
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import java.util.concurrent.atomic.AtomicInteger

/** Holds the first two reads back until both have happened, so both decisions see the name as free. */
internal class BothReadFirst(private val log: FactLog) : FactLog by log {
    private val reads = AtomicInteger()
    private val bothRead = CompletableDeferred<Unit>()

    override fun read(query: LogQuery): Flow<LogFact> = flow {
        val facts = log.read(query).toList()
        val count = reads.incrementAndGet()
        if (count == 2) bothRead.complete(Unit)
        if (count <= 2) bothRead.await()
        emitAll(facts.asFlow())
    }
}
