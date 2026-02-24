package io.factstore.fmodel

import com.fraktalio.fmodel.application.EventComputation
import com.fraktalio.fmodel.domain.IDecider
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow

fun interface CommandToQueryMapper<C, Q> {

    fun C.toQuery(): Q
}

interface DcbEventLockingRepository<Q, E, V> {

    suspend fun Q.fetchEvents(): Pair<Flow<E>, V?>

    fun Flow<E>.save(query: Q, version: V?): Flow<E>

}

interface EventSourcingLockingDecisionModel<C, S, E, Q, V> :
    EventComputation<C, S, E>,
    DcbEventLockingRepository<Q, E, V>,
    CommandToQueryMapper<C, Q>

fun <C, S, E, Q, V> EventSourcingLockingDecisionModel(
    decider: IDecider<C, S, E>,
    mapper: CommandToQueryMapper<C, Q>,
    eventRepository: DcbEventLockingRepository<Q, E, V>
): EventSourcingLockingDecisionModel<C, S, E, Q, V> =
    object : EventSourcingLockingDecisionModel<C, S, E, Q, V>,
        DcbEventLockingRepository<Q, E, V> by eventRepository,
        IDecider<C, S, E> by decider,
        CommandToQueryMapper<C, Q> by mapper {}

fun <C, S, E, Q, V> EventSourcingLockingDecisionModel<C, S, E, Q, V>.handleOptimistically(
    command: C
): Flow<E> = flow {
    val query = command.toQuery()
    val (events, lastId) = query.fetchEvents()
    val newEvents = events.computeNewEvents(command)
    val savedEvents = newEvents.save(query, lastId)
    emitAll(savedEvents)
}
