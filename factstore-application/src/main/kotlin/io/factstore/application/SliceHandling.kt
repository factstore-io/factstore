package io.factstore.application

import com.fraktalio.fmodel.domain.IDecider
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold

sealed interface DecisionResult<out F, out V> {

    class Recorded<F, V>(val facts: Flow<VersionedFact<F, V>>) : DecisionResult<F, V>

    data object ConditionViolated : DecisionResult<Nothing, Nothing>
}

suspend fun <C, S, F, Q, A, V> Slice<C, S, F, Q, A, V>.handle(command: C): DecisionResult<F, V> {
    val query = query(command)
    val read = fetch(query).foldInto(this)
    val newFactsToSave = decide(command, read.state)
    val appendCondition = condition(command, query, read.version)
    return save(newFactsToSave, appendCondition).toDecisionResult()
}

private fun <F, V> SaveResult<F, V>.toDecisionResult(): DecisionResult<F, V> = when (this) {
    is SaveResult.Saved -> DecisionResult.Recorded(facts)
    SaveResult.ConditionViolated -> DecisionResult.ConditionViolated
}

private data class FoldedRead<S, V>(val state: S, val version: V?)

// The state and the version come from one collection: a second one could read newer facts.
private suspend fun <S, F, V> Flow<VersionedFact<F, V>>.foldInto(decider: IDecider<*, S, F>): FoldedRead<S, V> =
    fold(
        initial = FoldedRead(state = decider.initialState, version = null),
    ) { read, (fact, version) ->
        FoldedRead(
            state = decider.evolve(read.state, fact),
            version = version,
        )
    }
