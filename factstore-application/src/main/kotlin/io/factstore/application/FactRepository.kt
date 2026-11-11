package io.factstore.application

import kotlinx.coroutines.flow.Flow

interface FactRepository<F, Q, A, V> {

    fun fetch(query: Q): Flow<Pair<F, V>>

    suspend fun save(facts: Flow<F>, condition: A): SaveResult<F, V>
}

sealed interface SaveResult<out F, out V> {

    class Saved<F, V>(val facts: Flow<Pair<F, V>>) : SaveResult<F, V>

    data object ConditionViolated : SaveResult<Nothing, Nothing>
}
