package io.factstore.application

import kotlinx.coroutines.flow.Flow

interface FactRepository<F, Q, A, V> {

    fun fetch(query: Q): Flow<VersionedFact<F, V>>

    suspend fun save(facts: Flow<F>, condition: A): SaveResult<F, V>
}

data class VersionedFact<out F, out V>(val fact: F, val version: V)

sealed interface SaveResult<out F, out V> {

    class Saved<F, V>(val facts: Flow<VersionedFact<F, V>>) : SaveResult<F, V>

    data object ConditionViolated : SaveResult<Nothing, Nothing>
}
