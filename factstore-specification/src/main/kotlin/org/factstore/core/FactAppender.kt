package org.factstore.core

interface FactAppender {

    suspend fun append(fact: Fact)

    suspend fun append(facts: List<Fact>)

    suspend fun append(request: AppendRequest): AppendResult

}