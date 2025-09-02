package com.cassisi.openeventstore.core.dcb

interface FactAppender {

    suspend fun append(fact: Fact)

    suspend fun append(facts: List<Fact>)

}