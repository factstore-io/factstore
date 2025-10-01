package com.cassisi.openeventstore.core.dcb

import java.time.Instant
import java.util.*

interface FactFinder {

    suspend fun findById(factId: UUID): Fact?

    suspend fun existsById(factId: UUID): Boolean

    suspend fun findInTimeRange(start: Instant, end: Instant = Instant.now()): List<Fact>

    suspend fun findBySubject(subjectType: String, subjectId: String): List<Fact>

    suspend fun findByTags(tags: List<Pair<String, String>>): List<Fact>

}

data class PayloadAttributeCondition(
    val eventType: String,
    val path: String,
    val value: String
)

data class PayloadQueryItem(
    val conditions: List<PayloadAttributeCondition>
)

data class PayloadQuery(
    val items: List<PayloadQueryItem>
)
