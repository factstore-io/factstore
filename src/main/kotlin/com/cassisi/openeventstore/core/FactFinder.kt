package com.cassisi.openeventstore.core

import java.time.Instant

interface FactFinder {

    suspend fun findById(factId: FactId): Fact?

    suspend fun existsById(factId: FactId): Boolean

    suspend fun findInTimeRange(start: Instant, end: Instant = Instant.now()): List<Fact>

    suspend fun findBySubject(subjectType: String, subjectId: String): List<Fact>

    suspend fun findByTags(tags: List<Pair<String, String>>): List<Fact>

    suspend fun findByTagQuery(query: TagQuery): List<Fact>

}
