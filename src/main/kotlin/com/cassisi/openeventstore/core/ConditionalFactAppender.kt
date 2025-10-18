package com.cassisi.openeventstore.core

import java.util.UUID

interface ConditionalFactAppender<P> {

    suspend fun append(fact: Fact, preCondition: P)

}

@JvmInline
value class SubjectAppendCondition(
    val expectedLatestEventId: UUID?
)

interface ConditionalSubjectFactAppender :
    ConditionalFactAppender<SubjectAppendCondition>,
    ConditionalBatchFactAppender


data class MultiSubjectAppendCondition(
    val expectedLastEventIds: Map<Pair<String, String>, UUID?> // Map<(subjectType, subjectId), expectedLastFactId>
)

interface ConditionalBatchFactAppender {
    suspend fun append(facts: List<Fact>, preCondition: MultiSubjectAppendCondition)
}

data class TagQueryBasedAppendCondition(
    val failIfEventsMatch: TagQuery,
    val after: UUID?
)

interface ConditionalTagQueryFactAppender {

    suspend fun append(facts: List<Fact>, condition: TagQueryBasedAppendCondition)

}