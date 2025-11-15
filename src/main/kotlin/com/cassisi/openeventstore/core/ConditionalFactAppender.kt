package com.cassisi.openeventstore.core

interface ConditionalFactAppender<P> {

    suspend fun append(fact: Fact, preCondition: P)

}

@JvmInline
value class SubjectAppendCondition(
    val expectedLatestEventId: FactId?
)

interface ConditionalSubjectFactAppender :
    ConditionalFactAppender<SubjectAppendCondition>,
    ConditionalBatchFactAppender


data class MultiSubjectAppendCondition(
    val expectedLastEventIds: Map<Pair<String, String>, FactId?> // Map<(subjectType, subjectId), expectedLastFactId>
)

interface ConditionalBatchFactAppender {
    suspend fun append(facts: List<Fact>, preCondition: MultiSubjectAppendCondition)
}

data class TagQueryBasedAppendCondition(
    val failIfEventsMatch: TagQuery,
    val after: FactId?
)

interface ConditionalTagQueryFactAppender {

    suspend fun append(facts: List<Fact>, condition: TagQueryBasedAppendCondition)

}

class AppendConditionViolationException(message: String) : FactStoreException(message)
