package com.cassisi.openeventstore.core.dcb

import java.util.UUID

interface ConditionalFactAppender<P> {

    suspend fun append(fact: Fact, preCondition: P)

}

@JvmInline
value class SubjectAppendCondition(
    val expectedLatestEventId: UUID?
)

interface ConditionalSubjectFactAppender : ConditionalFactAppender<SubjectAppendCondition>
