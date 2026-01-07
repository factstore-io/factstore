package org.factstore.core

import java.util.UUID

@JvmInline
value class IdempotencyKey(val value: UUID = UUID.randomUUID())

data class AppendRequest(
    val facts: List<Fact>,
    val idempotencyKey: IdempotencyKey,
    val condition: AppendCondition = AppendCondition.None
)

sealed interface AppendCondition {

    data object None : AppendCondition

    data class ExpectedLastFact(
        val subjectRef: SubjectRef,
        val expectedLastFactId: FactId?
    ) : AppendCondition

    data class ExpectedMultiSubjectLastFact(
        val expectations: Map<SubjectRef, FactId?>
    ) : AppendCondition

    data class TagQueryBased(
        val failIfEventsMatch: TagQuery,
        val after: FactId?
    ) : AppendCondition

}