package io.factstore.client.model

sealed class AppendCondition {

    data class ExpectedLastFact(
        val subject: String,
        val expectedLastFactId: String? = null,
    ) : AppendCondition()

    data class ExpectedMultiSubjectLastFact(
        val expectations: List<SubjectExpectation>,
    ) : AppendCondition()

    data class TagQueryBased(
        val failIfEventsMatch: TagQuery,
        val afterFactId: String? = null,
    ) : AppendCondition()
}

data class SubjectExpectation(
    val subject: String,
    val expectedLastFactId: String? = null,
)
