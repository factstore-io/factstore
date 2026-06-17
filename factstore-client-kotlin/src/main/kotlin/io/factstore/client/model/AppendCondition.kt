package io.factstore.client.model

sealed class AppendCondition {

    data class ExpectedLastFact(
        val subject: String,
        val expectedLastFactId: String? = null,
    ) : AppendCondition()

    data class TagQueryBased(
        val failIfEventsMatch: TagQuery,
        val afterFactId: String? = null,
    ) : AppendCondition()

    /**
     * Composite condition: all nested conditions must be satisfied (logical AND).
     * Conditions may be nested arbitrarily. A multi-subject expectation is
     * expressed as an [All] of several [ExpectedLastFact] conditions.
     */
    data class All(
        val conditions: List<AppendCondition>,
    ) : AppendCondition()
}
