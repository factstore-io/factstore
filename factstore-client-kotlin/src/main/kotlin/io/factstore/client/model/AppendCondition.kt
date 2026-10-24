package io.factstore.client.model

sealed class AppendCondition {

    data class ExpectedLastFact(
        val subject: String,
        val expectedLastFactId: String? = null,
    ) : AppendCondition()

    /**
     * Fails the append if a fact matching [query] was appended after [afterFactId], the last fact
     * the decision read, or, without it, if any fact matches at all.
     *
     * An [afterFactId] the store does not have fails the append with
     * [io.factstore.client.exceptions.AfterFactNotFoundException].
     */
    data class NoneMatch(
        val query: FactQuery,
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
