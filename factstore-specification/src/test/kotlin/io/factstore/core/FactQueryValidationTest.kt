package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class FactQueryValidationTest {

    @Test
    @DisplayName("A query requires at least one filter")
    fun filtersMustNotBeEmpty() {
        val ex = assertThrows<IllegalArgumentException> { FactQuery(emptyList()) }

        assertThat(ex.message).isEqualTo("A query must contain at least one filter.")
    }

    @Test
    @DisplayName("A query is limited in the number of filters")
    fun filtersAreLimited() {
        val filters = (1..FactQuery.MAX_FILTERS + 1).map { FactFilter(types = setOf(FactType("T$it"))) }

        val ex = assertThrows<IllegalArgumentException> { FactQuery(filters) }

        assertThat(ex.message).contains("not contain more than ${FactQuery.MAX_FILTERS} filters")
    }

    @Test
    @DisplayName("A filter must constrain something")
    fun filterMustConstrainAPredicate() {
        val ex = assertThrows<IllegalArgumentException> { FactFilter() }

        assertThat(ex.message).isEqualTo("A filter must constrain at least one of subjects, types or tags.")
    }

    @Test
    @DisplayName("A filter is limited in subjects, types and tags")
    fun predicatesAreLimited() {
        val subjects = (1..FactFilter.MAX_SUBJECTS + 1).map { Subject("subject/$it") }.toSet()
        val types = (1..FactFilter.MAX_TYPES + 1).map { FactType("TYPE_$it") }.toSet()
        val tags = (1..FactInput.MAX_TAGS + 1).associate { TagKey("key$it") to TagValue("value") }

        assertThat(assertThrows<IllegalArgumentException> { FactFilter(subjects = subjects) }.message)
            .contains("not match more than ${FactFilter.MAX_SUBJECTS} subjects")
        assertThat(assertThrows<IllegalArgumentException> { FactFilter(types = types) }.message)
            .contains("not match more than ${FactFilter.MAX_TYPES} types")
        assertThat(assertThrows<IllegalArgumentException> { FactFilter(tags = tags) }.message)
            .contains("At most ${FactInput.MAX_TAGS} tags")
    }

    @Test
    @DisplayName("A filter matches a fact only when every predicate it sets holds")
    fun matchesEveryPredicate() {
        val fact = Fact(
            id = FactId.generate(),
            type = FactType("USER_CREATED"),
            payload = "{}".toFactPayload(),
            subject = Subject("user/1"),
            appendedAt = java.time.Instant.now(),
            tags = mapOf(TagKey("role") to TagValue("admin")),
        )

        assertThat(FactFilter(subjects = setOf(Subject("user/1"))).matches(fact)).isTrue()
        assertThat(FactFilter(types = setOf(FactType("USER_CREATED")), tags = mapOf(TagKey("role") to TagValue("admin"))).matches(fact)).isTrue()
        assertThat(FactFilter(types = setOf(FactType("USER_LOCKED"))).matches(fact)).isFalse()
        assertThat(FactFilter(subjects = setOf(Subject("user/1")), tags = mapOf(TagKey("role") to TagValue("user"))).matches(fact)).isFalse()
    }
}
