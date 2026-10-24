package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class FactQueryValidationTest {

    @Test
    @DisplayName("A query requires at least one item")
    fun itemsMustNotBeEmpty() {
        val ex = assertThrows<IllegalArgumentException> { FactQuery(emptyList()) }

        assertThat(ex.message).isEqualTo("A query must contain at least one item.")
    }

    @Test
    @DisplayName("A query is limited in the number of items")
    fun itemsAreLimited() {
        val items = (1..FactQuery.MAX_ITEMS + 1).map { FactQueryItem(types = setOf(FactType("T$it"))) }

        val ex = assertThrows<IllegalArgumentException> { FactQuery(items) }

        assertThat(ex.message).contains("not contain more than ${FactQuery.MAX_ITEMS} items")
    }

    @Test
    @DisplayName("A query item must constrain something")
    fun itemMustConstrainAPredicate() {
        val ex = assertThrows<IllegalArgumentException> { FactQueryItem() }

        assertThat(ex.message).isEqualTo("A query item must constrain at least one of subjects, types or tags.")
    }

    @Test
    @DisplayName("A query item is limited in subjects, types and tags")
    fun predicatesAreLimited() {
        val subjects = (1..FactQueryItem.MAX_SUBJECTS + 1).map { Subject("subject/$it") }.toSet()
        val types = (1..FactQueryItem.MAX_TYPES + 1).map { FactType("TYPE_$it") }.toSet()
        val tags = (1..FactInput.MAX_TAGS + 1).associate { TagKey("key$it") to TagValue("value") }

        assertThat(assertThrows<IllegalArgumentException> { FactQueryItem(subjects = subjects) }.message)
            .contains("not match more than ${FactQueryItem.MAX_SUBJECTS} subjects")
        assertThat(assertThrows<IllegalArgumentException> { FactQueryItem(types = types) }.message)
            .contains("not match more than ${FactQueryItem.MAX_TYPES} types")
        assertThat(assertThrows<IllegalArgumentException> { FactQueryItem(tags = tags) }.message)
            .contains("At most ${FactInput.MAX_TAGS} tags")
    }

    @Test
    @DisplayName("A query item accepts as many tags as a fact can carry")
    fun maximumTagsAreAccepted() {
        val tags = (1..FactInput.MAX_TAGS).associate { TagKey("key$it") to TagValue("value") }

        assertThat(FactQueryItem(tags = tags).tags).hasSize(FactInput.MAX_TAGS)
    }

    @Test
    @DisplayName("A query item matches a fact only when every predicate it sets holds")
    fun matchesEveryPredicate() {
        val fact = Fact(
            id = FactId.generate(),
            type = FactType("USER_CREATED"),
            payload = "{}".toFactPayload(),
            subject = Subject("user/1"),
            appendedAt = java.time.Instant.now(),
            tags = mapOf(TagKey("role") to TagValue("admin")),
        )

        assertThat(FactQueryItem(subjects = setOf(Subject("user/1"))).matches(fact)).isTrue()
        assertThat(FactQueryItem(types = setOf(FactType("USER_CREATED")), tags = mapOf(TagKey("role") to TagValue("admin"))).matches(fact)).isTrue()
        assertThat(FactQueryItem(types = setOf(FactType("USER_LOCKED"))).matches(fact)).isFalse()
        assertThat(FactQueryItem(subjects = setOf(Subject("user/1")), tags = mapOf(TagKey("role") to TagValue("user"))).matches(fact)).isFalse()
    }
}
