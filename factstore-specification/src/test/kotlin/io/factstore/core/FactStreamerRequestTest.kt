package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class FactStreamerRequestTest {

    private val store = StoreName("test-store")

    @Test
    @DisplayName("A tag stream requires at least one tag")
    fun tagsMustNotBeEmpty() {
        val ex = assertThrows<IllegalArgumentException> {
            StreamFactsByTagsRequest(store, emptyMap(), ReadDirection.Forward, Limit.None)
        }

        assertThat(ex.message).isEqualTo("Tags must be defined!")
    }

    @Test
    @DisplayName("A tag stream cannot require more tags than a fact can carry")
    fun tagsMustBeSatisfiable() {
        val tags = (1..FactInput.MAX_TAGS + 1).associate { TagKey("key$it") to TagValue("value") }

        val ex = assertThrows<IllegalArgumentException> {
            StreamFactsByTagsRequest(store, tags, ReadDirection.Forward, Limit.None)
        }

        assertThat(ex.message).contains("At most ${FactInput.MAX_TAGS} tags")
    }

    @Test
    @DisplayName("A tag stream accepts as many tags as a fact can carry")
    fun maximumTagsAreAccepted() {
        val tags = (1..FactInput.MAX_TAGS).associate { TagKey("key$it") to TagValue("value") }

        val request = StreamFactsByTagsRequest(store, tags, ReadDirection.Forward, Limit.None)

        assertThat(request.tags).hasSize(FactInput.MAX_TAGS)
    }
}
