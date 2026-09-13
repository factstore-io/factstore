package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

class FindByTagsRequestTest {

    private val storeName = StoreName("orders")

    @Test
    fun `rejects a request without tags`() {
        assertThat(catchThrowable { FindByTagsRequest(storeName, emptyMap()) })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("Tags must be defined!")
    }

    @Test
    fun `accepts as many tags as a fact can carry`() {
        assertDoesNotThrow { FindByTagsRequest(storeName, tags(FactInput.MAX_TAGS)) }
    }

    @Test
    fun `rejects more tags than a fact can carry`() {
        assertThat(catchThrowable { FindByTagsRequest(storeName, tags(FactInput.MAX_TAGS + 1)) })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(FactInput.MAX_TAGS.toString())
    }

    private fun tags(count: Int) = (1..count).associate { TagKey("key$it") to TagValue("value$it") }
}
