package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

class FactInputTest {

    @Test
    fun `a fact carries at most five tags and twenty metadata entries`() {
        assertThat(FactInput.MAX_TAGS).isEqualTo(5)
        assertThat(FactInput.MAX_METADATA_ENTRIES).isEqualTo(20)
    }

    @Test
    fun `accepts a fact without tags and metadata`() {
        assertDoesNotThrow { input() }
    }

    @Test
    fun `accepts the maximum number of tags and metadata entries`() {
        assertDoesNotThrow { input(tagCount = FactInput.MAX_TAGS, metadataCount = FactInput.MAX_METADATA_ENTRIES) }
    }

    @Test
    fun `rejects one tag beyond the maximum`() {
        assertThat(catchThrowable { input(tagCount = FactInput.MAX_TAGS + 1) })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(FactInput.MAX_TAGS.toString())
    }

    @Test
    fun `rejects one metadata entry beyond the maximum`() {
        assertThat(catchThrowable { input(metadataCount = FactInput.MAX_METADATA_ENTRIES + 1) })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(FactInput.MAX_METADATA_ENTRIES.toString())
    }

    @Test
    fun `byte size counts the payload and every text value`() {
        val input = FactInput(
            type = FactType("ORDER_PLACED"),
            subject = Subject("order/1"),
            payload = FactPayload(ByteArray(100)),
            metadata = mapOf(MetadataKey("source") to MetadataValue("shop")),
            tags = mapOf(TagKey("order") to TagValue("1"), TagKey("archived") to TagValue("")),
        )

        val payload = 100
        val typeAndSubject = "ORDER_PLACED".length + "order/1".length
        val metadata = "source".length + "shop".length
        val tags = "order".length + "1".length + "archived".length

        assertThat(input.byteSize).isEqualTo(payload + typeAndSubject + metadata + tags)
    }

    private fun input(tagCount: Int = 0, metadataCount: Int = 0) = FactInput(
        type = FactType("ORDER_PLACED"),
        subject = Subject("order/1"),
        payload = "{}".toFactPayload(),
        metadata = (1..metadataCount).associate { MetadataKey("key$it") to MetadataValue("value$it") },
        tags = (1..tagCount).associate { TagKey("key$it") to TagValue("value$it") },
    )
}
