package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class FactQueryTest {

    @Test
    fun `TagTypeItem constructs successfully with valid types and tags`() {
        val types = setOf(FactType("person"), FactType("event"))
        val tags = mapOf(TagKey("key1") to TagValue("value1"), TagKey("key2") to TagValue("value2"))

        val item = TagTypeItem(types, tags)

        assertThat(item.types).isEqualTo(types)
        assertThat(item.tags).isEqualTo(tags)
    }

    @Test
    fun `TagTypeItem fails when types is empty`() {
        val types = emptySet<FactType>()
        val tags = mapOf(TagKey("key") to TagValue("value"))

        val ex = assertThrows<IllegalArgumentException> {
            TagTypeItem(types, tags)
        }
        assertThat(ex.message).isEqualTo("Both types and tags must be defined!")
    }

    @Test
    fun `TagTypeItem fails when tags is empty`() {
        val types = setOf(FactType("person"))
        val tags = emptyMap<TagKey, TagValue>()

        val ex = assertThrows<IllegalArgumentException> {
            TagTypeItem(types, tags)
        }
        assertThat(ex.message).isEqualTo("Both types and tags must be defined!")
    }

    @Test
    fun `TagTypeItem fails when both lists are empty`() {
        val ex = assertThrows<IllegalArgumentException> {
            TagTypeItem(emptySet(), emptyMap())
        }
        assertThat(ex.message).isEqualTo("Both types and tags must be defined!")
    }

    // --- TagOnlyQueryItem tests ---

    @Test
    fun `TagOnlyQueryItem constructs successfully with tags`() {
        val tags = mapOf(TagKey("country") to TagValue("US"))

        val item = TagOnlyQueryItem(tags)

        assertThat(item.tags).isEqualTo(tags)
    }

    @Test
    fun `TagOnlyQueryItem fails when tags is empty`() {
        val ex = assertThrows<IllegalArgumentException> {
            TagOnlyQueryItem(emptyMap())
        }
        assertThat(ex.message).isEqualTo("Tags must be defined!")
    }

    // --- TagQuery tests ---

    @Test
    fun `TagQuery constructs successfully with mixed query items`() {
        val item1 = TagOnlyQueryItem(mapOf(TagKey("k1") to TagValue("v1")))
        val item2 = TagTypeItem(setOf(FactType("type1")), mapOf(TagKey("k2") to TagValue("v2")))

        val query = TagQuery(listOf(item1, item2))

        assertThat(query.queryItems).containsExactly(item1, item2)
    }

    @Test
    fun `TagQuery accepts empty queryItems`() {
        assertThatThrownBy {
            TagQuery(emptyList())
        }.hasMessage("At least one query item must be present!")
    }
}