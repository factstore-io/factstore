package org.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class FactQueryTest {

    @Test
    fun `TagTypeItem constructs successfully with valid types and tags`() {
        val types = listOf("person", "event")
        val tags = listOf("key1" to "value1", "key2" to "value2")

        val item = TagTypeItem(types, tags)

        assertThat(item.types).isEqualTo(types)
        assertThat(item.tags).isEqualTo(tags)
    }

    @Test
    fun `TagTypeItem fails when types is empty`() {
        val types = emptyList<String>()
        val tags = listOf("key" to "value")

        val ex = assertThrows<IllegalArgumentException> {
            TagTypeItem(types, tags)
        }
        assertThat(ex.message).isEqualTo("Both types and tags must be defined!")
    }

    @Test
    fun `TagTypeItem fails when tags is empty`() {
        val types = listOf("person")
        val tags = emptyList<Pair<String, String>>()

        val ex = assertThrows<IllegalArgumentException> {
            TagTypeItem(types, tags)
        }
        assertThat(ex.message).isEqualTo("Both types and tags must be defined!")
    }

    @Test
    fun `TagTypeItem fails when both lists are empty`() {
        val ex = assertThrows<IllegalArgumentException> {
            TagTypeItem(emptyList(), emptyList())
        }
        assertThat(ex.message).isEqualTo("Both types and tags must be defined!")
    }

    // --- TagOnlyQueryItem tests ---

    @Test
    fun `TagOnlyQueryItem constructs successfully with tags`() {
        val tags = listOf("country" to "US")

        val item = TagOnlyQueryItem(tags)

        assertThat(item.tags).isEqualTo(tags)
    }

    @Test
    fun `TagOnlyQueryItem fails when tags is empty`() {
        val ex = assertThrows<IllegalArgumentException> {
            TagOnlyQueryItem(emptyList())
        }
        assertThat(ex.message).isEqualTo("Tags must be defined!")
    }

    // --- TagQuery tests ---

    @Test
    fun `TagQuery constructs successfully with mixed query items`() {
        val item1 = TagOnlyQueryItem(listOf("k1" to "v1"))
        val item2 = TagTypeItem(listOf("type1"), listOf("k2" to "v2"))

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