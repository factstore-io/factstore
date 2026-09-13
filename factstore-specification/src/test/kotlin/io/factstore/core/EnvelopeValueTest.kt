package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

/**
 * Validation of the client-supplied text values of the fact envelope.
 *
 * These types are constructed from untrusted input on every append, so the
 * cases below pin down both what they must accept — the naming conventions the
 * documentation and the examples promise — and what they must reject.
 */
class EnvelopeValueTest {

    private val conformingValues = listOf(
        "a",
        "A",
        "9",
        "ORDER_PLACED",
        "OrderPlaced",
        "com.acme.OrderPlaced",
        "order/12345",
        "USER:ALICE",
        "tenant/acme/order/98765",
        "a-b_c.d:e/f",
    )

    private val nonConformingValues = mapOf(
        "" to "empty",
        " " to "blank",
        "order/1 " to "trailing whitespace",
        " order/1" to "leading whitespace",
        "order 1" to "inner whitespace",
        "order\t1" to "tab",
        "order\n1" to "newline",
        "order\u00001" to "NUL",
        "order\u001b[2J1" to "ANSI escape sequence",
        "-order" to "leading separator",
        "order-" to "trailing separator",
        "/order" to "leading slash",
        "order/" to "trailing slash",
        "München" to "non-ASCII latin",
        "顧客/4711" to "non-ASCII CJK",
        "order/\ud800" to "unpaired high surrogate",
        "order/\udc00" to "unpaired low surrogate",
        "order'1" to "quote",
        "order@1" to "at sign",
        "order#1" to "hash",
        "order,1" to "comma",
    )

    @Test
    fun `subject accepts the documented naming conventions`() {
        conformingValues.forEach { value ->
            assertThat(Subject(value).value).isEqualTo(value)
        }
    }

    @Test
    fun `subject rejects non-conforming values`() {
        nonConformingValues.forEach { (value, reason) ->
            assertThat(catchThrowable { Subject(value) })
                .describedAs("Subject should reject %s", reason)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `fact type and tag key and metadata key reject non-conforming values`() {
        nonConformingValues.forEach { (value, reason) ->
            assertThat(catchThrowable { FactType(value) })
                .describedAs("FactType should reject %s", reason)
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThat(catchThrowable { TagKey(value) })
                .describedAs("TagKey should reject %s", reason)
                .isInstanceOf(IllegalArgumentException::class.java)
            assertThat(catchThrowable { MetadataKey(value) })
                .describedAs("MetadataKey should reject %s", reason)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `tag value and metadata value accept the empty string for presence-only semantics`() {
        assertDoesNotThrow { TagValue("") }
        assertDoesNotThrow { MetadataValue("") }
    }

    @Test
    fun `tag value and metadata value reject non-conforming non-empty values`() {
        nonConformingValues
            .filterKeys { it.isNotEmpty() }
            .forEach { (value, reason) ->
                assertThat(catchThrowable { TagValue(value) })
                    .describedAs("TagValue should reject %s", reason)
                    .isInstanceOf(IllegalArgumentException::class.java)
                assertThat(catchThrowable { MetadataValue(value) })
                    .describedAs("MetadataValue should reject %s", reason)
                    .isInstanceOf(IllegalArgumentException::class.java)
            }
    }

    @Test
    fun `values are accepted at their maximum length and rejected one character beyond`() {
        val cases = listOf<Triple<String, Int, (String) -> Any>>(
            Triple("Subject", Subject.MAX_LENGTH) { Subject(it) },
            Triple("Fact type", FactType.MAX_LENGTH) { FactType(it) },
            Triple("Tag key", TagKey.MAX_LENGTH) { TagKey(it) },
            Triple("Tag value", TagValue.MAX_LENGTH) { TagValue(it) },
            Triple("Metadata key", MetadataKey.MAX_LENGTH) { MetadataKey(it) },
            Triple("Metadata value", MetadataValue.MAX_LENGTH) { MetadataValue(it) },
        )

        cases.forEach { (field, maxLength, construct) ->
            assertDoesNotThrow("$field should accept $maxLength characters") {
                construct("a".repeat(maxLength))
            }

            assertThat(catchThrowable { construct("a".repeat(maxLength + 1)) })
                .describedAs("%s should reject %d characters", field, maxLength + 1)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(maxLength.toString())
        }
    }

    @Test
    fun `conversion helpers delegate to the value types`() {
        assertThat("order/1".toSubject()).isEqualTo(Subject("order/1"))
        assertThat("ORDER_PLACED".toFactType()).isEqualTo(FactType("ORDER_PLACED"))
        assertThat("region".toTagKey()).isEqualTo(TagKey("region"))
        assertThat("eu".toTagValue()).isEqualTo(TagValue("eu"))
        assertThat("correlation-id".toMetadataKey()).isEqualTo(MetadataKey("correlation-id"))
        assertThat("abc".toMetadataValue()).isEqualTo(MetadataValue("abc"))
        assertThat("my-store".toStoreName()).isEqualTo(StoreName("my-store"))
    }

    @Test
    fun `conversion helpers do not normalize their input`() {
        assertThat(catchThrowable { " order/1 ".toSubject() })
            .describedAs("toSubject() must not silently trim, matching String.toInt()")
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
