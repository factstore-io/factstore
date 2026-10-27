package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

class AppendFingerprintTest {

    private val alice = FactInput(
        type = FactType("USER_CREATED"),
        subject = Subject("user/alice"),
        payload = """{ "name": "Alice" }""".toFactPayload(),
        metadata = mapOf(MetadataKey("source") to MetadataValue("test"), MetadataKey("trace") to MetadataValue("1")),
        tags = mapOf(TagKey("region") to TagValue("eu"), TagKey("role") to TagValue("admin")),
    )

    private val bob = alice.copy(subject = Subject("user/bob"), payload = """{ "name": "Bob" }""".toFactPayload())

    private fun request(
        facts: List<FactInput> = listOf(alice),
        condition: AppendCondition = AppendCondition.None,
        storeName: StoreName = StoreName("store"),
        idempotencyKey: IdempotencyKey = IdempotencyKey.generate(),
    ) = AppendRequest(storeName, facts, idempotencyKey, condition)

    private fun noneMatch(vararg items: FactQueryItem, after: FactId? = null) =
        AppendCondition.NoneMatch(FactQuery(items.toList()), after)

    @Test
    fun `a fingerprint is a SHA-256 digest`() {
        assertThat(request().fingerprint().toByteArray()).hasSize(AppendFingerprint.SIZE)
        assertThat(catchThrowable { AppendFingerprint(ByteArray(16)) }).isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `the store and the idempotency key are not part of the fingerprint`() {
        assertThat(request(storeName = StoreName("other"), idempotencyKey = IdempotencyKey.generate()).fingerprint())
            .isEqualTo(request().fingerprint())
    }

    @Test
    fun `every part of a fact is part of the fingerprint`() {
        val variants = listOf(
            alice.copy(type = FactType("USER_DELETED")),
            alice.copy(subject = Subject("user/alicia")),
            alice.copy(payload = """{ "name": "Alicia" }""".toFactPayload()),
            alice.copy(metadata = alice.metadata + (MetadataKey("trace") to MetadataValue("2"))),
            alice.copy(tags = alice.tags + (TagKey("role") to TagValue("user"))),
            alice.copy(tags = alice.tags - TagKey("role")),
        )

        variants.forEach {
            assertThat(request(facts = listOf(it)).fingerprint()).isNotEqualTo(request().fingerprint())
        }
    }

    @Test
    fun `the order of the facts is part of the fingerprint`() {
        assertThat(request(facts = listOf(alice, bob)).fingerprint())
            .isNotEqualTo(request(facts = listOf(bob, alice)).fingerprint())
    }

    @Test
    fun `the order of metadata and tags is not part of the fingerprint`() {
        val reordered = alice.copy(
            metadata = alice.metadata.entries.reversed().associate { it.toPair() },
            tags = alice.tags.entries.reversed().associate { it.toPair() },
        )

        assertThat(request(facts = listOf(reordered)).fingerprint()).isEqualTo(request().fingerprint())
    }

    @Test
    fun `values are told apart by their sizes, not by where one ends`() {
        val split = alice.copy(type = FactType("AB"), subject = Subject("C"))
        val shifted = alice.copy(type = FactType("A"), subject = Subject("BC"))

        assertThat(request(facts = listOf(split)).fingerprint())
            .isNotEqualTo(request(facts = listOf(shifted)).fingerprint())
    }

    @Test
    fun `the condition is part of the fingerprint`() {
        val factId = FactId.generate()
        val conditions = listOf(
            AppendCondition.None,
            AppendCondition.ExpectedLastFact(Subject("user/alice"), null),
            AppendCondition.ExpectedLastFact(Subject("user/alice"), factId),
            AppendCondition.ExpectedLastFact(Subject("user/bob"), factId),
            noneMatch(FactQueryItem(subjects = setOf(Subject("user/alice")))),
            noneMatch(FactQueryItem(subjects = setOf(Subject("user/alice"))), after = factId),
            noneMatch(FactQueryItem(types = setOf(FactType("user/alice")))),
            AppendCondition.All(listOf(AppendCondition.ExpectedLastFact(Subject("user/alice"), null))),
        )

        val fingerprints = conditions.map { request(condition = it).fingerprint() }

        assertThat(fingerprints).doesNotHaveDuplicates()
    }

    @Test
    fun `the order of conditions, query items and their values is not part of the fingerprint`() {
        val first = AppendCondition.ExpectedLastFact(Subject("user/alice"), null)
        val second = noneMatch(
            FactQueryItem(subjects = setOf(Subject("user/alice"), Subject("user/bob"))),
            FactQueryItem(types = setOf(FactType("A"), FactType("B")), tags = mapOf(TagKey("k1") to TagValue("v"), TagKey("k2") to TagValue(""))),
        )
        val reorderedSecond = noneMatch(
            FactQueryItem(types = setOf(FactType("B"), FactType("A")), tags = mapOf(TagKey("k2") to TagValue(""), TagKey("k1") to TagValue("v"))),
            FactQueryItem(subjects = setOf(Subject("user/bob"), Subject("user/alice"))),
        )

        assertThat(request(condition = AppendCondition.All(listOf(second, first))).fingerprint())
            .isEqualTo(request(condition = AppendCondition.All(listOf(first, reorderedSecond))).fingerprint())
    }
}
