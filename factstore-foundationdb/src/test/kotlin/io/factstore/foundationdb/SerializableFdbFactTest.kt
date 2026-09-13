package io.factstore.foundationdb

import io.factstore.core.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

/**
 * FoundationDB rejects values larger than 100,000 bytes, and every fact is
 * stored as a single value. These tests check that the specification's limits
 * keep a fact inside that bound once it is actually serialized, so the
 * guarantee does not rest on arithmetic alone.
 */
class SerializableFdbFactTest {

    @Test
    fun `a fact with every value at its maximum serializes within the value limit`() {
        val fact = maximalFact()

        val encoded = fact.toSerializableFdbFact().encodeToByteArray()

        assertThat(encoded.size).isLessThan(FDB_VALUE_SIZE_LIMIT)
        assertThat(encoded.toSerializableFdbFact().toFact()).isEqualTo(fact)
    }

    private fun maximalFact(): Fact {
        return Fact(
            id = FactId.generate(),
            type = FactType("t".repeat(FactType.MAX_LENGTH)),
            payload = FactPayload(ByteArray(FactPayload.MAX_SIZE) { (it % 256).toByte() }),
            subject = Subject("s".repeat(Subject.MAX_LENGTH)),
            appendedAt = Instant.now(),
            metadata = (1..FactInput.MAX_METADATA_ENTRIES).associate { i ->
                MetadataKey("m$i".padEnd(MetadataKey.MAX_LENGTH, 'x')) to
                        MetadataValue("v".repeat(MetadataValue.MAX_LENGTH))
            },
            tags = (1..FactInput.MAX_TAGS).associate { i ->
                TagKey("t$i".padEnd(TagKey.MAX_LENGTH, 'x')) to TagValue("v".repeat(TagValue.MAX_LENGTH))
            },
        )
    }
}

private const val FDB_VALUE_SIZE_LIMIT = 100_000
