package io.factstore.application

import io.factstore.core.FactId
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.core.TagValue
import io.factstore.core.toFact
import io.factstore.domain.StoreCreated
import io.factstore.domain.StoreCreationRejected
import io.factstore.domain.SystemFact
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant

class SystemFactCodecTest {

    companion object {
        private val s1 = StoreId.generate()
        private val s2 = StoreId.generate()
        private val orders = StoreName("orders")

        @JvmStatic
        fun systemFacts(): List<SystemFact> = listOf(
            StoreCreated(s1, orders),
            StoreCreationRejected(s2, orders, StoreCreationRejected.Reason.NameTaken(holder = s1)),
        )
    }

    @ParameterizedTest
    @MethodSource("systemFacts")
    fun `decodes every system fact it encodes`(fact: SystemFact) {
        val stored = SystemFactCodec.encode(fact).toFact(FactId.generate(), Instant.now())

        assertThat(SystemFactCodec.decode(stored)).isEqualTo(fact)
    }

    @Test
    fun `tags a store's creation with the store and its name`() {
        val input = SystemFactCodec.encode(StoreCreated(s1, orders))

        assertThat(input.subject.value).isEqualTo(s1.uuid.toString())
        assertThat(input.tags).containsEntry(SystemFactTags.STORE_ID, TagValue(s1.uuid.toString()))
        assertThat(input.tags).containsEntry(SystemFactTags.STORE_NAME, TagValue("orders"))
    }

    @Test
    fun `tags a rejected creation with the store that was to be created, not the holder`() {
        val input = SystemFactCodec.encode(StoreCreationRejected(s2, orders, StoreCreationRejected.Reason.NameTaken(holder = s1)))

        assertThat(input.subject.value).isEqualTo(s2.uuid.toString())
        assertThat(input.tags).containsEntry(SystemFactTags.STORE_ID, TagValue(s2.uuid.toString()))
        assertThat(input.tags).containsEntry(SystemFactTags.STORE_NAME, TagValue("orders"))
    }
}
