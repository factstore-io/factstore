package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

class FactPayloadTest {

    @Test
    fun `maximum size is 64 KiB`() {
        assertThat(FactPayload.MAX_SIZE).isEqualTo(64 * 1024)
    }

    @Test
    fun `accepts an empty payload`() {
        assertThat(FactPayload(ByteArray(0)).data).isEmpty()
    }

    @Test
    fun `accepts a payload of exactly the maximum size`() {
        assertDoesNotThrow { FactPayload(ByteArray(FactPayload.MAX_SIZE)) }
    }

    @Test
    fun `rejects a payload one byte beyond the maximum size`() {
        assertThat(catchThrowable { FactPayload(ByteArray(FactPayload.MAX_SIZE + 1)) })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(FactPayload.MAX_SIZE.toString())
    }
}
