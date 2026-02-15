package org.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

class StreamingOptionSetTest {

    @Test
    fun `should create StreamingOptionSet with default values`() {
        val options = StreamingOptionSet()

        assertThat(options.lastSeenId).isNull()
        assertThat(options.batchSize).isEqualTo(1024)
        assertThat(options.pollDelayMs).isEqualTo(250L)
    }

    @Test
    fun `should create StreamingOptionSet with custom values`() {
        val lastSeenId = FactId(UUID.randomUUID())

        val options = StreamingOptionSet(
            lastSeenId = lastSeenId,
            batchSize = 50,
            pollDelayMs = 500L
        )

        assertThat(options.lastSeenId).isEqualTo(lastSeenId)
        assertThat(options.batchSize).isEqualTo(50)
        assertThat(options.pollDelayMs).isEqualTo(500L)
    }

    @Test
    fun `should reject batchSize equal to zero`() {
        assertThatThrownBy {
            StreamingOptionSet(batchSize = 0)
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("Batch size must be greater than zero, but was 0")
    }

    @Test
    fun `should reject batchSize less than zero`() {
        assertThatThrownBy {
            StreamingOptionSet(batchSize = -1)
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("Batch size must be greater than zero, but was -1")
    }

    @Test
    fun `should reject pollDelayMs equal to zero`() {
        assertThatThrownBy {
            StreamingOptionSet(pollDelayMs = 0L)
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("Poll delay must be greater than zero, but was 0")
    }

    @Test
    fun `should reject pollDelayMs less than zero`() {
        assertThatThrownBy {
            StreamingOptionSet(pollDelayMs = -10L)
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessage("Poll delay must be greater than zero, but was -10")
    }
}
