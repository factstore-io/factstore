package org.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

class StreamingOptionsTest {

    @Test
    fun `should create StreamingOptionSet with default values`() {
        val options = StreamingOptions()

        assertThat(options.startPosition).isEqualTo(StartPosition.Beginning)
    }

    @Test
    fun `should create StreamingOptionSet with custom values`() {
        val lastSeenId = FactId(UUID.randomUUID())

        val options = StreamingOptions(
            startPosition = StartPosition.After(lastSeenId),
        )

        assertThat(options.startPosition).isEqualTo(StartPosition.After(lastSeenId))
    }

}
