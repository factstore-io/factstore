package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

class StreamFactsRequestTest {

    @Test
    fun `should create StreamFactsRequest with default start position`() {
        val storeName = StoreName("test-store")
        val request = StreamFactsRequest(storeName)

        assertThat(request.storeName).isEqualTo(storeName)
        assertThat(request.startPosition).isEqualTo(StartPosition.Beginning)
    }

    @Test
    fun `should create StreamFactsRequest with custom start position`() {
        val storeName = StoreName("test-store")
        val lastSeenId = FactId(UUID.randomUUID())

        val request = StreamFactsRequest(
            storeName = storeName,
            startPosition = StartPosition.After(lastSeenId),
        )

        assertThat(request.startPosition).isEqualTo(StartPosition.After(lastSeenId))
    }

}
