package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

class SubscribeRequestTest {

    @Test
    fun `should create SubscribeRequest with default start position`() {
        val storeName = StoreName("test-store")
        val request = SubscribeRequest(storeName)

        assertThat(request.storeName).isEqualTo(storeName)
        assertThat(request.startPosition).isEqualTo(StartPosition.Beginning)
    }

    @Test
    fun `should create SubscribeRequest with custom start position`() {
        val lastSeenId = FactId(UUID.randomUUID())

        val request = SubscribeRequest(
            storeName = StoreName("test-store"),
            startPosition = StartPosition.After(lastSeenId),
        )

        assertThat(request.startPosition).isEqualTo(StartPosition.After(lastSeenId))
    }

}
