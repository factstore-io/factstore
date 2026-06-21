package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

class ReplayRequestTest {

    @Test
    fun `should create ReplayRequest with default start`() {
        val storeName = StoreName("test-store")
        val request = ReplayRequest(storeName)

        assertThat(request.storeName).isEqualTo(storeName)
        assertThat(request.start).isEqualTo(ReplayStart.Beginning)
    }

    @Test
    fun `should create ReplayRequest with After start`() {
        val checkpoint = FactId(UUID.randomUUID())

        val request = ReplayRequest(
            storeName = StoreName("test-store"),
            start = ReplayStart.After(checkpoint),
        )

        assertThat(request.start).isEqualTo(ReplayStart.After(checkpoint))
    }

}
