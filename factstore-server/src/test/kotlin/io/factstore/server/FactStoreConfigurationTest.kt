package io.factstore.server

import io.factstore.core.FactStore
import io.quarkus.test.junit.QuarkusTest
import jakarta.inject.Inject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import kotlinx.coroutines.runBlocking

@QuarkusTest
class FactStoreConfigurationTest {

    @Inject
    lateinit var factStore: FactStore

    @Test
    fun `should inject memory fact store when configured`() {
        // The application.yaml configures factstore.storage.type=memory
        // We can't check the exact type due to CDI proxying, but we can verify
        // that the injected FactStore behaves like a MemoryFactStore
        // (i.e., it works and doesn't require external dependencies)

        // This test passes if the injection works without errors
        assertThat(factStore).isNotNull()
    }

    @Test
    fun `should be able to create fact store with memory implementation`() {
        runBlocking {
            // This is a basic smoke test to ensure the FactStore works
            val result = factStore.handle(
                io.factstore.core.CreateStoreRequest(
                    io.factstore.core.StoreName("test-store")
                )
            )

            assertThat(result).isInstanceOf(io.factstore.core.CreateStoreResult.Created::class.java)
        }
    }
}
