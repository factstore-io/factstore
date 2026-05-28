package io.factstore.server.grpc

import io.quarkus.grpc.GrpcClient
import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.coroutines.awaitSuspending
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.junit.jupiter.api.Test

@QuarkusTest
class GrpcInfoServiceTest {

    @GrpcClient
    lateinit var infoService: InfoService

    @ConfigProperty(name = "quarkus.application.version")
    private lateinit var version: String

    @Test
    fun `getServerInfo returns expected app metadata`(): Unit = runBlocking {
        val info = infoService.getServerInfo(getServerInfoRequest { }).awaitSuspending()

        assertThat(info.app).isEqualTo("factstore-server-test")
        assertThat(info.version).isEqualTo(version)
        assertThat(info.storageBackend).isEqualTo("memory")
    }
}
