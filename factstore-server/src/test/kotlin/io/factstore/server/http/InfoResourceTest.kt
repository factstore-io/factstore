package io.factstore.server.http

import io.factstore.server.info.ServerInfo
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import org.assertj.core.api.Assertions.assertThat
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.junit.jupiter.api.Test

@QuarkusTest
class InfoResourceTest {

    @ConfigProperty(name = "quarkus.application.version")
    private lateinit var version: String // version fixed by Quarkus

    @Test
    fun testGetInfo() {

        val expectedServerInfo = ServerInfo(
            app = "factstore-server-test",
            version = version,
            storageBackend = "memory"
        )

        val actualServerInfo = given()
            .`when`()
            .get("/v1/info")
            .then()
            .statusCode(200)
            .extract().`as`(ServerInfo::class.java)

        assertThat(actualServerInfo).isEqualTo(expectedServerInfo)
    }
}