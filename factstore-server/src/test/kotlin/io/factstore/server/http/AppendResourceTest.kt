package io.factstore.server.http

import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import io.restassured.http.ContentType
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import java.util.UUID

@QuarkusTest
@TestMethodOrder(OrderAnnotation::class)
class AppendResourceTest {

    private val storeName = "append-test-store"

    @BeforeEach
    fun setup() {
        // Ensure store exists before appending
        given()
            .contentType(ContentType.JSON)
            .body(mapOf("name" to storeName))
            .post("/v1/stores")
    }

    @Test
    @Order(1)
    @DisplayName("POST /v1/stores/{name}/facts - Should return 200 when facts are appended")
    fun appendFacts() {
        val request = mapOf(
            "facts" to listOf(
                mapOf(
                    "type" to "order.created",
                    "subject" to "order-123",
                    "payload" to mapOf("data" to "e1t9".toByteArray()) // Base64 in real JSON
                )
            )
        )

        given()
            .pathParam("storeName", storeName)
            .contentType(ContentType.JSON)
            .body(request)
            .`when`()
            .post("/v1/stores/{storeName}/facts")
            .then()
            .statusCode(200)
    }

    @Test
    @Order(2)
    @DisplayName("POST /v1/stores/{name}/facts - Should return 400 ApiError when facts list is empty")
    fun appendFactsInvalidRequest() {
        val request = mapOf(
            "facts" to emptyList<Any>()
        )

        val error = given()
            .pathParam("storeName", storeName)
            .contentType(ContentType.JSON)
            .body(request)
            .`when`()
            .post("/v1/stores/{storeName}/facts")
            .then()
            .statusCode(400)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.InvalidInput)
    }

    @Test
    @Order(3)
    @DisplayName("POST /v1/stores/{name}/facts - Should return 409 ApiError when condition fails")
    fun appendFactsConditionConflict() {
        val factId = UUID.randomUUID()
        val request = mapOf(
            "facts" to listOf(
                mapOf(
                    "type" to "test",
                    "subject" to "sub-1",
                    "payload" to mapOf("data" to "abc".toByteArray())
                )
            ),
            "condition" to mapOf(
                "type" to "expectedLastFact",
                "subject" to "sub-1",
                "expectedLastFactId" to factId // This ID won't exist in a fresh store
            )
        )

        val error = given()
            .pathParam("storeName", storeName)
            .contentType(ContentType.JSON)
            .body(request)
            .`when`()
            .post("/v1/stores/{storeName}/facts")
            .then()
            .statusCode(409)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.ConditionViolated)
        assertThat(error.message).contains("condition was not satisfied")
    }

    @Test
    @Order(4)
    @DisplayName("POST /v1/stores/{name}/facts - Should return 404 ApiError when store does not exist")
    fun appendFactsStoreNotFound() {
        val unknownStore = "ghost-store"

        val error = given()
            .pathParam("storeName", unknownStore)
            .contentType(ContentType.JSON)
            .body(mapOf("facts" to listOf(mapOf("type" to "t", "subject" to "s", "payload" to mapOf("data" to "data".toByteArray())))))
            .`when`()
            .post("/v1/stores/{storeName}/facts")
            .then()
            .statusCode(404)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.NotFound)
        assertThat(error.details).containsEntry("name", unknownStore)
    }
}
