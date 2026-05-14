package io.factstore.server.http

import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import io.restassured.http.ContentType.JSON
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.Matchers.hasSize
import org.junit.jupiter.api.*
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import java.util.*
import java.util.Base64

@QuarkusTest
@TestMethodOrder(OrderAnnotation::class)
class QueryResourceTest {

    private val storeName = "query-test-store"
    private val subject = "user-42"

    @Test
    @Order(1)
    @DisplayName("GET /v1/stores/{name}/facts/{factId} - Should return 200 when fact exists")
    fun findById() {
        // 1. Seed a fact
        val factId = UUID.randomUUID()
        seedFact(factId, subject)

        // 2. Query by ID
        val response = given()
            .pathParam("storeName", storeName)
            .pathParam("factId", factId)
            .`when`()
            .get("/v1/stores/{storeName}/facts/{factId}")
            .then()
            .statusCode(200)
            .extract().`as`(FactHttp::class.java)

        assertThat(response.id).isEqualTo(factId)
        assertThat(response.subject).isEqualTo(subject)
    }

    @Test
    @Order(2)
    @DisplayName("GET /v1/stores/{name}/subjects/{subject}/facts - Should return list of facts for subject")
    fun findBySubject() {
        given()
            .pathParam("storeName", storeName)
            .pathParam("subject", subject)
            .queryParam("limit", 10)
            .queryParam("direction", "forward")
            .`when`()
            .get("/v1/stores/{storeName}/subjects/{subject}/facts")
            .then()
            .statusCode(200)
            .body("$", hasSize<Any>(1))
    }

    @Test
    @Order(3)
    @DisplayName("GET /v1/stores/{name}/facts - Should return 400 ApiError when tags and time range are combined")
    fun findFactsConflict() {
        val error = given()
            .pathParam("storeName", storeName)
            .queryParam("tag", "category=books")
            .queryParam("from", "2024-01-01T00:00:00Z")
            .`when`()
            .get("/v1/stores/{storeName}/facts")
            .then()
            .statusCode(400)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.Conflict)
        assertThat(error.message).contains("Combining tag filters with time range is not yet supported")
    }

    @Test
    @Order(4)
    @DisplayName("GET /v1/stores/{name}/facts - Should return facts filtered by tags")
    fun findByTags() {
        // Seeding a specific tagged fact
        seedFact(UUID.randomUUID(), "tagged-sub", mapOf("region" to "europe"))

        val results = given()
            .pathParam("storeName", storeName)
            .queryParam("tag", "region=europe")
            .`when`()
            .get("/v1/stores/{storeName}/facts")
            .then()
            .statusCode(200)
            .extract().jsonPath().getList(".", FactHttp::class.java)

        assertThat(results).isNotEmpty
        assertThat(results.first().tags).containsEntry("region", "europe")
    }

    @Test
    @Order(5)
    @DisplayName("GET /v1/stores/{name}/facts/{factId} - Should return 404 ApiError when fact missing")
    fun findByIdNotFound() {
        val randomId = UUID.randomUUID()
        val error = given()
            .pathParam("storeName", storeName)
            .pathParam("factId", randomId)
            .`when`()
            .get("/v1/stores/{storeName}/facts/{factId}")
            .then()
            .statusCode(404)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.NotFound)
        assertThat(error.details).containsEntry("id", randomId.toString())
    }

    // Helper to seed data via the already tested Store and Append APIs
    private fun seedFact(id: UUID, sub: String, tags: Map<String, String> = emptyMap()) {
        // Ensure store exists
        given().contentType(JSON).body(mapOf("name" to storeName)).post("/v1/stores")

        val base64Data = Base64.getEncoder().encodeToString("test-payload".toByteArray())
        val appendRequest = mapOf(
            "facts" to listOf(
                mapOf(
                    "id" to id,
                    "type" to "test.type",
                    "subject" to sub,
                    "payload" to mapOf("data" to base64Data),
                    "tags" to tags
                )
            )
        )
        given()
            .pathParam("storeName", storeName)
            .contentType(JSON)
            .body(appendRequest)
            .post("/v1/stores/{storeName}/facts")
            .then().statusCode(200)
    }

}
