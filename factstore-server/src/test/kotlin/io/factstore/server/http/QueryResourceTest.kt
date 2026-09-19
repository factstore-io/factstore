package io.factstore.server.http

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import io.restassured.http.ContentType.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import java.util.*
import java.util.Base64

@QuarkusTest
@TestMethodOrder(OrderAnnotation::class)
class QueryResourceTest {

    companion object {
        const val NDJSON = "application/x-ndjson"
    }

    private val storeName = "query-test-store"
    private val objectMapper = ObjectMapper()
    private val subject = "user-42"

    @Test
    @Order(1)
    @DisplayName("GET /v1/stores/{name}/facts/{factId} - Should return 200 when fact exists")
    fun findById() {
        // 1. Seed a fact
        val factId = seedFact(subject)

        // 2. Query by ID
        val response = given()
            .pathParam("storeName", storeName)
            .pathParam("factId", factId)
            .`when`()
            .get("/api/v1/stores/{storeName}/facts/{factId}")
            .then()
            .statusCode(200)
            .extract().`as`(FactHttp::class.java)

        assertThat(response.id).isEqualTo(factId)
        assertThat(response.subject).isEqualTo(subject)
    }

    @Test
    @Order(2)
    @DisplayName("GET /v1/stores/{name}/facts?subject - Should stream the subject's facts as NDJSON")
    fun streamFactsBySubject() {
        val body = given()
            .pathParam("storeName", storeName)
            .queryParam("subject", subject)
            .queryParam("limit", 10)
            .queryParam("direction", "forward")
            .`when`()
            .get("/api/v1/stores/{storeName}/facts")
            .then()
            .statusCode(200)
            .contentType(NDJSON)
            .extract().asString()

        val lines = ndjsonLines(body)
        assertThat(lines).hasSize(2)
        assertThat(lines[0]["fact"]["subject"].asText()).isEqualTo(subject)
        assertThat(lines[1]["end"]["count"].asLong()).isEqualTo(1)
    }

    @Test
    @Order(2)
    @DisplayName("GET /v1/stores/{name}/facts?subject - Should return 404 ApiError when the store does not exist")
    fun streamFactsBySubjectOfMissingStore() {
        val error = given()
            .queryParam("subject", subject)
            .queryParam("direction", "forward")
            .`when`()
            .get("/api/v1/stores/missing-store/facts")
            .then()
            .statusCode(404)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.NotFound)
    }

    @Test
    @Order(2)
    @DisplayName("GET /v1/stores/{name}/facts?type - Should stream the type's facts as NDJSON")
    fun streamFactsByType() {
        val body = given()
            .pathParam("storeName", storeName)
            .queryParam("type", "test.type")
            .queryParam("direction", "forward")
            .`when`()
            .get("/api/v1/stores/{storeName}/facts")
            .then()
            .statusCode(200)
            .contentType(NDJSON)
            .extract().asString()

        val lines = ndjsonLines(body)
        assertThat(lines).hasSize(2)
        assertThat(lines[0]["fact"]["type"].asText()).isEqualTo("test.type")
        assertThat(lines[1]["end"]["count"].asLong()).isEqualTo(1)
    }

    @Test
    @Order(2)
    @DisplayName("POST /v1/stores/{name}/facts:query - Should stream the matching facts as NDJSON")
    fun streamFactsByQuery() {
        // RestAssured percent-encodes `:` by default, and `facts%3Aquery` is a different path.
        val body = given()
            .urlEncodingEnabled(false)
            .contentType(JSON)
            .body(
                mapOf(
                    "filters" to listOf(
                        mapOf("subjects" to listOf(subject)),
                        mapOf("types" to listOf("test.type"), "tags" to mapOf("region" to "europe")),
                    ),
                    "direction" to "forward",
                )
            )
            .`when`()
            .post("/api/v1/stores/$storeName/facts:query")
            .then()
            .statusCode(200)
            .contentType(NDJSON)
            .extract().asString()

        val lines = ndjsonLines(body)
        assertThat(lines.dropLast(1)).allMatch { it.has("fact") }
        assertThat(lines[0]["fact"]["subject"].asText()).isEqualTo(subject)
        assertThat(lines.last()["end"]["count"].asLong()).isEqualTo(lines.size - 1L)
    }

    @Test
    @Order(2)
    @DisplayName("POST /v1/stores/{name}/facts:query - Should return 404 ApiError when the store does not exist")
    fun streamFactsByQueryOfMissingStore() {
        val error = given()
            .urlEncodingEnabled(false)
            .contentType(JSON)
            .body(mapOf("filters" to listOf(mapOf("types" to listOf("test.type")))))
            .`when`()
            .post("/api/v1/stores/missing-store/facts:query")
            .then()
            .statusCode(404)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.NotFound)
    }

    @Test
    @DisplayName("GET /v1/stores/{name}/facts - Should continue after a fact, in both directions")
    fun streamFactsContinued() {
        // Its own store: the ordered tests above assert on the contents of theirs.
        val store = "continuation-test-store"
        val first = seedFact("sub-1", store = store)
        val second = seedFact("sub-2", store = store)

        // Continuing after the newest fact leaves nothing to stream.
        val afterNewest = streamFacts(store, mapOf("continueAfter" to second.toString()))
        assertThat(afterNewest).hasSize(1)
        assertThat(afterNewest.single()["end"]["count"].asLong()).isZero()

        // Forward from the first fact: the second one follows it.
        val forward = streamFacts(store, mapOf("continueAfter" to first.toString()))
        assertThat(forward.dropLast(1).map { it["fact"]["id"].asText() }).containsExactly(second.toString())

        // Backward from the second: the first one comes before it.
        val backward = streamFacts(
            store,
            mapOf("continueAfter" to second.toString(), "direction" to "backward"),
        )
        assertThat(backward.dropLast(1).map { it["fact"]["id"].asText() }).containsExactly(first.toString())
    }

    @Test
    @DisplayName("GET /v1/stores/{name}/facts - Should return 404 ApiError when the continuation does not exist")
    fun streamFactsContinuedAfterUnknownFact() {
        val store = "continuation-test-store"
        seedFact("sub-1", store = store)
        val unknown = UUID.randomUUID()

        val error = given()
            .queryParam("continueAfter", unknown.toString())
            .`when`()
            .get("/api/v1/stores/$store/facts")
            .then()
            .statusCode(404)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.NotFound)
        assertThat(error.details).containsEntry("id", unknown.toString())
    }

    @Test
    @Order(4)
    @DisplayName("GET /v1/stores/{name}/facts - Should combine subject, type and tags into one filter")
    fun streamFactsByFilter() {
        val store = "filter-test-store"
        seedFact("sub-1", tags = mapOf("region" to "eu"), store = store)
        seedFact("sub-2", tags = mapOf("region" to "eu"), store = store)

        // Subject AND type AND tag: only the first fact satisfies all three.
        val lines = streamFacts(
            store,
            mapOf("subject" to "sub-1", "type" to "test.type", "tag" to "region=eu"),
        )

        assertThat(lines.dropLast(1)).hasSize(1)
        assertThat(lines[0]["fact"]["subject"].asText()).isEqualTo("sub-1")
    }

    private fun streamFacts(store: String, params: Map<String, String>): List<JsonNode> {
        val request = given()
        params.forEach { (name, value) -> request.queryParam(name, value) }
        return ndjsonLines(
            request.`when`()
                .get("/api/v1/stores/$store/facts")
                .then()
                .statusCode(200)
                .contentType(NDJSON)
                .extract().asString()
        )
    }

    @Test
    @Order(4)
    @DisplayName("GET /v1/stores/{name}/facts - Should stream facts filtered by tags as NDJSON")
    fun streamFactsByTags() {
        // Seeding a specific tagged fact
        seedFact("tagged-sub", mapOf("region" to "europe"))

        val body = given()
            .pathParam("storeName", storeName)
            .queryParam("tag", "region=europe")
            .queryParam("direction", "forward")
            .`when`()
            .get("/api/v1/stores/{storeName}/facts")
            .then()
            .statusCode(200)
            .contentType(NDJSON)
            .extract().asString()

        val lines = ndjsonLines(body)
        assertThat(lines).hasSize(2)
        assertThat(lines[0]["fact"]["tags"]["region"].asText()).isEqualTo("europe")
        assertThat(lines[1]["end"]["count"].asLong()).isEqualTo(1)
    }

    @Test
    @Order(5)
    @DisplayName("GET /v1/stores/{name}/facts - Should stream forward by default when no direction is given")
    fun streamFactsDefaultsToForward() {
        val body = given()
            .pathParam("storeName", storeName)
            .queryParam("limit", 1)
            .`when`()
            .get("/api/v1/stores/{storeName}/facts")
            .then()
            .statusCode(200)
            .contentType(NDJSON)
            .extract().asString()

        // Forward with limit 1: the oldest fact, seeded by the first test.
        val lines = ndjsonLines(body)
        assertThat(lines[0]["fact"]["subject"].asText()).isEqualTo(subject)
        assertThat(lines[1]["end"]["count"].asLong()).isEqualTo(1)
    }

    @Test
    @Order(5)
    @DisplayName("GET /v1/stores/{name}/facts - Should stream all facts of the store as NDJSON")
    fun streamFacts() {
        val body = given()
            .pathParam("storeName", storeName)
            .queryParam("direction", "backward")
            .queryParam("limit", 1)
            .`when`()
            .get("/api/v1/stores/{storeName}/facts")
            .then()
            .statusCode(200)
            .contentType(NDJSON)
            .extract().asString()

        // Backward with limit 1: only the newest fact, the tagged one seeded above.
        val lines = ndjsonLines(body)
        assertThat(lines).hasSize(2)
        assertThat(lines[0]["fact"]["subject"].asText()).isEqualTo("tagged-sub")
        assertThat(lines[1]["end"]["count"].asLong()).isEqualTo(1)
    }

    @Test
    @Order(6)
    @DisplayName("GET /v1/stores/{name}/facts/{factId} - Should return 404 ApiError when fact missing")
    fun findByIdNotFound() {
        val randomId = UUID.randomUUID()
        val error = given()
            .pathParam("storeName", storeName)
            .pathParam("factId", randomId)
            .`when`()
            .get("/api/v1/stores/{storeName}/facts/{factId}")
            .then()
            .statusCode(404)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.NotFound)
        assertThat(error.details).containsEntry("id", randomId.toString())
    }

    private fun ndjsonLines(body: String): List<JsonNode> =
        body.lines().filter { it.isNotBlank() }.map { objectMapper.readTree(it) }

    // Helper to seed data via the already tested Store and Append APIs.
    // Returns the server-assigned fact id.
    private fun seedFact(sub: String, tags: Map<String, String> = emptyMap(), store: String = storeName): UUID {
        // Ensure store exists
        given().contentType(JSON).body(mapOf("name" to store)).post("/api/v1/stores")

        val base64Data = Base64.getEncoder().encodeToString("test-payload".toByteArray())
        val appendRequest = mapOf(
            "facts" to listOf(
                mapOf(
                    "type" to "test.type",
                    "subject" to sub,
                    "payload" to mapOf("data" to base64Data),
                    "tags" to tags
                )
            )
        )
        val appended = given()
            .contentType(JSON)
            .body(appendRequest)
            .post("/api/v1/stores/$store/facts")
            .then().statusCode(200)
            .extract().`as`(AppendedHttp::class.java)
        return appended.factIds.single()
    }

}
