package io.factstore.server.http

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured
import io.restassured.RestAssured.given
import io.restassured.http.ContentType.JSON
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.Base64
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

/**
 * A subscription never ends, so it cannot be read with RestAssured: every test here reads the
 * NDJSON body until it has the fact lines it expects, then closes the connection.
 */
@QuarkusTest
class SubscriptionResourceTest {

    private val objectMapper = ObjectMapper()
    private val client: HttpClient = HttpClient.newHttpClient()

    @Test
    @DisplayName("GET /facts:subscribe - Should drain the facts the store already holds, in order")
    fun subscribeFromBeginning() {
        val store = store("subscribe-beginning")
        val first = seedFact(store, type = "order.placed")
        val second = seedFact(store, type = "order.shipped")

        val facts = subscribe(store, "from=beginning", factLines = 2)

        assertThat(facts.map { it["id"].asText() }).containsExactly(first.toString(), second.toString())
    }


    @Test
    @DisplayName("GET /facts:subscribe - Should emit the facts appended while it runs")
    fun subscribeFollowsTheTail() {
        val store = store("subscribe-tail")
        seedFact(store, type = "order.placed")

        lateinit var appended: UUID
        val facts = subscribe(store, "from=end", factLines = 1) {
            appended = seedFact(store, type = "order.shipped")
        }

        assertThat(facts.single()["id"].asText()).isEqualTo(appended.toString())
    }

    @Test
    @DisplayName("GET /facts:subscribe - Should continue after the fact it was given")
    fun subscribeContinuesAfterAFact() {
        val store = store("subscribe-continue")
        val first = seedFact(store, type = "order.placed")
        val second = seedFact(store, type = "order.shipped")

        val facts = subscribe(store, "continueAfter=$first", factLines = 1)

        assertThat(facts.single()["id"].asText()).isEqualTo(second.toString())
    }

    @Test
    @DisplayName("GET /facts:subscribe - Should answer 404 before the stream starts when the store is missing")
    fun subscribeToMissingStore() {
        val error = given()
            .`when`().urlEncodingEnabled(false)
            .get("/api/v1/stores/no-such-store/facts:subscribe")
            .then()
            .statusCode(404)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.NotFound)
    }

    @Test
    @DisplayName("GET /facts:subscribe - Should answer 404 when the fact to continue after is unknown")
    fun subscribeWithUnknownContinuation() {
        val store = store("subscribe-unknown-continuation")
        seedFact(store, type = "order.placed")

        val error = given()
            .`when`().urlEncodingEnabled(false)
            .queryParam("continueAfter", UUID.randomUUID().toString())
            .get("/api/v1/stores/$store/facts:subscribe")
            .then()
            .statusCode(404)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.details?.get("kind")).isEqualTo("continuation")
    }

    /**
     * Opens a subscription, runs [whileOpen] once the response has started, and returns the
     * first [factLines] facts it writes.
     */
    private fun subscribe(
        store: String,
        query: String,
        factLines: Int,
        whileOpen: () -> Unit = {},
    ): List<JsonNode> {
        val request = HttpRequest.newBuilder(
            URI.create("http://localhost:${RestAssured.port}/api/v1/stores/$store/facts:subscribe?$query")
        ).header("Accept", "application/x-ndjson").build()

        val response = client.send(request, HttpResponse.BodyHandlers.ofInputStream())
        assertThat(response.statusCode()).isEqualTo(200)
        assertThat(response.headers().firstValue("content-type").orElse(""))
            .startsWith("application/x-ndjson")

        return response.body().use { body ->
            val reader = body.bufferedReader()
            val facts = CompletableFuture.supplyAsync {
                val collected = mutableListOf<JsonNode>()
                while (collected.size < factLines) {
                    val line = reader.readLine() ?: break
                    if (line.isBlank()) continue
                    val parsed = objectMapper.readTree(line)
                    when {
                        parsed.has("fact") -> collected.add(parsed.get("fact"))
                        // A heartbeat only keeps an idle subscription alive.
                        parsed.has("heartbeat") -> Unit
                        else -> error("Unexpected subscription line: $line")
                    }
                }
                collected.toList()
            }
            whileOpen()
            facts.get(20, TimeUnit.SECONDS)
        }
    }

    private fun store(name: String): String {
        given().contentType(JSON).body(mapOf("name" to name)).post("/api/v1/stores")
        return name
    }

    private fun seedFact(store: String, type: String, tags: Map<String, String> = emptyMap()): UUID {
        val payload = Base64.getEncoder().encodeToString("""{"test":true}""".toByteArray())
        val appended = given()
            .contentType(JSON)
            .body(
                mapOf(
                    "facts" to listOf(
                        mapOf(
                            "type" to type,
                            "subject" to "order/1",
                            "payload" to mapOf("data" to payload),
                            "tags" to tags,
                        )
                    )
                )
            )
            .post("/api/v1/stores/$store/facts")
            .then().statusCode(200)
            .extract().`as`(AppendedHttp::class.java)
        return appended.factIds.single()
    }
}
