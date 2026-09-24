package io.factstore.server.http

import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import io.restassured.http.ContentType.JSON
import io.restassured.response.Response
import io.restassured.specification.RequestSpecification
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource

/**
 * Every endpoint reports input it cannot parse as `400 InvalidInput`, before it looks up the
 * store — so the store named here does not have to exist.
 */
@QuarkusTest
class InvalidInputTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRequests")
    @DisplayName("Every endpoint - Should return 400 InvalidInput when the input cannot be parsed")
    fun invalidInputIsRejected(description: String, request: () -> Response) {
        val error = request()
            .then()
            .statusCode(400)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.InvalidInput)
        assertThat(error.message).isNotBlank()
    }

    companion object {

        private const val STORES = "/api/v1/stores"
        private const val FACTS = "$STORES/unknown-store/facts"

        private val validFact = mapOf("type" to "T", "subject" to "s", "payload" to mapOf("data" to "e30="))

        private fun case(description: String, request: () -> Response): Arguments = arguments(description, request)

        private fun json(body: Any): RequestSpecification = given().contentType(JSON).body(body)

        /**
         * A request whose path keeps its colon: RestAssured percent-encodes `:` by default, and
         * `facts%3Aquery` is a different path than the `facts:query` operation.
         */
        private fun jsonToOperation(body: Any): RequestSpecification =
            given().urlEncodingEnabled(false).contentType(JSON).body(body)

        private fun forward(): RequestSpecification = given().queryParam("direction", "forward")

        @JvmStatic
        fun invalidRequests(): List<Arguments> = listOf(
            case("create a store with an invalid name") { json(mapOf("name" to "1 bad")).post(STORES) },
            case("find a store with an invalid name") { given().get("$STORES/1-bad!") },
            case("remove a store with an invalid name") { given().delete("$STORES/1-bad!") },

            case("append a fact with an invalid subject") {
                json(mapOf("facts" to listOf(validFact + ("subject" to "order 1")))).post(FACTS)
            },
            case("append a fact with six tags") {
                json(mapOf("facts" to listOf(validFact + ("tags" to (1..6).associate { "k$it" to "v" })))).post(FACTS)
            },
            case("append without facts") { json(mapOf("facts" to emptyList<Any>())).post(FACTS) },
            case("append malformed JSON") { json("""{"facts": [""").post(FACTS) },
            case("append a fact without a payload") {
                json(mapOf("facts" to listOf(mapOf("type" to "T", "subject" to "s")))).post(FACTS)
            },
            case("append with an idempotency key that is not a UUID") {
                json(mapOf("idempotencyKey" to "nope", "facts" to listOf(validFact))).post(FACTS)
            },

            case("find a fact by an id that is not a UUID") { given().get("$FACTS/not-a-uuid") },
            case("stream the facts of an invalid subject") {
                forward().get("$STORES/unknown-store/subjects/order 1/facts")
            },
            case("query facts without filters") {
                jsonToOperation(mapOf("filters" to emptyList<Any>())).post("$FACTS:query")
            },
            case("query facts with a filter without predicates") {
                jsonToOperation(mapOf("filters" to listOf(emptyMap<String, Any>()))).post("$FACTS:query")
            },
            case("query facts with an invalid type") {
                jsonToOperation(mapOf("filters" to listOf(mapOf("types" to listOf("order created"))))).post("$FACTS:query")
            },
            case("query facts in an unknown direction") {
                jsonToOperation(mapOf("filters" to listOf(mapOf("types" to listOf("T"))), "direction" to "sideways"))
                    .post("$FACTS:query")
            },
            case("query facts with limit 0") {
                jsonToOperation(mapOf("filters" to listOf(mapOf("types" to listOf("T"))), "limit" to 0)).post("$FACTS:query")
            },
            case("stream the facts of an invalid type") {
                forward().get("$STORES/unknown-store/types/order created/facts")
            },
            case("stream facts with limit 0") { forward().queryParam("limit", 0).get(FACTS) },
            case("stream facts continued after an id that is not a UUID") {
                forward().queryParam("continueAfter", "not-a-uuid").get(FACTS)
            },
            case("query facts continued after an id that is not a UUID") {
                jsonToOperation(
                    mapOf(
                        "filters" to listOf(mapOf("types" to listOf("T"))),
                        "continueAfter" to "not-a-uuid",
                    )
                ).post("$FACTS:query")
            },
            case("stream facts in an unknown direction") { given().queryParam("direction", "sideways").get(FACTS) },
            case("stream facts by a tag without '='") { forward().queryParam("tag", "abc").get(FACTS) },

            case("subscribe after an id that is not a UUID") { given().queryParam("after", "nope").get("$FACTS/subscribe") },
            case("subscribe from an unknown position") { given().queryParam("from", "middle").get("$FACTS/subscribe") },
            case("replay after an id that is not a UUID") { given().queryParam("after", "nope").get("$FACTS/replay") },
        )
    }
}
