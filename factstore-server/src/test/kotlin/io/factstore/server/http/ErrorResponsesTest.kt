package io.factstore.server.http

import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import io.restassured.http.ContentType.JSON
import io.restassured.http.ContentType.TEXT
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@QuarkusTest
class ErrorResponsesTest {

    @Test
    @DisplayName("Any endpoint - Should return 500 InternalError without internal details when it fails unexpectedly")
    fun unexpectedFailure() {
        val error = given()
            .get("/api/test/failures/illegal-state")
            .then()
            .statusCode(500)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.InternalError)
        assertThat(error.message).doesNotContain(FailingTestResource.SECRET)
        assertThat(error.details).containsKey("errorId")
    }

    @Test
    @DisplayName("Any endpoint - Should return 500 when an IllegalArgumentException comes from outside parsing")
    fun illegalArgumentOutsideParsing() {
        val error = given()
            .get("/api/test/failures/illegal-argument")
            .then()
            .statusCode(500)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.InternalError)
        assertThat(error.message).doesNotContain(FailingTestResource.SECRET)
    }

    @Test
    @DisplayName("POST /v1/stores - Should keep the framework's 415 and add an ApiError body")
    fun frameworkStatus() {
        val error = given()
            .contentType(TEXT)
            .body("hello")
            .post("/api/v1/stores")
            .then()
            .statusCode(415)
            .contentType(JSON)
            .extract().`as`(ApiError::class.java)

        assertThat(error.reason).isEqualTo(Reason.InvalidInput)
        assertThat(error.code).isEqualTo(415)
    }

    @Test
    @DisplayName("POST /v1/stores/{name}/facts - Should name the offending field when the body is malformed")
    fun malformedBody() {
        val error = given()
            .contentType(JSON)
            .body("""{"facts":[{"type":"T","subject":"s","payload":{"data":"!!!"}}]}""")
            .post("/api/v1/stores/unknown-store/facts")
            .then()
            .statusCode(400)
            .extract().`as`(ApiError::class.java)

        assertThat(error.message).contains("facts[0].payload.data").doesNotContain("io.factstore")
    }
}
