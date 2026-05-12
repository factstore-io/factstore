package io.factstore.server.http

import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import io.restassured.http.ContentType.JSON
import org.assertj.core.api.Assertions.assertThat
import org.hamcrest.Matchers.notNullValue
import org.junit.jupiter.api.*
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation

@QuarkusTest
@TestMethodOrder(OrderAnnotation::class)
class StoreResourceTest {

    @Test
    @Order(1)
    @DisplayName("POST /v1/stores - Should return 201 when store is created")
    fun createStore() {
        given()
            .contentType(JSON)
            .body(mapOf("name" to "production-logs"))
            .`when`()
            .post("/v1/stores")
            .then()
            .statusCode(201)
            .body("id", notNullValue())
    }

    @Test
    @Order(2)
    @DisplayName("POST /v1/stores - Should return 400 ApiError when request is invalid")
    fun createStoreInvalidRequest() {
        val invalidName = "!!!-bad-name-!!!" // Violates @Pattern regex

        val error = given()
            .contentType(JSON)
            .body(mapOf("name" to invalidName))
            .`when`()
            .post("/v1/stores")
            .then()
            .statusCode(400)
            .extract().`as`(ApiError::class.java)

        assertThat(error).apply {
            extracting(ApiError::status).isEqualTo("Failure")
            extracting(ApiError::reason).isEqualTo(Reason.InvalidInput)
        }
    }

    @Test
    @Order(3)
    @DisplayName("HEAD /v1/stores/{name} - Should return 200 when store exists")
    fun verifyExistence() {
        given()
            .`when`()
            .head("/v1/stores/production-logs")
            .then()
            .statusCode(200)
    }

    @Test
    @Order(4)
    @DisplayName("HEAD /v1/stores/{name} - Should return 404 when name format is invalid")
    fun validationBorder() {
        given()
            .`when`()
            .head("/v1/stores/!!invalid!!")
            .then()
            .statusCode(400)
    }

    @Test
    @Order(5)
    @DisplayName("POST /v1/stores - Should return 409 ApiError when name already exists")
    fun conflictTest() {
        val name = "duplicate-store"

        given()
            .contentType(JSON)
            .body(mapOf("name" to name))
            .post("/v1/stores")
            .then().statusCode(201)

        val error = given()
            .contentType(JSON)
            .body(mapOf("name" to name))
            .`when`()
            .post("/v1/stores")
            .then()
            .statusCode(409)
            .extract().`as`(ApiError::class.java)

        assertThat(error).apply {
            extracting(ApiError::reason).isEqualTo(Reason.AlreadyExists)
            extracting(ApiError::message).isEqualTo("Store '$name' already exists.")
            extracting(ApiError::details).isEqualTo(mapOf("name" to name))
        }
    }

    @Test
    @Order(6)
    @DisplayName("DELETE /v1/stores/{name} - Should return 200 when store is removed")
    fun deleteStore() {
        given()
            .`when`()
            .delete("/v1/stores/production-logs")
            .then()
            .statusCode(200)

        given()
            .`when`()
            .head("/v1/stores/production-logs")
            .then()
            .statusCode(404)
    }

    @Test
    @Order(7)
    @DisplayName("DELETE /v1/stores/{name} - Should return 404 ApiError when missing")
    fun deleteStoreNotFound() {
        val name = "non-existent-store"

        val error = given()
            .`when`()
            .delete("/v1/stores/$name")
            .then()
            .statusCode(404)
            .extract().`as`(ApiError::class.java)

        assertThat(error).apply {
            extracting(ApiError::reason).isEqualTo(Reason.NotFound)
            extracting(ApiError::code).isEqualTo(404)
            extracting(ApiError::details).isEqualTo(mapOf(
                "kind" to "store",
                "name" to name
            ))
        }
    }
}
