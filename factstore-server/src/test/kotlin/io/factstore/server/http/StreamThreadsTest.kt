package io.factstore.server.http

import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

@QuarkusTest
class StreamThreadsTest {

    @Test
    @DisplayName("A fact stream continues on the request's event loop, whichever thread resumes it")
    fun streamStaysOnTheRequestsEventLoop() {
        given().get("/api/test/stream-threads").then().statusCode(200)

        val threads = StreamThreadsTestResource.THREADS.toList()
        assertThat(threads.map { it.first }).containsExactly(
            "resource",
            "start",
            "after a delay",
            "after a future completed elsewhere",
            "after work on another dispatcher",
        )
        val eventLoop = threads.first().second
        assertThat(eventLoop).startsWith("vert.x-eventloop-thread-")
        assertThat(threads).allSatisfy { (_, thread) -> assertThat(thread).isEqualTo(eventLoop) }
    }
}
