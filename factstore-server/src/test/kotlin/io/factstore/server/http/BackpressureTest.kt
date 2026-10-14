package io.factstore.server.http

import io.quarkus.test.common.http.TestHTTPResource
import io.quarkus.test.junit.QuarkusTest
import io.restassured.RestAssured.given
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.net.Socket
import java.net.URL

/**
 * A fact stream is read only as fast as its client reads it, so a slow client cannot make the
 * server hold a whole store in memory.
 */
@QuarkusTest
class BackpressureTest {

    @TestHTTPResource("/")
    lateinit var base: URL

    @Test
    @DisplayName("A fact stream whose client stops reading stops being read, and ends when the client leaves")
    fun streamKeepsBackpressure() {
        // About 1.3 MB of NDJSON per thousand facts: far more than the socket buffers hold.
        val count = 100_000
        Socket(base.host, base.port).use { socket ->
            socket.receiveBufferSize = 64 * 1024
            socket.getOutputStream().apply {
                write("GET /api/test/backpressure/facts?count=$count HTTP/1.1\r\nHost: ${base.host}\r\n\r\n".toByteArray())
                flush()
            }
            val input = socket.getInputStream()
            val buffer = ByteArray(16 * 1024)
            var read = 0
            while (read < 64 * 1024) read += input.read(buffer).also { check(it >= 0) { "The stream ended early." } }

            // The client stops reading. Without backpressure, the server would go on to produce every fact.
            Thread.sleep(2_000)
            val whileStalled = produced()
            assertThat(whileStalled).isLessThan(count / 10)
        }

        // The client has left: the stream is not read any further.
        Thread.sleep(1_000)
        val afterLeaving = produced()
        Thread.sleep(1_000)
        assertThat(produced()).isEqualTo(afterLeaving).isLessThan(count / 10)
    }

    private fun produced(): Int = given().get("/api/test/backpressure/produced").asString().toInt()
}
