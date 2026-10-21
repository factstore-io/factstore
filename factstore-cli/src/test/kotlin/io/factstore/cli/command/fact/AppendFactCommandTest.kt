package io.factstore.cli.command.fact

import com.github.ajalt.clikt.command.test
import io.factstore.cli.connection.FactStoreClients
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.io.InputStream

/**
 * `--url` and `--store` are given throughout, so that no config file is read, and each usage
 * error comes before anything is sent to the server.
 */
class AppendFactCommandTest {

    private val clients = FactStoreClients()
    private val stdin = System.`in`

    @AfterEach
    fun restore() {
        System.setIn(stdin)
        clients.close()
    }

    @Test
    fun `without a payload, stdin is not read`(): Unit = runBlocking {
        // Whether stdin is a terminal cannot be told reliably, and reading one waits on the keyboard.
        System.setIn(UnreadableStdin)

        val result = AppendFactCommand(clients).test("$CONNECTION --type ORDER_PLACED --subject order/1")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("missing argument", "payload")
    }

    @Test
    fun `a payload of - is read from stdin`(): Unit = runBlocking {
        // An empty stdin is the one outcome that shows it was read without reaching the server.
        System.setIn(InputStream.nullInputStream())

        val result = AppendFactCommand(clients).test("$CONNECTION --type ORDER_PLACED --subject order/1 -")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("the payload is empty")
    }

    /** Stands in for a keyboard nobody types at: an empty stream would hide that it was read. */
    private object UnreadableStdin : InputStream() {
        override fun read(): Int = throw AssertionError("stdin was read, which would wait on the keyboard")
    }

    private companion object {
        const val CONNECTION = "--url http://localhost:1 --store demo"
    }
}
