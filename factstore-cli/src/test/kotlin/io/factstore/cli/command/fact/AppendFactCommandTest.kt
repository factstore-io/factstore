package io.factstore.cli.command.fact

import com.github.ajalt.clikt.command.test
import io.factstore.cli.connection.FactStoreClients
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.io.InputStream

class AppendFactCommandTest {

    private val clients = FactStoreClients()
    private val stdin = System.`in`

    @AfterEach
    fun restore() {
        System.setIn(stdin)
        clients.close()
    }

    @Test
    fun `without a payload, a terminal on stdin is not read even when stdout is redirected`(): Unit = runBlocking {
        // `fact append ... > out.log`: reading stdin here would wait on the keyboard, with no prompt.
        // --url and --store are given so that no config file is read, and the usage error comes
        // before anything is sent to the server.
        System.setIn(UnreadableStdin)

        val result = AppendFactCommand(clients).test(
            "--url http://localhost:1 --store demo --type ORDER_PLACED --subject order/1",
            inputInteractive = true,
            outputInteractive = false,
        )

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("no payload given")
    }

    /** Stands in for a keyboard nobody types at: an empty stream would hide that it was read. */
    private object UnreadableStdin : InputStream() {
        override fun read(): Int = throw AssertionError("stdin was read, which would wait on the keyboard")
    }
}
