package io.factstore.cli.connection

import com.github.ajalt.clikt.command.SuspendingCliktCommand
import com.github.ajalt.clikt.command.test
import com.github.ajalt.clikt.parameters.groups.provideDelegate
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.URI
import java.nio.file.Path
import kotlin.io.path.writeText

class ConnectionOptionsTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `without anything set, the connection is the local default`(): Unit = runBlocking {
        assertThat(resolve().connection.url).isEqualTo(URI.create("http://localhost:8080"))
    }

    @Test
    fun `the context named by the config file is used`(): Unit = runBlocking {
        val contexts = contexts(
            """
            current-context = prod
            context.local.url = http://localhost:8080
            context.prod.url = https://facts.example.com:443
            context.prod.store = orders
            """
        )

        val options = resolve(contexts = contexts)

        assertThat(options.connection.url).isEqualTo(URI.create("https://facts.example.com:443"))
        assertThat(options.defaultStore).isEqualTo("orders")
    }

    @Test
    fun `--context picks another context than the current one`(): Unit = runBlocking {
        val contexts = contexts(
            """
            current-context = prod
            context.local.url = http://localhost:9999
            context.prod.url = https://facts.example.com
            """
        )

        val options = resolve("--context", "local", contexts = contexts)

        assertThat(options.connection.url).isEqualTo(URI.create("http://localhost:9999"))
    }

    @Test
    fun `--url wins over the environment, which wins over the context`(): Unit = runBlocking {
        val contexts = contexts("current-context = prod\ncontext.prod.url = https://from-file.example.com")
        val env = mapOf("FACTSTORE_URL" to "https://from-env.example.com")

        assertThat(resolve(contexts = contexts, env = env).connection.url)
            .isEqualTo(URI.create("https://from-env.example.com"))
        assertThat(resolve("--url", "https://from-flag.example.com", contexts = contexts, env = env).connection.url)
            .isEqualTo(URI.create("https://from-flag.example.com"))
    }

    @Test
    fun `a context the config file does not define is a usage error`(): Unit = runBlocking {
        // Resolving it to nothing would send the command to the built-in default instead -- a
        // mistyped --context prod would quietly talk to localhost.
        val contexts = contexts("current-context = prod\ncontext.prod.url = https://facts.example.com")

        val result = command(contexts).test("--context prodd")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("--context", "prodd")
    }

    @Test
    fun `a url without a host is a usage error`(): Unit = runBlocking {
        val result = command(FactStoreContexts.Empty).test("--url not-a-url")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("--url", "scheme and a host")
    }

    private fun contexts(config: String): FactStoreContexts {
        val file = tempDir.resolve("config")
        file.writeText(config.trimIndent())
        return FactStoreContexts.from(file)
    }

    /** Parses [args] the way the CLI would, and hands back what the options resolved to. */
    private suspend fun resolve(
        vararg args: String,
        contexts: FactStoreContexts = FactStoreContexts.Empty,
        env: Map<String, String> = emptyMap(),
    ): ConnectionOptions {
        val command = command(contexts)
        // Clikt's test helper owns the environment the command sees, so the real one cannot
        // leak in and nothing has to be set on the process.
        command.test(args.toList(), envvars = env)
        return command.connectionOptions
    }

    private fun command(contexts: FactStoreContexts) =
        object : SuspendingCliktCommand(name = "probe") {
            val connectionOptions by ConnectionOptions(contexts)

            override suspend fun run() = Unit
        }
}
