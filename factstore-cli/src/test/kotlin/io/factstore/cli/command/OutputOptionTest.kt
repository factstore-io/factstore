package io.factstore.cli.command

import com.github.ajalt.clikt.command.SuspendingCliktCommand
import com.github.ajalt.clikt.command.test
import io.factstore.cli.output.OutputFormat
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class OutputOptionTest {

    @Test
    fun `a format is matched regardless of case`(): Unit = runBlocking {
        // As --direction and --from are, and as -o was under Picocli.
        val command = command()

        command.test("-o JSON")

        assertThat(command.output).isEqualTo(OutputFormat.Json)
    }

    @Test
    fun `a format the command does not offer is a usage error`(): Unit = runBlocking {
        val result = command(OutputFormat.Table, OutputFormat.Ndjson).test("-o json")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("-o", "table", "ndjson")
    }

    private fun command(vararg formats: OutputFormat = OutputFormat.entries.toTypedArray()) =
        object : SuspendingCliktCommand(name = "probe") {
            val output by outputOption(*formats)

            override suspend fun run() = Unit
        }
}
