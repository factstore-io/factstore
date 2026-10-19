package io.factstore.cli.output

import com.github.ajalt.mordant.rendering.AnsiLevel
import com.github.ajalt.mordant.terminal.Terminal
import com.github.ajalt.mordant.terminal.TerminalRecorder
import io.factstore.client.model.Fact
import io.factstore.client.model.FactPayload
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant

class FactOutputTest {

    private val recorder = TerminalRecorder(ansiLevel = AnsiLevel.NONE)
    private val terminal = Terminal(terminalInterface = recorder, width = 200)

    // A bounded read writes to a plain sink rather than the terminal, so the tests hand one in;
    // the terminal is only asked for the width to size the table's columns to.
    private val out = StringBuilder()

    private val fact = fact(type = "OrderPlaced", payload = """{"total":5}""")

    @Test
    fun `a table names every column and holds a row per fact`(): Unit = runBlocking {
        flowOf(fact, fact(type = "OrderPaid")).writeTo(terminal, OutputFormat.Table, out)

        val output = out.toString()
        assertThat(output.lines().first { "ID" in it })
            .contains("ID", "TYPE", "SUBJECT", "APPENDED AT", "PAYLOAD")
        assertThat(output).contains("OrderPlaced", "OrderPaid", """{"total":5}""")
    }

    @Test
    fun `an empty read still prints its table header`(): Unit = runBlocking {
        flowOf<Fact>().writeTo(terminal, OutputFormat.Table, out)

        assertThat(out.toString()).contains("ID", "TYPE")
    }

    @Test
    fun `a table is written as the facts arrive, not once they are all in`(): Unit = runBlocking {
        // Sizing the columns to the facts would mean holding a whole history before the first
        // row, which is how a large store runs the CLI out of memory.
        val rowsBeforeTheLastFact = StringBuilder()
        val facts = flow {
            emit(fact)
            rowsBeforeTheLastFact.append(out)
            emit(fact(type = "OrderPaid"))
        }

        facts.writeTo(terminal, OutputFormat.Table, out)

        assertThat(rowsBeforeTheLastFact).contains("OrderPlaced")
        assertThat(rowsBeforeTheLastFact).doesNotContain("OrderPaid")
    }

    @Test
    fun `columns are sized to the terminal, and an id is never cut`(): Unit = runBlocking {
        flowOf(fact).writeTo(Terminal(terminalInterface = recorder, width = 100), OutputFormat.Table, out)

        val row = out.toString().lines().first { "OrderPlaced" in it }
        assertThat(row).startsWith(fact.id)
        assertThat(row.length).isLessThanOrEqualTo(100)
    }

    @Test
    fun `json is one array, ndjson one object per line`(): Unit = runBlocking {
        flowOf(fact, fact).writeTo(terminal, OutputFormat.Json, out)
        val json = out.toString()
        assertThat(json.trim()).startsWith("[").endsWith("]")
        assertThat(json).contains("\"type\": \"OrderPlaced\"")

        out.clear()
        flowOf(fact, fact).writeTo(terminal, OutputFormat.Ndjson, out)
        val lines = out.toString().trim().lines()
        assertThat(lines).hasSize(2)
        assertThat(lines.first()).startsWith("{").contains("\"type\":\"OrderPlaced\"")
    }

    @Test
    fun `a payload longer than the preview is cut, and newlines do not break the row`(): Unit = runBlocking {
        val long = fact(payload = "{\n  \"note\": \"${"x".repeat(80)}\"\n}")

        flowOf(long).writeTo(terminal, OutputFormat.Table, out)

        val row = out.toString().lines().first { "OrderPlaced" in it }
        assertThat(row).contains("…")
        assertThat(out.toString().lines().count { "OrderPlaced" in it }).isEqualTo(1)
    }

    @Test
    fun `a followed stream writes a line per fact, never a table`(): Unit = runBlocking {
        flowOf(fact, fact(type = "OrderPaid")).followTo(terminal, OutputFormat.Table, out)

        val lines = recorder.output().trim().lines()
        assertThat(lines).hasSize(2)
        assertThat(lines.first()).contains("OrderPlaced", """{"total":5}""")
        assertThat(recorder.output()).doesNotContain("TYPE")
    }

    private fun fact(
        type: String = "OrderPlaced",
        payload: String = """{"total":5}""",
    ) = Fact(
        id = "41d763ea-d4b9-4756-8681-90164dae22dc",
        type = type,
        subject = "order/1",
        appendedAt = Instant.parse("2026-01-02T03:04:05Z"),
        payload = FactPayload(payload.toByteArray()),
        metadata = emptyMap(),
        tags = emptyMap(),
    )
}
