package io.factstore.cli.output

import com.github.ajalt.mordant.terminal.Terminal
import io.factstore.client.model.StoreInfo
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

class StoreOutputTest {

    // A terminal of its own, so the assertions do not depend on the width or colours of
    // whatever runs the tests.
    private val terminal = Terminal(width = 100, ansiLevel = com.github.ajalt.mordant.rendering.AnsiLevel.NONE)

    @Test
    fun `renders a row per store, with the creation time in the given zone`() {
        val stores = listOf(
            store(name = "orders", createdAt = Instant.parse("2026-01-02T03:04:05Z")),
            store(name = "payments", createdAt = Instant.parse("2026-03-04T05:06:07Z")),
        )

        val rendered = terminal.render(stores.toStoreTable(ZoneOffset.UTC))

        assertThat(rendered.lines().filter { it.contains("20") }).hasSize(2)
        assertThat(rendered).contains("orders", "2026-01-02 03:04:05")
        assertThat(rendered).contains("payments", "2026-03-04 05:06:07")
    }

    @Test
    fun `names every column in the header`() {
        val rendered = terminal.render(listOf(store()).toStoreTable(ZoneOffset.UTC))

        assertThat(rendered.lines().first { it.contains("NAME") })
            .contains("NAME", "CREATED AT", "STORE ID")
    }

    private fun store(
        name: String = "orders",
        createdAt: Instant = Instant.parse("2026-01-02T03:04:05Z"),
    ) = StoreInfo(id = "6b1f0e9e-7b7a-4a1e-9d4e-2f3a5c6d7e8f", name = name, createdAt = createdAt)
}
