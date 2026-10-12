package io.factstore.cli.command

import io.factstore.cli.exception.CliUsageException
import io.factstore.client.model.Fact
import io.factstore.client.model.FactPayload
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant

class FactOutputTest {

    @Test
    fun `a row lines up with the header`() {
        val row = fact().toTableRow()
        val header = tableHeader().lines().first()

        assertThat(row.indexOf("OrderPlaced")).isEqualTo(header.indexOf("TYPE"))
        assertThat(row.indexOf("order/1")).isEqualTo(header.indexOf("SUBJECT"))
        assertThat(row.indexOf("2026-01-02T03:04:05Z")).isEqualTo(header.indexOf("APPENDED AT"))
        assertThat(row.indexOf("{\"total\":5}")).isEqualTo(header.indexOf("PAYLOAD"))
    }

    @Test
    fun `a value wider than its column is cut, and the columns after it stay in place`() {
        val long = fact(type = "t".repeat(50), subject = "s".repeat(50), payload = "p".repeat(50))
        val short = fact()

        val row = long.toTableRow()

        assertThat(row).contains("t".repeat(29) + "…  ").doesNotContain("t".repeat(30))
        assertThat(row).contains("s".repeat(29) + "…  ").doesNotContain("s".repeat(30))
        assertThat(row).endsWith("p".repeat(39) + "…")
        assertThat(row.indexOf("2026-01-02")).isEqualTo(short.toTableRow().indexOf("2026-01-02"))
    }

    @Test
    fun `a payload of several lines stays on one row`() {
        assertThat(fact(payload = "{\n\"total\":5\n}").toTableRow().lines()).hasSize(1)
    }

    @Test
    fun `an absent or positive limit is accepted`() {
        assertThatCode { requirePositiveLimit(null) }.doesNotThrowAnyException()
        assertThatCode { requirePositiveLimit(1) }.doesNotThrowAnyException()
    }

    @ParameterizedTest
    @ValueSource(ints = [0, -1])
    fun `a limit that is not positive is a usage error`(limit: Int) {
        assertThatThrownBy { requirePositiveLimit(limit) }
            .isInstanceOf(CliUsageException::class.java)
            .hasMessage("--limit must be positive, but was $limit.")
    }

    private fun fact(type: String = "OrderPlaced", subject: String = "order/1", payload: String = "{\"total\":5}") =
        Fact(
            id = "2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11",
            type = type,
            subject = subject,
            appendedAt = Instant.parse("2026-01-02T03:04:05.678Z"),
            payload = FactPayload(payload.toByteArray()),
            metadata = emptyMap(),
            tags = emptyMap(),
        )
}
