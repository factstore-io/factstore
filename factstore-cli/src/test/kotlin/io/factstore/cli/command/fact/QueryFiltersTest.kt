package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.UsageError
import io.factstore.client.model.FactFilter
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

class QueryFiltersTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `a filter is read from JSON`() {
        val filter = """{"types":["OrderPlaced"],"tags":{"region":"eu"}}""".toFactFilter()

        assertThat(filter).isEqualTo(FactFilter(types = listOf("OrderPlaced"), tags = mapOf("region" to "eu")))
    }

    @Test
    fun `a misspelled property is rejected rather than ignored`() {
        // Dropping it silently would widen the query: the reader would get more than they asked for.
        assertThatThrownBy { """{"typo":["OrderPlaced"]}""".toFactFilter() }
            .isInstanceOf(UsageError::class.java)
    }

    @Test
    fun `a query file holds every filter`() {
        val file = tempDir.resolve("query.json").toFile()
        file.writeText("""{"filters":[{"subjects":["order/1"]},{"tags":{"sku":"ESP-1000"}}]}""")

        assertThat(file.toFactFilters()).containsExactly(
            FactFilter(subjects = listOf("order/1")),
            FactFilter(tags = mapOf("sku" to "ESP-1000")),
        )
    }

    @Test
    fun `a missing query file is a usage error`() {
        assertThatThrownBy { File(tempDir.toFile(), "absent.json").toFactFilters() }
            .isInstanceOf(UsageError::class.java)
            .hasMessageContaining("does not exist")
    }
}
