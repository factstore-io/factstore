package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.UsageError
import io.factstore.client.model.FactQueryItem
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

class QueryJsonTest {

    @TempDir
    lateinit var tempDir: Path

    @Test
    fun `a query item is read from JSON`() {
        val item = """{"types":["OrderPlaced"],"tags":{"region":"eu"}}""".toFactQueryItem()

        assertThat(item).isEqualTo(FactQueryItem(types = listOf("OrderPlaced"), tags = mapOf("region" to "eu")))
    }

    @Test
    fun `a misspelled property is rejected rather than ignored`() {
        // Dropping it silently would widen the query: the reader would get more than they asked for.
        assertThatThrownBy { """{"typo":["OrderPlaced"]}""".toFactQueryItem() }
            .isInstanceOf(UsageError::class.java)
    }

    @Test
    fun `a query file holds every item`() {
        val file = tempDir.resolve("query.json").toFile()
        file.writeText("""{"items":[{"subjects":["order/1"]},{"tags":{"sku":"ESP-1000"}}]}""")

        assertThat(file.toFactQueryItems()).containsExactly(
            FactQueryItem(subjects = listOf("order/1")),
            FactQueryItem(tags = mapOf("sku" to "ESP-1000")),
        )
    }

    @Test
    fun `a missing query file is a usage error`() {
        assertThatThrownBy { File(tempDir.toFile(), "absent.json").toFactQueryItems() }
            .isInstanceOf(UsageError::class.java)
            .hasMessageContaining("does not exist")
    }
}
