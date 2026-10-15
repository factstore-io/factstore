package io.factstore.cli.command.fact

import io.factstore.cli.exception.CliUsageException
import io.factstore.client.model.FactFilter
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.File

class QueryFiltersTest {

    @TempDir
    lateinit var directory: File

    @Test
    fun `a filter is parsed with every property it sets`() {
        val filter = parseFilter("""{"subjects":["order/1"],"types":["OrderPlaced"],"tags":{"token":"a=b"}}""")

        assertThat(filter).isEqualTo(
            FactFilter(subjects = listOf("order/1"), types = listOf("OrderPlaced"), tags = mapOf("token" to "a=b"))
        )
    }

    @Test
    fun `a property a filter leaves out is empty`() {
        assertThat(parseFilter("""{"types":["OrderPlaced"]}""")).isEqualTo(FactFilter(types = listOf("OrderPlaced")))
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """{"subjects":"order/1"}""",
            """{"tags":["region"]}""",
            """{"types":[1]}""",
            """{"types":[null]}""",
            """{"subject":["order/1"],"types":["OrderPlaced"]}""",
            """["OrderPlaced"]""",
            """{"types":[""",
            "",
        ]
    )
    fun `a filter of the wrong shape is a usage error`(filter: String) {
        assertThatThrownBy { parseFilter(filter) }
            .isInstanceOf(CliUsageException::class.java)
            .hasMessageStartingWith("The filter '$filter' is not valid")
    }

    @Test
    fun `a query file yields its filters in order`() {
        val file = queryFile("""{"filters":[{"types":["A"]},{"subjects":["order/1"]}]}""")

        assertThat(readQueryFile(file)).containsExactly(
            FactFilter(types = listOf("A")),
            FactFilter(subjects = listOf("order/1")),
        )
    }

    @Test
    fun `a missing query file is a usage error`() {
        val file = File(directory, "missing.json")

        assertThatThrownBy { readQueryFile(file) }
            .isInstanceOf(CliUsageException::class.java)
            .hasMessage("The query file '$file' does not exist.")
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """{"filters":[""",
            """{}""",
            """{"filters":{"types":["A"]}}""",
            """{"filters":[{"types":"A"}]}""",
            """{"filters":[{"types":["A"]}],"limit":5}""",
            """[{"types":["A"]}]""",
        ]
    )
    fun `a query file of the wrong shape is a usage error`(content: String) {
        val file = queryFile(content)

        assertThatThrownBy { readQueryFile(file) }
            .isInstanceOf(CliUsageException::class.java)
            .hasMessageStartingWith("The query file '$file' is not valid")
    }

    private fun queryFile(content: String): File = File(directory, "query.json").apply { writeText(content) }
}
