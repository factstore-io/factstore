package io.factstore.cli.converter

import io.factstore.cli.exception.CliUsageException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TagFilterTest {

    @Test
    fun `tags with different keys are all required`() {
        assertThat(listOf("region" to "eu", "tier" to "gold").toTagFilter())
            .isEqualTo(mapOf("region" to "eu", "tier" to "gold"))
    }

    @Test
    fun `a tag key given twice with different values is a usage error`() {
        assertThatThrownBy { listOf("region" to "eu", "region" to "us").toTagFilter() }
            .isInstanceOf(CliUsageException::class.java)
            .hasMessageContaining("Tag 'region' is given more than once")
    }

    @Test
    fun `a tag key given twice with the same value is a usage error`() {
        assertThatThrownBy { listOf("region" to "eu", "region" to "eu").toTagFilter() }
            .isInstanceOf(CliUsageException::class.java)
    }
}
