package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.UsageError
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TagsTest {

    @Test
    fun `tags become the map the client library takes`() {
        val tags = listOf("region" to "eu", "env" to "prod").toTagFilter()

        assertThat(tags).containsExactlyInAnyOrderEntriesOf(mapOf("region" to "eu", "env" to "prod"))
    }

    @Test
    fun `the same key twice is a usage error`() {
        // All tags must match, so two values for one key match nothing at all.
        assertThatThrownBy { listOf("region" to "eu", "region" to "us").toTagFilter() }
            .isInstanceOf(UsageError::class.java)
            .hasMessageContaining("more than once")
    }

    @Test
    fun `a tag without a key is a usage error`() {
        assertThatThrownBy { listOf("" to "eu").toTagFilter() }
            .isInstanceOf(UsageError::class.java)
    }
}
