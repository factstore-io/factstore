package io.factstore.cli.command

import com.github.ajalt.clikt.command.SuspendingCliktCommand
import com.github.ajalt.clikt.command.test
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class TagOptionTest {

    @Test
    fun `tags become the pairs the client library takes`(): Unit = runBlocking {
        val command = command()

        command.test("--tag region=eu --tag env=prod")

        assertThat(command.tags).containsExactly("region" to "eu", "env" to "prod")
    }

    @Test
    fun `a value may be empty, as long as it was asked for`(): Unit = runBlocking {
        val command = command()

        command.test("--tag region=")

        assertThat(command.tags).containsExactly("region" to "")
    }

    @Test
    fun `spaces around the key and the value are trimmed`(): Unit = runBlocking {
        // Kept, they would make 'region ' a key no filter on 'region' matches.
        val command = command()

        command.test(listOf("--tag", " region = eu "))

        assertThat(command.tags).containsExactly("region" to "eu")
    }

    @Test
    fun `a key of only spaces is a usage error`(): Unit = runBlocking {
        val result = command().test(listOf("--tag", "  =eu"))

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("--tag", "key=value")
    }

    @Test
    fun `a tag without an '=' is a usage error`(): Unit = runBlocking {
        // Taking the value as empty instead would quietly ask for tags nothing carries.
        val result = command().test("--tag region")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("--tag", "key=value")
    }

    @Test
    fun `a tag without a key is a usage error`(): Unit = runBlocking {
        val result = command().test("--tag =eu")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("--tag", "key=value")
    }

    @Test
    fun `the same key twice is a usage error, in the caller's own words`(): Unit = runBlocking {
        // All tags must match, so two values for one key match nothing at all.
        val result = command().test("--tag region=eu --tag region=us")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("'region'", "more than once", "do it differently")
    }

    @Test
    fun `there is no -t, which could pass for --type as well`(): Unit = runBlocking {
        val result = command().test("-t region=eu")

        assertThat(result.statusCode).isEqualTo(1)
        assertThat(result.stderr).contains("no such option", "-t")
    }

    private fun command() = object : SuspendingCliktCommand(name = "probe") {
        val tags by tagOption(help = "A tag to match", duplicateHint = "do it differently")

        override suspend fun run() = Unit
    }
}
