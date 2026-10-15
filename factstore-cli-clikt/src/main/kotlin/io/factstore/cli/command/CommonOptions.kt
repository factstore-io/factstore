package io.factstore.cli.command

import com.github.ajalt.clikt.core.ParameterHolder
import com.github.ajalt.clikt.parameters.transform.TransformContext
import com.github.ajalt.clikt.parameters.options.check
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.splitPair
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.types.enum
import com.github.ajalt.clikt.parameters.types.int
import io.factstore.cli.output.OutputFormat
import io.factstore.client.model.ReadDirection
import java.util.UUID

/*
 * The options several commands share, each declared once.
 *
 * Clikt builds options through extension functions on ParameterHolder, so sharing one is sharing
 * a function: `private val output by outputOption()` reads like the option it declares, and the
 * help text cannot drift between commands.
 */

/**
 * Fails unless [value] is a fact id.
 *
 * Ids travel as strings, the way the client library takes them; this only rejects what cannot be
 * one, so a typo is a usage error rather than a round trip to the server.
 */
fun TransformContext.factId(value: String): String {
    if (runCatching { UUID.fromString(value) }.isFailure) {
        fail("expected a fact id (a UUID), but was '$value'")
    }
    return value
}

/** `-o`: the shape facts are written in. */
fun ParameterHolder.outputOption() = option(
    "--output", "-o",
    help = "How to write the facts: ${OutputFormat.choices}",
).enum<OutputFormat>(key = { it.cliName }).default(OutputFormat.Table)

/** `--tag`: a tag a fact must carry, repeatable. */
fun ParameterHolder.tagOption(help: String) = option(
    "--tag", "-t",
    metavar = "<key=value>",
    help = help,
).splitPair().multiple()

/** `--continue-after`: where a read picks up, exclusive and in reading order. */
fun ParameterHolder.continueAfterOption() = option(
    "--continue-after",
    metavar = "<factId>",
    help = "Continue after this fact, exclusive and in reading order",
).convert { factId(it) }

/** `--direction`: which way a read runs through the store. */
fun ParameterHolder.directionOption() = option(
    "--direction",
    help = "Read oldest first (forward) or newest first (backward)",
).enum<ReadDirection>(key = { it.name.lowercase() }).default(ReadDirection.FORWARD)

/** `--limit`: how many facts at most; absent means all of them. */
fun ParameterHolder.limitOption() = option(
    "--limit",
    metavar = "<n>",
    help = "Stop after this many facts (default: all of them)",
).int().check("--limit must be positive") { it > 0 }
