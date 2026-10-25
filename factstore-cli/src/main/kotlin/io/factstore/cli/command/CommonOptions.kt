package io.factstore.cli.command

import com.github.ajalt.clikt.core.ParameterHolder
import com.github.ajalt.clikt.parameters.options.check
import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.multiple
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.validate
import com.github.ajalt.clikt.parameters.transform.TransformContext
import com.github.ajalt.clikt.parameters.types.choice
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
 * Fails unless [value] is a UUID, [what] naming what it was meant to be.
 *
 * Ids travel as strings, the way the client library takes them; this only rejects what cannot be
 * one, so a typo is a usage error rather than a round trip to the server.
 */
fun TransformContext.uuid(what: String, value: String): String {
    if (runCatching { UUID.fromString(value) }.isFailure) {
        fail("expected $what (a UUID), but was '$value'")
    }
    return value
}

/** Fails unless [value] is a fact id. */
fun TransformContext.factId(value: String): String = uuid("a fact id", value)

/** `-o`: the shape the output is written in, out of the [formats] the command offers. */
fun ParameterHolder.outputOption(vararg formats: OutputFormat = OutputFormat.entries.toTypedArray()) = option(
    "--output", "-o",
    help = "How to write the output",
).choice(formats.associateBy { it.cliName }, ignoreCase = true).default(OutputFormat.Table)

/**
 * Fails unless [value] is a tag, which is a key and a value with an `=` between them.
 *
 * Without the `=` there is nothing to take as the value, and taking the empty string instead
 * would turn `--tag region` into a filter nothing matches, or into a fact carrying a tag whose
 * value is missing.
 *
 * Both sides are trimmed: `--tag 'region = eu'` means `region=eu`, and kept as given it would
 * write a key no filter on `region` matches, or ask for one no fact carries.
 */
fun TransformContext.tag(value: String): Pair<String, String> {
    val key = value.substringBefore('=').trim()
    if ('=' !in value || key.isEmpty()) {
        fail("expected a tag as key=value, but was '$value'")
    }
    return key to value.substringAfter('=').trim()
}

/**
 * `--tag`: a tag, repeatable.
 *
 * A fact carries one value per key, so a key can only be given once. What to do instead differs
 * between asking for tags and writing them, which is what [duplicateHint] says; both are checked
 * while the command line is parsed, so the error names the option that caused it.
 *
 * There is no short form: `-t` meant `--type` for `fact append` under Picocli, and could pass
 * for either. Spelling both out leaves nothing to mistake.
 */
fun ParameterHolder.tagOption(help: String, duplicateHint: String) = option(
    "--tag",
    metavar = "<key=value>",
    help = help,
).convert { tag(it) }.multiple().validate { tags ->
    val duplicate = tags.groupBy { (key, _) -> key }.entries.firstOrNull { (_, given) -> given.size > 1 }
    if (duplicate != null) {
        fail("tag '${duplicate.key}' is given more than once; $duplicateHint")
    }
}

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
).enum<ReadDirection>(key = { it.name.lowercase() }).default(ReadDirection.Forward)

/** `--limit`: how many facts at most; absent means all of them. */
fun ParameterHolder.limitOption() = option(
    "--limit",
    metavar = "<n>",
    help = "Stop after this many facts (default: all of them)",
).int().check("--limit must be positive") { it > 0 }
