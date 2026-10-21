package io.factstore.cli.command

import com.github.ajalt.clikt.core.BaseCliktCommand
import com.github.ajalt.clikt.core.UsageError

/**
 * A usage error raised while a command runs, reported against that command.
 *
 * The context carried by a usage error is what decides whose usage line is printed with it.
 * Clikt attaches its own while it parses the command line, but an error raised later, from
 * `run()`, has none -- and would be reported against the root command, telling someone who
 * mistyped `factstore fact query` how to use `factstore`.
 */
internal fun BaseCliktCommand<*>.usageError(message: String): UsageError =
    UsageError(message).also { it.context = currentContext }
