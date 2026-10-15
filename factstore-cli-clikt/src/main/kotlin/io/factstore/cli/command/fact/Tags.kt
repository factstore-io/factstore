package io.factstore.cli.command.fact

import com.github.ajalt.clikt.core.UsageError

/**
 * The tags from repeated `--tag key=value` options, as the client library wants them.
 *
 * A fact must carry all of them, so a key can only be asked for once: `--tag a=1 --tag a=2`
 * would match nothing, and silently returning nothing is worse than saying so.
 */
internal fun List<Pair<String, String>>.toTagFilter(): Map<String, String> = buildMap {
    for ((key, value) in this@toTagFilter) {
        if (key.isBlank()) throw UsageError("a tag needs a key, as in --tag region=eu")
        if (key in this) {
            throw UsageError("tag '$key' is given more than once; to match any of several values use 'fact query'")
        }
        put(key, value)
    }
}
