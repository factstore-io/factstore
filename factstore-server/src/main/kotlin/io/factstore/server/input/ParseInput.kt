package io.factstore.server.input

/**
 * Client input could not be turned into a valid request.
 *
 * Transports report it as a client error.
 */
class InvalidInputException(
    override val message: String,
    cause: Throwable,
) : RuntimeException(message, cause)

/**
 * Runs [parse] and reports a failure to interpret client input as an [InvalidInputException].
 *
 * Only parsing belongs inside: a failure while executing a request is not the client's fault.
 */
internal inline fun <T> parseInput(parse: () -> T): T =
    try {
        parse()
    } catch (e: IllegalArgumentException) {
        throw InvalidInputException(e.message ?: "The request is invalid.", e)
    }
