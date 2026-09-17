package io.factstore.server.grpc

import io.factstore.server.input.InvalidInputException
import io.factstore.server.input.parseInput
import io.grpc.Status

/**
 * Runs [parse] and reports a failure to interpret client input as `INVALID_ARGUMENT`.
 *
 * Coroutine services bypass Quarkus's default exception handling, so without this an invalid
 * request would reach the client as `UNKNOWN`, without a description.
 */
internal inline fun <T> parseRequest(parse: () -> T): T =
    try {
        parseInput(parse)
    } catch (e: InvalidInputException) {
        throw Status.INVALID_ARGUMENT.withDescription(e.message).withCause(e).asException()
    }
