package io.factstore.client.internal

import io.factstore.client.exceptions.FactStoreRpcException
import io.factstore.client.exceptions.FactStoreTimeoutException
import io.factstore.client.exceptions.FactStoreUnavailableException
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException

/**
 * Translates a gRPC transport failure into the corresponding [io.factstore.client.exceptions.FactStoreException].
 * Anything that is not a gRPC status carrier (including our own domain exceptions) is returned unchanged.
 */
internal fun Throwable.toFactStoreException(): Throwable {
    val status = when (this) {
        is StatusException -> status
        is StatusRuntimeException -> status
        else -> return this
    }
    return when (status.code) {
        Status.Code.UNAVAILABLE -> FactStoreUnavailableException(this)
        Status.Code.DEADLINE_EXCEEDED -> FactStoreTimeoutException(this)
        else -> FactStoreRpcException(status.code.name, status.description, this)
    }
}

/**
 * Runs a unary gRPC call, mapping any gRPC status failure to a [io.factstore.client.exceptions.FactStoreException].
 * Domain exceptions thrown while interpreting a successful response pass through untouched.
 */
internal suspend inline fun <T> grpcCall(block: () -> T): T =
    try {
        block()
    } catch (e: StatusException) {
        throw e.toFactStoreException()
    } catch (e: StatusRuntimeException) {
        throw e.toFactStoreException()
    }
