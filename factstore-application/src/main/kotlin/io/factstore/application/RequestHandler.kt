package io.factstore.application

import io.factstore.core.IdempotencyKey
import kotlinx.coroutines.delay

/** What travels with a request, next to it rather than in it. */
data class Envelope<out R>(val request: R, val idempotencyKey: IdempotencyKey)

/** Handles one request from start to end: one decision, attempted again while its condition is violated. */
fun interface RequestHandler<in R, out F, out V> {

    suspend fun handle(envelope: Envelope<R>): DecisionResult<F, V>
}

/**
 * A handler that fingerprints the request and turns it into a command once, so every attempt
 * decides about the same command under the same key. Only a violated condition is attempted again,
 * as long as [retryPolicy] allows; the result is that of the last attempt.
 */
fun <R, C, S, F, Q, A, V> RequestHandler(
    slice: Slice<C, S, F, Q, A, V>,
    encoding: RequestEncoding<R>,
    toCommand: (R) -> C,
    retryPolicy: RetryPolicy,
): RequestHandler<R, F, V> = RequestHandler { envelope ->
    val idempotency = Idempotency(envelope.idempotencyKey, encoding.fingerprint(envelope.request))
    slice.handleRetrying(toCommand(envelope.request), idempotency, retryPolicy)
}

private tailrec suspend fun <C, S, F, Q, A, V> Slice<C, S, F, Q, A, V>.handleRetrying(
    command: C,
    idempotency: Idempotency,
    retryPolicy: RetryPolicy,
    failedAttempts: Int = 0,
): DecisionResult<F, V> {
    val result = handle(command, idempotency)
    if (result !is DecisionResult.ConditionViolated) return result
    val wait = retryPolicy.delayAfter(failedAttempts + 1) ?: return result
    delay(wait)
    return handleRetrying(command, idempotency, retryPolicy, failedAttempts + 1)
}
