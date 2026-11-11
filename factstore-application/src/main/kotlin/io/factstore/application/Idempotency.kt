package io.factstore.application

import io.factstore.core.Fingerprint
import io.factstore.core.IdempotencyKey

/**
 * The idempotency key of a request, with the request's fingerprint.
 *
 * A decision recorded under a key uses it up: a retry under the same key is answered with the facts
 * first recorded, a different request under it is rejected.
 */
data class Idempotency(val key: IdempotencyKey, val fingerprint: Fingerprint)
