package io.factstore.core

/**
 * The fingerprint of a request, stored with its idempotency key to tell a retry of the same
 * request from a different request under a used key.
 *
 * It is opaque: fingerprints are only compared, by their value. Whoever computes them writes the
 * same request as the same value every time.
 */
@JvmInline
value class Fingerprint(val value: String) {

    init {
        require(value.isNotEmpty()) { "A fingerprint must not be empty." }
    }
}
