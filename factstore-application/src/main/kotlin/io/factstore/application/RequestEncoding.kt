package io.factstore.application

import io.factstore.core.Fingerprint
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.security.MessageDigest

/**
 * How a request is written for its fingerprint: under a stable type name, with its fields in a
 * fixed order.
 *
 * The encoding is stored data: fingerprints are kept with their idempotency keys, so changing a
 * request's encoding makes a retry across the change look like a reused key.
 */
interface RequestEncoding<in R> {

    /** The request's name in its fingerprint; independent of class names, so renaming a class changes nothing. */
    val typeName: String

    fun encode(request: R, encoder: RequestEncoder)
}

/** Writes a request's fields so that no two requests read the same: every value is prefixed with its size. */
class RequestEncoder internal constructor(private val out: DataOutputStream) {

    fun string(value: String) {
        val bytes = value.encodeToByteArray()
        out.writeInt(bytes.size)
        out.write(bytes)
    }
}

/** The SHA-256 of the request's type name and fields, in lowercase hex. */
fun <R> RequestEncoding<R>.fingerprint(request: R): Fingerprint {
    val bytes = ByteArrayOutputStream()
    val encoder = RequestEncoder(DataOutputStream(bytes))
    encoder.string(typeName)
    encode(request, encoder)
    return Fingerprint(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()).toHexString())
}
