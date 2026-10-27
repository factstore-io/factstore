package io.factstore.core

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.security.MessageDigest
import java.util.Arrays

/**
 * A digest of what an append asks for: its facts and its condition.
 *
 * An idempotency key is stored with the fingerprint of its request, which tells a retry of the
 * same request ([AppendResult.AlreadyApplied]) from a different request under a used key
 * ([AppendResult.IdempotencyKeyReused]).
 *
 * Two requests have the same fingerprint when they append the same facts, in the same order,
 * under the same condition. The store name and the idempotency key are not part of it: a key
 * belongs to one store already, and is what the fingerprint is compared under. Whatever has no
 * order is compared without one, so a retry that builds its maps, sets, query items or
 * [AppendCondition.All] in another order is still the same request.
 *
 * Every backend stores fingerprints, so their encoding is part of the store's data: changing it
 * makes a retry across the change look like a reused key.
 *
 * @author Domenic Cassisi
 */
class AppendFingerprint(bytes: ByteArray) {

    companion object {

        /** The size of a fingerprint in bytes: a SHA-256 digest. */
        const val SIZE = 32

        private const val ALGORITHM = "SHA-256"
    }

    private val bytes = bytes.copyOf()

    init {
        require(bytes.size == SIZE) { "A fingerprint must be $SIZE bytes, but was ${bytes.size}." }
    }

    /** The fingerprint's bytes, as a copy. */
    fun toByteArray(): ByteArray = bytes.copyOf()

    override fun equals(other: Any?): Boolean = other is AppendFingerprint && bytes.contentEquals(other.bytes)

    override fun hashCode(): Int = bytes.contentHashCode()

    override fun toString(): String = "AppendFingerprint(${bytes.toHexString()})"

    /** Encodes requests unambiguously: every value is tagged or prefixed with its size. */
    internal object Encoding {

        fun of(request: AppendRequest): AppendFingerprint {
            val encoded = encode {
                writeInt(request.facts.size)
                request.facts.forEach { writeFact(it) }
                writeCondition(request.condition)
            }
            return AppendFingerprint(MessageDigest.getInstance(ALGORITHM).digest(encoded))
        }

        private fun encode(block: DataOutputStream.() -> Unit): ByteArray =
            ByteArrayOutputStream().also { DataOutputStream(it).use(block) }.toByteArray()

        private fun DataOutputStream.writeFact(fact: FactInput) {
            writeText(fact.type.value)
            writeText(fact.subject.value)
            writeSized(fact.payload.data)
            writeEntries(fact.metadata.entries.map { (key, value) -> key.value to value.value })
            writeEntries(fact.tags.entries.map { (key, value) -> key.value to value.value })
        }

        private fun DataOutputStream.writeCondition(condition: AppendCondition) {
            when (condition) {
                AppendCondition.None -> writeByte(0)
                is AppendCondition.ExpectedLastFact -> {
                    writeByte(1)
                    writeText(condition.subject.value)
                    writeFactId(condition.expectedLastFactId)
                }
                is AppendCondition.All -> {
                    writeByte(2)
                    writeUnordered(condition.conditions.map { encode { writeCondition(it) } })
                }
                is AppendCondition.NoneMatch -> {
                    writeByte(3)
                    writeUnordered(condition.query.items.map { encode { writeQueryItem(it) } })
                    writeFactId(condition.after)
                }
            }
        }

        private fun DataOutputStream.writeQueryItem(item: FactQueryItem) {
            writeUnordered(item.subjects.map { it.value.encodeToByteArray() })
            writeUnordered(item.types.map { it.value.encodeToByteArray() })
            writeEntries(item.tags.entries.map { (key, value) -> key.value to value.value })
        }

        private fun DataOutputStream.writeFactId(factId: FactId?) {
            writeBoolean(factId != null)
            if (factId != null) {
                writeLong(factId.uuid.mostSignificantBits)
                writeLong(factId.uuid.leastSignificantBits)
            }
        }

        /** Writes the entries of a map, whose keys are unique, ordered by key. */
        private fun DataOutputStream.writeEntries(entries: List<Pair<String, String>>) {
            writeInt(entries.size)
            entries.sortedBy { it.first }.forEach { (key, value) ->
                writeText(key)
                writeText(value)
            }
        }

        /** Writes encoded values whose order carries no meaning, ordered by their bytes. */
        private fun DataOutputStream.writeUnordered(values: List<ByteArray>) {
            writeInt(values.size)
            values.sortedWith(Arrays::compareUnsigned).forEach { writeSized(it) }
        }

        private fun DataOutputStream.writeText(value: String) = writeSized(value.encodeToByteArray())

        private fun DataOutputStream.writeSized(value: ByteArray) {
            writeInt(value.size)
            write(value)
        }
    }
}

/**
 * The [AppendFingerprint] of this request.
 *
 * It is computed from the whole request on every call, so a backend computes it once per append.
 */
fun AppendRequest.fingerprint(): AppendFingerprint = AppendFingerprint.Encoding.of(this)
