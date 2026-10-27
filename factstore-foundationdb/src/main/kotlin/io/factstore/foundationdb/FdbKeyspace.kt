package io.factstore.foundationdb

import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_VALUE
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import io.factstore.core.AppendFingerprint
import io.factstore.core.FactId
import io.factstore.core.StoreId
import io.factstore.core.StoreMetadata
import io.factstore.core.StoreName
import io.factstore.core.FactType
import io.factstore.core.IdempotencyKey
import io.factstore.core.ReadDirection
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagValue
import io.factstore.core.toFactId
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

/*
 * The key layout of FactStore, as tuples under one root directory:
 *
 * ```
 *  ("stores", storeId)                                    = store metadata
 *  ("store-index", name)                                  = (storeId)
 *  (1, storeId, versionstamp)                             = serialized fact
 *  (2, storeId, factId)                                   = (versionstamp)
 *  (100, storeId)                                         = (versionstamp of the newest fact)
 *  (102, storeId, type, versionstamp)                     = (factId)
 *  (103, storeId, subject, versionstamp)                  = (factId)
 *  (105, storeId, tagKey, tagValue, versionstamp)         = (factId)
 *  (106, storeId, type, tagKey, tagValue, versionstamp)   = (factId)
 *  (200, storeId, idempotencyKey)                         = (versionstamp, fact count, fingerprint)
 * ```
 */

private const val STORES = "stores"
private const val STORE_INDEX = "store-index"
private const val FACTS = 1
private const val FACT_POSITIONS = 2
private const val HEAD_INDEX = 100
private const val TYPE_INDEX = 102
private const val SUBJECT_INDEX = 103
private const val TAGS_INDEX = 105
private const val TAGS_TYPE_INDEX = 106
private const val IDEMPOTENCY_KEYS = 200

/** Where FactStore keeps its keys: one subspace per kind of record, all under [root]. */
internal class FdbKeyspace(root: Subspace) {
    val storeSubspace = StoreSubspace(root[STORES])
    val storeNameToIdIndex = StoreNameToIdIndexSubspace(root[STORE_INDEX])
    val factSubspace = FactSubspace(root[FACTS])
    val headSubspace = HeadSubspace(root[HEAD_INDEX])
    val factPositionIndexSubspace = FactPositionIndexSubspace(root[FACT_POSITIONS])
    val typeIndexSubspace = TypeIndexSubspace(root[TYPE_INDEX])
    val subjectIndexSubspace = SubjectIndexSubspace(root[SUBJECT_INDEX])
    val tagsIndexSubspace = TagsIndexSubspace(root[TAGS_INDEX])
    val tagsTypeIndexSubspace = TagsTypeIndexSubspace(root[TAGS_TYPE_INDEX])
    val idempotencySubspace = IdempotencySubspace(root[IDEMPOTENCY_KEYS])
}

internal class StoreSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun metadataOf(storeId: StoreId): StoreMetadata? =
        tr[keyOf(storeId)].await()?.toFdbStoreMetadata()?.toStoreMetadata()

    /** The metadata of every store. */
    context(tr: ReadTransaction)
    suspend fun all(): List<StoreMetadata> =
        tr.getRange(subspace.range()).asList().await().map { it.value.toFdbStoreMetadata().toStoreMetadata() }

    context(tr: Transaction)
    fun save(metadata: StoreMetadata) {
        tr[keyOf(metadata.id)] = metadata.toFdbStoreMetadata().encode()
    }

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(keyOf(storeId))
    }

    private fun keyOf(storeId: StoreId): ByteArray = subspace.pack(Tuple.from(storeId.uuid))

}

internal class StoreNameToIdIndexSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun storeIdOf(name: StoreName): StoreId? =
        tr[keyOf(name)].await()?.let { StoreId(Tuple.fromBytes(it).getUUID(0)) }

    context(tr: Transaction)
    fun save(name: StoreName, storeId: StoreId) {
        tr[keyOf(name)] = Tuple.from(storeId.uuid).pack()
    }

    context(tr: Transaction)
    fun clear(name: StoreName) {
        tr.clear(keyOf(name))
    }

    private fun keyOf(name: StoreName): ByteArray = subspace.pack(Tuple.from(name.value))

}

internal class HeadSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun headOf(storeId: StoreId): FactPosition? =
        tr[keyOf(storeId)].await()?.let { Tuple.fromBytes(it).getVersionstamp(0) }

    /** The key holding the position of the store's newest fact, which moves with every append. */
    fun keyOf(storeId: StoreId): ByteArray = subspace.pack(Tuple.from(storeId.uuid))

    context(tr: Transaction)
    fun save(storeId: StoreId, incompleteVersionstamp: Versionstamp) {
        val positionValue = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        tr.mutate(SET_VERSIONSTAMPED_VALUE, keyOf(storeId), positionValue)
    }

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(keyOf(storeId))
    }

}

internal class FactSubspace(private val subspace: Subspace) {

    /** The serialized fact at [position], read when the future is awaited. */
    context(tr: ReadTransaction)
    fun factAt(storeId: StoreId, position: FactPosition): CompletableFuture<ByteArray?> =
        tr[keyOf(storeId, position)]

    context(tr: Transaction)
    fun save(storeId: StoreId, incompleteVersionstamp: Versionstamp, serializedFact: ByteArray) {
        val globalPositionKey = subspace.packWithVersionstamp(
            Tuple.from(storeId.uuid, incompleteVersionstamp)
        )
        tr.mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, serializedFact)
    }

    /** The serialized facts of one append: the [count] positions starting at [first], in order. */
    context(tr: ReadTransaction)
    suspend fun factsAppendedTogether(storeId: StoreId, first: FactPosition, count: Int): List<ByteArray> {
        val end = Versionstamp.complete(first.transactionVersion, first.userVersion + count)
        return tr.getRange(keyOf(storeId, first), keyOf(storeId, end)).asList().await().map { it.value }
    }

    fun keyOf(storeId: StoreId, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, position))

    fun rangeOf(storeId: StoreId): Range = subspace.rangeOf(storeId)

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class FactPositionIndexSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun positionOf(storeId: StoreId, factId: FactId): FactPosition? =
        tr[keyOf(storeId, factId)].await()?.let { Tuple.fromBytes(it).getVersionstamp(0) }

    context(tr: Transaction)
    fun save(storeId: StoreId, factId: FactId, incompleteVersionstamp: Versionstamp) {
        val value = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        tr.mutate(SET_VERSIONSTAMPED_VALUE, keyOf(storeId, factId), value)
    }

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

    private fun keyOf(storeId: StoreId, factId: FactId): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, factId.uuid))

}

/**
 * The entries of one index under one prefix, such as the facts of one subject in one store.
 *
 * Every entry is keyed by the prefix followed by the position of its fact, and holds the fact's id.
 * The index subspaces only say what their prefixes are; how a position becomes a key and back is
 * defined here, once for all of them.
 */
internal class IndexRange(private val subspace: Subspace, private val prefix: Tuple) {

    val range: Range = subspace.range(prefix)

    /** The key the fact at [position] has in this range. */
    fun keyOf(position: FactPosition): ByteArray = subspace.pack(prefix.add(position))

    /** The position of the fact behind [key], a key of this range. */
    fun positionOf(key: ByteArray): FactPosition = subspace.unpack(key).getLastAsFactPosition()

    /** The keys of this range up to the fact at [head], or all of them without one. */
    fun pinnedKeys(head: FactPosition?, direction: ReadDirection): PinnedKeys =
        PinnedKeys(range, head?.let(::keyOf), direction)

    /** Adds the fact to this range, at the position its append's versionstamp will give it. */
    context(tr: Transaction)
    fun save(factId: FactId, incompleteVersionstamp: Versionstamp) {
        val key = subspace.packWithVersionstamp(prefix.add(incompleteVersionstamp))
        tr.mutate(SET_VERSIONSTAMPED_KEY, key, Tuple.from(factId.uuid).pack())
    }
}

internal class TypeIndexSubspace(private val subspace: Subspace) {

    fun indexRange(storeId: StoreId, type: FactType) =
        IndexRange(subspace, Tuple.from(storeId.uuid, type.value))

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class SubjectIndexSubspace(private val subspace: Subspace) {

    fun indexRange(storeId: StoreId, subject: Subject) =
        IndexRange(subspace, Tuple.from(storeId.uuid, subject.value))

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class TagsIndexSubspace(private val subspace: Subspace) {

    fun indexRange(storeId: StoreId, tag: Map.Entry<TagKey, TagValue>) =
        IndexRange(subspace, Tuple.from(storeId.uuid, tag.key.value, tag.value.value))

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class TagsTypeIndexSubspace(private val subspace: Subspace) {

    fun indexRange(storeId: StoreId, type: FactType, tag: Map.Entry<TagKey, TagValue>) =
        IndexRange(subspace, Tuple.from(storeId.uuid, type.value, tag.key.value, tag.value.value))

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

/**
 * What an idempotency key applied: the facts its append stored, and the fingerprint of its request.
 *
 * The facts of one append share the transaction's versionstamp and are told apart by their index
 * as user version, so they are the [factCount] positions starting at [firstPosition].
 */
internal class AppliedAppend(
    val firstPosition: FactPosition,
    val factCount: Int,
    val fingerprint: AppendFingerprint,
)

internal class IdempotencySubspace(private val subspace: Subspace) {

    /** What the key applied in the store, or `null` if the key was not used there. */
    context(tr: ReadTransaction)
    suspend fun appliedAppendOf(storeId: StoreId, idempotencyKey: IdempotencyKey): AppliedAppend? =
        tr[keyOf(storeId, idempotencyKey)].await()?.let { value ->
            val tuple = Tuple.fromBytes(value)
            AppliedAppend(
                firstPosition = tuple.getVersionstamp(0),
                factCount = tuple.getLong(1).toInt(),
                fingerprint = AppendFingerprint(tuple.getBytes(2)),
            )
        }

    /** Records that the key applied the [factCount] facts this transaction appends. */
    context(tr: Transaction)
    fun save(storeId: StoreId, idempotencyKey: IdempotencyKey, factCount: Int, fingerprint: AppendFingerprint) {
        val value = Tuple.from(Versionstamp.incomplete(0), factCount, fingerprint.toByteArray()).packWithVersionstamp()
        tr.mutate(SET_VERSIONSTAMPED_VALUE, keyOf(storeId, idempotencyKey), value)
    }

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

    private fun keyOf(storeId: StoreId, idempotencyKey: IdempotencyKey): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, idempotencyKey.uuid))

}

/** The keys of this subspace that belong to one store: every key here starts with the store's id. */
private fun Subspace.rangeOf(storeId: StoreId): Range = range(Tuple.from(storeId.uuid))

/** Where a fact sits in its store: the versionstamp of the transaction that appended it. */
internal typealias FactPosition = Versionstamp

internal fun Tuple.getFirstAsFactId(): FactId = getUUID(0).toFactId()
internal fun Tuple.getLastAsFactPosition(): FactPosition = getVersionstamp(size() - 1)
