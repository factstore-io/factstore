package io.factstore.foundationdb

import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_VALUE
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import io.factstore.core.FactId
import io.factstore.core.StoreId
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
 *  (200, storeId, idempotencyKey)                         = empty
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

private val EMPTY_BYTE_ARRAY = ByteArray(0)

/** Where FactStore keeps its keys: one subspace per kind of record, all under one root. */
internal class FdbKeyspace(
    val storeSubspace: StoreSubspace,
    val storeNameToIdIndex: StoreNameToIdIndexSubspace,
    val factSubspace: FactSubspace,
    val headSubspace: HeadSubspace,
    val factPositionIndexSubspace: FactPositionIndexSubspace,
    val typeIndexSubspace: TypeIndexSubspace,
    val subjectIndexSubspace: SubjectIndexSubspace,
    val tagsIndexSubspace: TagsIndexSubspace,
    val tagsTypeIndexSubspace: TagsTypeIndexSubspace,
    val idempotencySubspace: IdempotencySubspace,
) {

    companion object {

        /** The subspaces of a fact store kept under [root]. */
        fun under(root: Subspace): FdbKeyspace = FdbKeyspace(
            storeSubspace = StoreSubspace(root.subspace(Tuple.from(STORES))),
            storeNameToIdIndex = StoreNameToIdIndexSubspace(root.subspace(Tuple.from(STORE_INDEX))),
            factSubspace = FactSubspace(root.subspace(Tuple.from(FACTS))),
            headSubspace = HeadSubspace(root.subspace(Tuple.from(HEAD_INDEX))),
            factPositionIndexSubspace = FactPositionIndexSubspace(root.subspace(Tuple.from(FACT_POSITIONS))),
            typeIndexSubspace = TypeIndexSubspace(root.subspace(Tuple.from(TYPE_INDEX))),
            subjectIndexSubspace = SubjectIndexSubspace(root.subspace(Tuple.from(SUBJECT_INDEX))),
            tagsIndexSubspace = TagsIndexSubspace(root.subspace(Tuple.from(TAGS_INDEX))),
            tagsTypeIndexSubspace = TagsTypeIndexSubspace(root.subspace(Tuple.from(TAGS_TYPE_INDEX))),
            idempotencySubspace = IdempotencySubspace(root.subspace(Tuple.from(IDEMPOTENCY_KEYS))),
        )
    }
}

internal class StoreSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun getMetadata(storeId: StoreId): FdbStoreMetadata? =
        tr[subspace.pack(Tuple.from(storeId.uuid))].await()?.toFdbStoreMetadata()

    context(tr: Transaction)
    fun saveMetadata(metadata: FdbStoreMetadata) {
        tr[subspace.pack(Tuple.from(metadata.storeId))] = metadata.encode()
    }

    fun range(): Range = subspace.range()

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.pack(Tuple.from(storeId.uuid)))
    }

}

internal class StoreNameToIdIndexSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun lookUpStore(name: StoreName): StoreId? =
        tr[subspace.pack(Tuple.from(name.value))].await()?.let { StoreId(Tuple.fromBytes(it).getUUID(0)) }

    context(tr: Transaction)
    fun save(name: StoreName, storeId: StoreId) {
        tr[subspace.pack(Tuple.from(name.value))] = Tuple.from(storeId.uuid).pack()
    }

    context(tr: Transaction)
    fun clear(name: StoreName) {
        tr.clear(subspace.pack(Tuple.from(name.value)))
    }

}

internal class HeadSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun head(storeId: StoreId): FactPosition? =
        tr[subspace.pack(Tuple.from(storeId.uuid))].await()?.let { Tuple.fromBytes(it).getVersionstamp(0) }

    fun headKey(storeId: StoreId): ByteArray =
        subspace.pack(storeId.uuid)

    context(tr: Transaction)
    fun save(storeId: StoreId, incompleteVersionstamp: Versionstamp) {
        val headKey = subspace.pack(storeId.uuid)
        val positionValue = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        tr.mutate(SET_VERSIONSTAMPED_VALUE, headKey, positionValue)
    }

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.pack(storeId.uuid))
    }

}

internal class FactSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun findFact(storeId: StoreId, factPosition: FactPosition): CompletableFuture<ByteArray?> {
        val factKey = subspace.pack(Tuple.from(storeId.uuid, factPosition))
        return tr[factKey]
    }

    context(tr: Transaction)
    fun saveFact(storeId: StoreId, incompleteVersionstamp: Versionstamp, serializedFact: ByteArray) {
        val globalPositionKey = subspace.packWithVersionstamp(
            Tuple.from(storeId.uuid, incompleteVersionstamp)
        )
        tr.mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, serializedFact)
    }

    fun getFactKey(storeId: StoreId, factPosition: FactPosition): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, factPosition))

    fun getRange(storeId: StoreId): Range = subspace.rangeOf(storeId)

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class FactPositionIndexSubspace(private val subspace: Subspace) {

    context(tr: ReadTransaction)
    suspend fun getPosition(storeId: StoreId, factId: FactId): FactPosition? =
        tr[subspace.pack(Tuple.from(storeId.uuid, factId.uuid))].await()?.let { Tuple.fromBytes(it).getVersionstamp(0) }

    context(tr: Transaction)
    fun savePosition(storeId: StoreId, factId: FactId, incompleteVersionstamp: Versionstamp) {
        val key = subspace.pack(Tuple.from(storeId.uuid, factId.uuid))
        val value = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        tr.mutate(SET_VERSIONSTAMPED_VALUE, key, value)
    }

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

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
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class SubjectIndexSubspace(private val subspace: Subspace) {

    fun indexRange(storeId: StoreId, subject: Subject) =
        IndexRange(subspace, Tuple.from(storeId.uuid, subject.value))

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class TagsIndexSubspace(private val subspace: Subspace) {

    fun indexRange(storeId: StoreId, tag: Map.Entry<TagKey, TagValue>) =
        IndexRange(subspace, Tuple.from(storeId.uuid, tag.key.value, tag.value.value))

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class TagsTypeIndexSubspace(private val subspace: Subspace) {

    fun indexRange(storeId: StoreId, type: FactType, tag: Map.Entry<TagKey, TagValue>) =
        IndexRange(subspace, Tuple.from(storeId.uuid, type.value, tag.key.value, tag.value.value))

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

internal class IdempotencySubspace(private val subspace: Subspace) {

    fun pack(storeId: StoreId, idempotencyKey: IdempotencyKey): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, idempotencyKey.value))

    context(tr: Transaction)
    fun save(storeId: StoreId, idempotencyKey: IdempotencyKey) {
        val key = pack(storeId, idempotencyKey)
        tr[key] = EMPTY_BYTE_ARRAY
    }

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.rangeOf(storeId))
    }

}

/** The keys of this subspace that belong to one store: every key here starts with the store's id. */
private fun Subspace.rangeOf(storeId: StoreId): Range = range(Tuple.from(storeId.uuid))

/** Where a fact sits in its store: the versionstamp of the transaction that appended it. */
internal typealias FactPosition = Versionstamp

internal fun Tuple.getFirstAsFactId(): FactId = getUUID(0).toFactId()
internal fun Tuple.getLastAsFactPosition(): FactPosition = getVersionstamp(size() - 1)
