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
private const val EVENT_TYPE_INDEX = 102
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
    val eventTypeIndexSubspace: EventTypeIndexSubspace,
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
            eventTypeIndexSubspace = EventTypeIndexSubspace(root.subspace(Tuple.from(EVENT_TYPE_INDEX))),
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

    fun getRange(storeId: StoreId): Range =
        subspace.range(Tuple.from(storeId.uuid))

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
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
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
    }

}

internal class EventTypeIndexSubspace(private val subspace: Subspace) {

    fun range(storeId: StoreId, factType: FactType): Range =
        subspace.range(Tuple.from(storeId.uuid, factType.value))

    fun getKey(storeId: StoreId, factType: FactType, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, factType.value, position))

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

    context(tr: Transaction)
    fun save(storeId: StoreId, factId: FactId, factType: FactType, incompleteVersionstamp: Versionstamp) {
        val eventTypeIndexKey = subspace.packWithVersionstamp(
            Tuple.from(storeId.uuid, factType.value, incompleteVersionstamp)
        )
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tr.mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, factIdTuple)
    }

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
    }

}

internal class SubjectIndexSubspace(private val subspace: Subspace) {

    fun range(storeId: StoreId, subject: Subject): Range =
        subspace.range(Tuple.from(storeId.uuid, subject.value))

    fun getKey(storeId: StoreId, subject: Subject, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, subject.value, position))

    context(tr: Transaction)
    fun save(storeId: StoreId, factId: FactId, subject: Subject, incompleteVersionstamp: Versionstamp) {
        val keyTuple = Tuple.from(storeId.uuid, subject.value, incompleteVersionstamp)
        val keyBytes = subspace.packWithVersionstamp(keyTuple)
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tr.mutate(SET_VERSIONSTAMPED_KEY, keyBytes, factIdTuple)
    }

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
    }

}

internal class TagsIndexSubspace(private val subspace: Subspace) {

    fun getKey(storeId: StoreId, tag: Pair<TagKey, TagValue>, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, tag.first.value, tag.second.value, position))

    fun range(storeId: StoreId, tag: Pair<TagKey, TagValue>): Range =
        subspace.range(Tuple.from(storeId.uuid, tag.first.value, tag.second.value))

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

    context(tr: Transaction)
    fun save(storeId: StoreId, factId: FactId, tags: Map<TagKey, TagValue>, incompleteVersionstamp: Versionstamp) {
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tags.forEach { (key, value) ->
            val tagsEntryIndex = subspace.packWithVersionstamp(
                Tuple.from(storeId.uuid, key.value, value.value, incompleteVersionstamp)
            )
            tr.mutate(SET_VERSIONSTAMPED_KEY, tagsEntryIndex, factIdTuple)
        }
    }

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
    }

}

internal class TagsTypeIndexSubspace(private val subspace: Subspace) {

    fun getKey(storeId: StoreId, factType: FactType, tag: Pair<TagKey, TagValue>, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, factType.value, tag.first.value, tag.second.value, position))

    fun range(storeId: StoreId, factType: FactType, tag: Pair<TagKey, TagValue>): Range =
        subspace.range(Tuple.from(storeId.uuid, factType.value, tag.first.value, tag.second.value))

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

    context(tr: Transaction)
    fun save(
        storeId: StoreId,
        factId: FactId,
        type: FactType,
        tags: Map<TagKey, TagValue>,
        incompleteVersionstamp: Versionstamp
    ) {
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tags.forEach { (key, value) ->
            val tagTypeIndex = subspace.packWithVersionstamp(
                Tuple.from(storeId.uuid, type.value, key.value, value.value, incompleteVersionstamp)
            )
            tr.mutate(SET_VERSIONSTAMPED_KEY, tagTypeIndex, factIdTuple)
        }
    }

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
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
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
    }

}

/** Where a fact sits in its store: the versionstamp of the transaction that appended it. */
internal typealias FactPosition = Versionstamp

internal fun Tuple.getFirstAsFactId(): FactId = getUUID(0).toFactId()
internal fun Tuple.getLastAsFactPosition(): FactPosition = getVersionstamp(size() - 1)
