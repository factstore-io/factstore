package io.factstore.foundationdb

import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_VALUE
import com.apple.foundationdb.Range
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.FactId
import io.factstore.core.StoreId
import io.factstore.core.StoreName
import io.factstore.core.FactType
import io.factstore.core.IdempotencyKey
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagValue
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import java.time.Instant
import java.util.concurrent.CompletableFuture

data class FdbFactStoreContext(
    val storeSubspace: StoreSubspace,
    val storeNameToIdIndex: StoreNameToIdIndexSubspace,
    val factSubspace: FactSubspace,
    val headSubspace: HeadSubspace,
    val factPositionIndexSubspace: FactPositionIndexSubspace,
    val eventTypeIndexSubspace: EventTypeIndexSubspace,
    val createdAtIndexSubspace: CreatedAtIndexSubspace,
    val subjectIndexSubspace: SubjectIndexSubspace,
    val metadataIndexSubspace: MetadataIndexSubspace,
    val tagsIndexSubspace: TagsIndexSubspace,
    val tagsTypeIndexSubspace: TagsTypeIndexSubspace,
    val idempotencySubspace: IdempotencySubspace,
) {

    companion object {

        fun create(rootDirectory: FactStoreRootDirectory): FdbFactStoreContext {
            val root = rootDirectory.rootDirectorySubspace
            return FdbFactStoreContext(
                storeSubspace = StoreSubspace(root.subspace(Tuple.from(STORES))),
                storeNameToIdIndex = StoreNameToIdIndexSubspace(root.subspace(Tuple.from(STORE_INDEX))),
                factSubspace = FactSubspace(root.subspace(Tuple.from(FACTS))),
                headSubspace = HeadSubspace(root.subspace(Tuple.from(HEAD_INDEX))),
                factPositionIndexSubspace = FactPositionIndexSubspace(root.subspace(Tuple.from(FACT_POSITIONS))),
                eventTypeIndexSubspace = EventTypeIndexSubspace(root.subspace(Tuple.from(EVENT_TYPE_INDEX))),
                createdAtIndexSubspace = CreatedAtIndexSubspace(root.subspace(Tuple.from(CREATED_AT_INDEX))),
                subjectIndexSubspace = SubjectIndexSubspace(root.subspace(Tuple.from(SUBJECT_INDEX))),
                metadataIndexSubspace = MetadataIndexSubspace(root.subspace(Tuple.from(METADATA_INDEX))),
                tagsIndexSubspace = TagsIndexSubspace(root.subspace(Tuple.from(TAGS_INDEX))),
                tagsTypeIndexSubspace = TagsTypeIndexSubspace(root.subspace(Tuple.from(TAGS_TYPE_INDEX))),
                idempotencySubspace = IdempotencySubspace(root.subspace(Tuple.from(IDEMPOTENCY_KEYS)))
            )
        }

    }
}

context(tr: ReadTransaction)
fun FdbFactStoreContext.getMetadata(storeId: StoreId): CompletableFuture<FdbStoreMetadata?> =
    storeSubspace.getMetadata(storeId)

fun FdbFactStoreContext.saveMetadata(metadata: FdbStoreMetadata, tr: Transaction) {
    with(tr) {
        storeSubspace.saveMetadata(metadata)
        storeNameToIdIndex.save(StoreName(metadata.name), StoreId(metadata.storeId))
    }
}

context(tr: ReadTransaction)
fun FdbFactStoreContext.lookUpStoreIdByName(name: StoreName): CompletableFuture<StoreId?> =
    with(tr) {
        storeNameToIdIndex.lookUpStore(name)
    }

@JvmInline
value class StoreSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun getMetadata(storeId: StoreId): CompletableFuture<FdbStoreMetadata?> =
        tr[subspace.pack(Tuple.from(storeId.uuid))].thenApply { valueBytes ->
            valueBytes?.let { Avro.decodeFromByteArray<FdbStoreMetadata>(it) }
        }

    context(tr: Transaction)
    fun saveMetadata(metadata: FdbStoreMetadata) {
        tr[subspace.pack(Tuple.from(metadata.storeId))] = Avro.encodeToByteArray(metadata)
    }

    fun range(): Range = subspace.range()

    context(tr: Transaction)
    fun clear(storeId: StoreId) {
        tr.clear(subspace.pack(Tuple.from(storeId.uuid)))
    }

}

@JvmInline
value class StoreNameToIdIndexSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun lookUpStore(name: StoreName): CompletableFuture<StoreId?> {
        return tr[subspace.pack(Tuple.from(name.value))].thenApply { valueBytes ->
            valueBytes?.let { StoreId(Tuple.fromBytes(it).getUUID(0)) }
        }
    }

    context(tr: Transaction)
    fun save(name: StoreName, storeId: StoreId) {
        tr[subspace.pack(Tuple.from(name.value))] = Tuple.from(storeId.uuid).pack()
    }

    context(tr: Transaction)
    fun clear(name: StoreName) {
        tr.clear(subspace.pack(Tuple.from(name.value)))
    }

}

@JvmInline
value class HeadSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun head(storeId: StoreId): CompletableFuture<FactPosition?> =
        tr[subspace.pack(Tuple.from(storeId.uuid))].thenApply { valueBytes ->
            valueBytes?.let { Tuple.fromBytes(it).getVersionstamp(0) }
        }

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

@JvmInline
value class FactSubspace(val subspace: Subspace) {

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
        tr.clear(subspace.pack(Tuple.from(storeId.uuid)))
    }

}

@JvmInline
value class FactPositionIndexSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun exists(storeId: StoreId, factId: FactId): CompletableFuture<Boolean> {
        return tr[subspace.pack(Tuple.from(storeId.uuid, factId.uuid))].thenApply {
            it != null
        }
    }

    context(tr: ReadTransaction)
    fun getPosition(storeId: StoreId, factId: FactId): CompletableFuture<FactPosition?> =
        tr[subspace.pack(Tuple.from(storeId.uuid, factId.uuid))].thenApply { valueBytes ->
            valueBytes?.let { Tuple.fromBytes(it).getVersionstamp(0) }
        }

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

@JvmInline
value class EventTypeIndexSubspace(val subspace: Subspace) {

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

@JvmInline
value class CreatedAtIndexSubspace(val subspace: Subspace) {

    context(tr: Transaction)
    fun save(storeId: StoreId, factId: FactId, createdAt: Instant, incompleteVersionstamp: Versionstamp) {
        val createdAtIndexKey = subspace.packWithVersionstamp(
            Tuple.from(storeId.uuid, createdAt.epochSecond, createdAt.nano, incompleteVersionstamp)
        )
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tr.mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, factIdTuple)
    }

    fun getKey(storeId: StoreId, createdAt: Instant): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, createdAt.epochSecond, createdAt.nano))

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
    }

}

@JvmInline
value class SubjectIndexSubspace(val subspace: Subspace) {

    fun range(storeId: StoreId, subject: Subject): Range =
        subspace.range(Tuple.from(storeId.uuid, subject.value))

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

@JvmInline
value class MetadataIndexSubspace(val subspace: Subspace) {

    context(tr: Transaction)
    fun save(storeId: StoreId, factId: FactId, metadata: Map<String, String>, incompleteVersionstamp: Versionstamp) {
        val factIdTuple = Tuple.from(factId.uuid).pack()
        metadata.forEach { (key, value) ->
            val metadataEntryIndex = subspace.packWithVersionstamp(
                Tuple.from(storeId.uuid, key, value, incompleteVersionstamp)
            )
            tr.mutate(SET_VERSIONSTAMPED_KEY, metadataEntryIndex, factIdTuple)
        }
    }

    context(tr: Transaction)
    fun clearRange(storeId: StoreId) {
        tr.clear(subspace.range(Tuple.from(storeId.uuid)))
    }

}

@JvmInline
value class TagsIndexSubspace(val subspace: Subspace) {

    fun getKey(storeId: StoreId, tag: Pair<TagKey, TagValue>): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, tag.first.value, tag.second.value))

    fun getKey(storeId: StoreId, tag: Pair<TagKey, TagValue>, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, tag.first.value, tag.second.value, position))

    fun range(storeId: StoreId, tag: Pair<TagKey, TagValue>): Range =
        subspace.range(Tuple.from(storeId.uuid, tag.first.value, tag.second.value))

    fun range(storeId: StoreId, key: TagKey, value: TagValue): Range =
        subspace.range(Tuple.from(storeId.uuid, key.value, value.value))

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

@JvmInline
value class TagsTypeIndexSubspace(val subspace: Subspace) {

    fun getKey(storeId: StoreId, factType: FactType, tag: Pair<TagKey, TagValue>): ByteArray =
        subspace.pack(Tuple.from(storeId.uuid, factType.value, tag.first.value, tag.second.value))

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

@JvmInline
value class IdempotencySubspace(val subspace: Subspace) {

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
