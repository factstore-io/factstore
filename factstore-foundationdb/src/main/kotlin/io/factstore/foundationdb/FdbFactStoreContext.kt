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
import io.factstore.core.FactStoreId
import io.factstore.core.FactStoreName
import io.factstore.core.FactType
import io.factstore.core.SubjectRef
import io.factstore.core.TagKey
import io.factstore.core.TagValue
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import java.time.Instant
import java.util.concurrent.CompletableFuture

data class FdbFactStoreContext(
    val storeSubspace: Subspace,
    val storeNameToIdIndex: Subspace,
    val factSubspace: FactSubspace,
    val headSubspace: HeadSubspace,
    val factPositionIndexSubspace: FactPositionIndexSubspace,
    val eventTypeIndexSubspace: EventTypeIndexSubspace,
    val createdAtIndexSubspace: CreatedAtIndexSubspace,
    val subjectIndexSubspace: SubjectIndexSubspace,
    val metadataIndexSubspace: MetadataIndexSubspace,
    val tagsIndexSubspace: TagsIndexSubspace,
    val tagsTypeIndexSubspace: TagsTypeIndexSubspace,
    val idempotencySubspace: Subspace,
) {

    companion object {

        fun create(rootDirectory: FactStoreRootDirectory): FdbFactStoreContext {
            val root = rootDirectory.rootDirectorySubspace
            return FdbFactStoreContext(
                storeSubspace = root.subspace(Tuple.from(STORES)),
                storeNameToIdIndex = root.subspace(Tuple.from(STORE_INDEX)),
                factSubspace = FactSubspace(root.subspace(Tuple.from(FACTS))),
                headSubspace = HeadSubspace(root.subspace(Tuple.from(HEAD_INDEX))),
                factPositionIndexSubspace = FactPositionIndexSubspace(root.subspace(Tuple.from(FACT_POSITIONS))),
                eventTypeIndexSubspace = EventTypeIndexSubspace(root.subspace(Tuple.from(EVENT_TYPE_INDEX))),
                createdAtIndexSubspace = CreatedAtIndexSubspace(root.subspace(Tuple.from(CREATED_AT_INDEX))),
                subjectIndexSubspace = SubjectIndexSubspace(root.subspace(Tuple.from(SUBJECT_INDEX))),
                metadataIndexSubspace = MetadataIndexSubspace(root.subspace(Tuple.from(METADATA_INDEX))),
                tagsIndexSubspace = TagsIndexSubspace(root.subspace(Tuple.from(TAGS_INDEX))),
                tagsTypeIndexSubspace = TagsTypeIndexSubspace(root.subspace(Tuple.from(TAGS_TYPE_INDEX))),
                idempotencySubspace = root.subspace(Tuple.from(IDEMPOTENCY_KEYS))
            )
        }

    }
}

fun FdbFactStoreContext.getMetadata(factStoreId: FactStoreId, tr: ReadTransaction): CompletableFuture<FdbFactStoreMetadata?> =
    tr[storeSubspace.pack(Tuple.from(factStoreId.uuid))].thenApply { valueBytes ->
        valueBytes?.let { Avro.decodeFromByteArray<FdbFactStoreMetadata>(it) }
    }

fun FdbFactStoreContext.saveMetadata(metadata: FdbFactStoreMetadata, tr: Transaction) {
    tr[storeSubspace.pack(Tuple.from(metadata.storeId))] = Avro.encodeToByteArray(metadata)
    tr[storeNameToIdIndex.pack(Tuple.from(metadata.name))] = Tuple.from(metadata.storeId).pack()
}

fun FdbFactStoreContext.lookUpFactstoreIdByName(name: FactStoreName, tr: ReadTransaction): CompletableFuture<FactStoreId?> =
    tr[storeNameToIdIndex.pack(Tuple.from(name.value))].thenApply { valueBytes ->
        valueBytes?.let { FactStoreId(Tuple.fromBytes(it).getUUID(0)) }
    }

@JvmInline
value class HeadSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun head(factstoreId: FactStoreId): CompletableFuture<FactPosition?> =
        tr[subspace.pack(Tuple.from(factstoreId.uuid))].thenApply { valueBytes ->
            valueBytes?.let { Tuple.fromBytes(it).getVersionstamp(0) }
        }

    fun headKey(factstoreId: FactStoreId): ByteArray =
        subspace.pack(factstoreId.uuid)

    context(tr: Transaction)
    fun save(factstoreId: FactStoreId, incompleteVersionstamp: Versionstamp) {
        val headKey = subspace.pack(factstoreId.uuid)
        val positionValue = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        tr.mutate(SET_VERSIONSTAMPED_VALUE, headKey, positionValue)
    }

}

@JvmInline
value class FactSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun findFact(factStoreId: FactStoreId, factPosition: FactPosition): CompletableFuture<ByteArray?> {
        val factKey = subspace.pack(Tuple.from(factStoreId.uuid, factPosition))
        return tr[factKey]
    }

    context(tr: Transaction)
    fun saveFact(factstoreId: FactStoreId, incompleteVersionstamp: Versionstamp, serializedFact: ByteArray) {
        val globalPositionKey = subspace.packWithVersionstamp(
            Tuple.from(factstoreId.uuid, incompleteVersionstamp)
        )
        tr.mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, serializedFact)
    }

    fun getFactKey(factstoreId: FactStoreId, factPosition: FactPosition): ByteArray =
        subspace.pack(Tuple.from(factstoreId.uuid, factPosition))

    fun getRange(factstoreId: FactStoreId): Range =
        subspace.range(Tuple.from(factstoreId.uuid))
}

@JvmInline
value class FactPositionIndexSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun exists(factstoreId: FactStoreId, factId: FactId): CompletableFuture<Boolean> {
        return tr[subspace.pack(Tuple.from(factstoreId.uuid, factId.uuid))].thenApply {
            it != null
        }
    }

    context(tr: ReadTransaction)
    fun getPosition(factstoreId: FactStoreId, factId: FactId): CompletableFuture<FactPosition?> =
        tr[subspace.pack(Tuple.from(factstoreId.uuid, factId.uuid))].thenApply { valueBytes ->
            valueBytes?.let { Tuple.fromBytes(it).getVersionstamp(0) }
        }

     context(tr: Transaction)
     fun savePosition(factstoreId: FactStoreId, factId: FactId, incompleteVersionstamp: Versionstamp) {
         val key = subspace.pack(Tuple.from(factstoreId.uuid, factId.uuid))
         val value = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
         tr.mutate(SET_VERSIONSTAMPED_VALUE, key, value)
     }

}

@JvmInline
value class EventTypeIndexSubspace(val subspace: Subspace) {

    context(tr: Transaction)
    fun save(factStoreId: FactStoreId, factId: FactId, factType: FactType, incompleteVersionstamp: Versionstamp) {
        val eventTypeIndexKey = subspace.packWithVersionstamp(
            Tuple.from(factStoreId.uuid, factType.value, incompleteVersionstamp)
        )
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tr.mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, factIdTuple)
    }

}

@JvmInline
value class CreatedAtIndexSubspace(val subspace: Subspace) {

    context(tr: Transaction)
    fun save(factStoreId: FactStoreId, factId: FactId, createdAt: Instant, incompleteVersionstamp: Versionstamp) {
        val createdAtIndexKey = subspace.packWithVersionstamp(
            Tuple.from(factStoreId.uuid, createdAt.epochSecond, createdAt.nano, incompleteVersionstamp)
        )
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tr.mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, factIdTuple)
    }

    fun getKey(factstoreId: FactStoreId, createdAt: Instant): ByteArray =
        subspace.pack(Tuple.from(factstoreId.uuid, createdAt.epochSecond, createdAt.nano))

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()
}

@JvmInline
value class SubjectIndexSubspace(val subspace: Subspace) {

    fun range(factStoreId: FactStoreId, subjectRef: SubjectRef): Range =
        subspace.range(Tuple.from(factStoreId.uuid, subjectRef.type, subjectRef.id))

    context(tr: Transaction)
    fun save(factStoreId: FactStoreId, factId: FactId, subjectRef: SubjectRef, incompleteVersionstamp: Versionstamp) {
        val keyTuple = Tuple.from(factStoreId.uuid, subjectRef.type, subjectRef.id, incompleteVersionstamp)
        val keyBytes = subspace.packWithVersionstamp(keyTuple)
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tr.mutate(SET_VERSIONSTAMPED_KEY, keyBytes, factIdTuple)
    }

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

}

@JvmInline
value class MetadataIndexSubspace(val subspace: Subspace) {

    context(tr: Transaction)
    fun save(factStoreId: FactStoreId, factId: FactId, metadata:  Map<String, String>, incompleteVersionstamp: Versionstamp) {
        val factIdTuple = Tuple.from(factId.uuid).pack()
        metadata.forEach { (key, value) ->
            val metadataEntryIndex = subspace.packWithVersionstamp(
                Tuple.from(factStoreId.uuid, key, value, incompleteVersionstamp)
            )
            tr.mutate(SET_VERSIONSTAMPED_KEY, metadataEntryIndex, factIdTuple)

        }
    }

}

@JvmInline
value class TagsIndexSubspace(val subspace: Subspace) {

    fun getKey(factStoreId: FactStoreId, tag: Pair<TagKey, TagValue>): ByteArray =
        subspace.pack(Tuple.from(factStoreId.uuid, tag.first.value, tag.second.value))

    fun getKey(factStoreId: FactStoreId, tag: Pair<TagKey, TagValue>, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(factStoreId.uuid, tag.first.value, tag.second.value, position))

    fun range(factStoreId: FactStoreId, tag: Pair<TagKey, TagValue>): Range =
        subspace.range(Tuple.from(factStoreId.uuid, tag.first.value, tag.second.value))

    fun range(factStoreId: FactStoreId, key: TagKey, value: TagValue): Range =
        subspace.range(Tuple.from(factStoreId.uuid, key.value, value.value))

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

    context(tr: Transaction)
    fun save(factStoreId: FactStoreId, factId: FactId, tags: Map<TagKey, TagValue>, incompleteVersionstamp: Versionstamp) {
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tags.forEach { (key, value) ->
            val tagsEntryIndex = subspace.packWithVersionstamp(
                Tuple.from(factStoreId.uuid, key.value, value.value, incompleteVersionstamp)
            )
            tr.mutate(SET_VERSIONSTAMPED_KEY, tagsEntryIndex, factIdTuple)
        }

    }

}

@JvmInline
value class TagsTypeIndexSubspace(val subspace: Subspace) {

    fun getKey(factStoreId: FactStoreId, factType: FactType, tag: Pair<TagKey, TagValue>): ByteArray =
        subspace.pack(Tuple.from(factStoreId.uuid, factType.value, tag.first.value, tag.second.value))

    fun getKey(factStoreId: FactStoreId, factType: FactType, tag: Pair<TagKey, TagValue>, position: FactPosition): ByteArray =
        subspace.pack(Tuple.from(factStoreId.uuid, factType.value, tag.first.value, tag.second.value, position))

    fun range(factStoreId: FactStoreId, factType: FactType, tag: Pair<TagKey, TagValue>): Range =
        subspace.range(Tuple.from(factStoreId.uuid, factType.value, tag.first.value, tag.second.value))

    fun unpackPosition(key: ByteArray): FactPosition =
        subspace.unpack(key).getLastAsFactPosition()

    context(tr: Transaction)
    fun save(factStoreId: FactStoreId, factId: FactId, type: FactType, tags: Map<TagKey, TagValue>, incompleteVersionstamp: Versionstamp) {
        val factIdTuple = Tuple.from(factId.uuid).pack()
        tags.forEach { (key, value) ->
            val tagTypeIndex = subspace.packWithVersionstamp(
                Tuple.from(factStoreId.uuid, type.value, key.value, value.value, incompleteVersionstamp)
            )
            tr.mutate(SET_VERSIONSTAMPED_KEY, tagTypeIndex, factIdTuple)
        }
    }
}
