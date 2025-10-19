package com.cassisi.openeventstore.core.impl

import com.apple.foundationdb.Database
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_VALUE
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.cassisi.openeventstore.core.Fact
import com.cassisi.openeventstore.core.Subject
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.text.Charsets.UTF_8

const val FACT_STORE = "fact-store"
const val FACT_ID = "id"
const val FACT_TYPE = "type"
const val FACT_PAYLOAD = "payload"
const val FACT_SUBJECT_TYPE = "subject-type"
const val FACT_SUBJECT_ID = "subject-id"
const val CREATED_AT = "created-at"
const val POSITION = "position"
const val METADATA = "metadata"
const val TAGS = "tags"

const val GLOBAL_FACT_POSITION_INDEX = "global"
const val CREATED_AT_INDEX = "created-at-index"
const val EVENT_TYPE_INDEX = "type-index"
const val SUBJECT_INDEX = "subject-index"
const val METADATA_INDEX = "metadata-index"
const val TAGS_INDEX = "tags-index"
const val TAGS_TYPE_INDEX = "tags-type-index"

val EMPTY_BYTE_ARRAY = ByteArray(0)
const val DEFAULT_INDEX = 0

/**
 * A simple event/fact store implementation based on FoundationDB.
 *
 * FACT SPACES:
 *  /fact-store/id/{factId} = ∅   (existence / deduplication anchor)
 *  /fact-store/type/{factId} = type
 *  /fact-store/payload/{factId} = payload
 *  /fact-store/subject-type/{factId} = payload
 *  /fact-store/subject-id/{factId} = payload
 *  /fact-store/created-at/{factId} = timestamp in UTC
 *  /fact-store/position/{factId} = versionstamp + index
 *  /fact-store/metadata/{factId}/{key} = metadata value
 *  /fact-store/tags/{factId}/{key} = value
 *
 * INDEX SPACES
 *  /fact-store/global/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/type-index/{type}/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/created-at-index/{epochSecond}/{nano}/{vs}/{index}/{factId} = ∅
 *  /fact-store/subject-index/{subjectType}/{subjectId}/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/metadata-index/{key}/{value}/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/tags-index/{key}/{value}/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/tags-type-index/{type}/{key}/{value}/{versionstamp}/{index}/{factId} = ∅
 *
 */
class FdbFactStore(
    internal val db: Database
) {


    // DIRECTORIES
    internal val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(FACT_STORE)).get()

    // FACT SPACES
    internal val factIdSubspace = root.subspace(Tuple.from(FACT_ID))
    internal val factTypeSubspace = root.subspace(Tuple.from(FACT_TYPE))
    internal val factPayloadSubspace = root.subspace(Tuple.from(FACT_PAYLOAD))
    internal val subjectTypeSubspace = root.subspace(Tuple.from(FACT_SUBJECT_TYPE))
    internal val subjectIdSubspace = root.subspace(Tuple.from(FACT_SUBJECT_ID))
    internal val createdAtSubspace = root.subspace(Tuple.from(CREATED_AT))
    internal val positionSubspace = root.subspace(Tuple.from(POSITION))
    internal val metadataSubspace = root.subspace(Tuple.from(METADATA))
    internal val tagsSubspace = root.subspace(Tuple.from(TAGS))

    // INDEX SPACES
    internal val globalFactPositionSubspace = root.subspace(Tuple.from(GLOBAL_FACT_POSITION_INDEX))
    internal val eventTypeIndexSubspace = root.subspace(Tuple.from(EVENT_TYPE_INDEX))
    internal val createdAtIndexSubspace = root.subspace(Tuple.from(CREATED_AT_INDEX))
    internal val subjectIndexSubspace = root.subspace(Tuple.from(SUBJECT_INDEX))
    internal val metadataIndexSubspace = root.subspace(Tuple.from(METADATA_INDEX))
    internal val tagsIndexSubspace = root.subspace(Tuple.from(TAGS_INDEX))
    internal val tagsTypeIndexSubspace = root.subspace(Tuple.from(TAGS_TYPE_INDEX))


    internal fun List<Fact>.store(transaction: Transaction) {
        forEachIndexed { index, fact ->
            transaction.store(fact, index)
        }
    }

    internal fun Transaction.store(fact: Fact, index: Int = DEFAULT_INDEX) {
        storeFact(fact, index)
        storeIndexes(fact, index)
    }

    private fun Transaction.storeFact(fact: Fact, index: Int) {
        val factIdTuple = Tuple.from(fact.id)

        this[factIdSubspace.pack(factIdTuple)] = EMPTY_BYTE_ARRAY
        this[factTypeSubspace.pack(factIdTuple)] = fact.type.toByteArray(UTF_8)
        this[factPayloadSubspace.pack(factIdTuple)] = fact.payload
        this[subjectTypeSubspace.pack(factIdTuple)] = fact.subject.type.toByteArray(UTF_8)
        this[subjectIdSubspace.pack(factIdTuple)] = fact.subject.id.toByteArray(UTF_8)
        this[createdAtSubspace.pack(factIdTuple)] = Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano).pack()

        val positionKey = positionSubspace.pack(factIdTuple)
        val positionValue = Tuple.from(Versionstamp.incomplete(), index).packWithVersionstamp()
        mutate(SET_VERSIONSTAMPED_VALUE, positionKey, positionValue)

        fact.metadata.forEach { (key, value) ->
            this[metadataSubspace.pack(factIdTuple.add(key))] = value.toByteArray(UTF_8)
        }

        fact.tags.forEach { (key, value) ->
            this[tagsSubspace.pack(factIdTuple.add(key))] = value.toByteArray(UTF_8)
        }
    }

    private fun Transaction.storeIndexes(fact: Fact, index: Int) {
        val factId = fact.id

        val globalPositionKey = globalFactPositionSubspace.packWithVersionstamp(
            Tuple.from(Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, EMPTY_BYTE_ARRAY)

        val eventTypeIndexKey = eventTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.type, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, EMPTY_BYTE_ARRAY)

        val createdAtIndexKey = createdAtIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, EMPTY_BYTE_ARRAY)

        val subjectIndex = subjectIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.subject.type, fact.subject.id, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, subjectIndex, EMPTY_BYTE_ARRAY)

        fact.metadata.forEach { (key, value) ->
            val metadataEntryIndex = metadataIndexSubspace.packWithVersionstamp(
                Tuple.from(key, value, Versionstamp.incomplete(), index, factId)
            )
            mutate(SET_VERSIONSTAMPED_KEY, metadataEntryIndex, EMPTY_BYTE_ARRAY)
        }

        fact.tags.forEach { (key, value) ->
            val tagsEntryIndex = tagsIndexSubspace.packWithVersionstamp(
                Tuple.from(key, value, Versionstamp.incomplete(), index, factId)
            )
            mutate(SET_VERSIONSTAMPED_KEY, tagsEntryIndex, EMPTY_BYTE_ARRAY)

            val tagTypeIndex = tagsTypeIndexSubspace.packWithVersionstamp(
                Tuple.from(fact.type, key, value, Versionstamp.incomplete(), index, factId)
            )
            mutate(SET_VERSIONSTAMPED_KEY, tagTypeIndex, EMPTY_BYTE_ARRAY)
        }
    }


    internal fun ReadTransaction.loadFact(factId: UUID): CompletableFuture<FdbFact?> {
        val factIdTuple = Tuple.from(factId)
        val typeKey = factTypeSubspace.pack(factIdTuple)
        val createdAtKey = createdAtSubspace.pack(factIdTuple)
        val positionKey = positionSubspace.pack(factIdTuple)
        val payloadKey = factPayloadSubspace.pack(factIdTuple)
        val subjectTypeKey = subjectTypeSubspace.pack(factIdTuple)
        val subjectIdKey = subjectIdSubspace.pack(factIdTuple)
        val metadataKey = metadataSubspace.range(factIdTuple)
        val tagsRange = tagsSubspace.range(factIdTuple)

        val typeFuture = this[typeKey]
        val createdAtFuture = this[createdAtKey]
        val positionKeyFuture = this[positionKey]
        val payloadFuture = this[payloadKey]
        val subjectTypeFuture = this[subjectTypeKey]
        val subjectIdFuture = this[subjectIdKey]
        val metadataFuture = this.getRange(metadataKey).asList()
        val tagsFuture = this.getRange(tagsRange).asList()

        return CompletableFuture.allOf(
            typeFuture,
            createdAtFuture,
            positionKeyFuture,
            payloadFuture,
            subjectTypeFuture,
            subjectIdFuture,
            metadataFuture,
            tagsFuture
        ).thenApply {
            val typeBytes = typeFuture.getNow(null) ?: return@thenApply null
            val createdAtBytes = createdAtFuture.getNow(null) ?: return@thenApply null
            val payloadBytes = payloadFuture.getNow(null) ?: return@thenApply null
            val subjectTypeBytes = subjectTypeFuture.getNow(null) ?: return@thenApply null
            val subjectIdBytes = subjectIdFuture.getNow(null) ?: return@thenApply null
            val createdAtTuple = Tuple.fromBytes(createdAtBytes)
            val createdAtInstant = Instant.ofEpochSecond(
                createdAtTuple.getLong(0),
                createdAtTuple.getLong(1)
            )

            val positionBytes = positionKeyFuture.getNow(null) ?: return@thenApply null
            val positionTuple = Tuple.fromBytes(positionBytes)

            val metadata: Map<String, String> = metadataFuture.getNow(emptyList()).associate { kv ->
                val tuple = Tuple.fromBytes(kv.key)
                val key = tuple.getString(3)
                val value = kv.value.toString(UTF_8)
                key to value
            }

            val tags: Map<String, String> = tagsFuture.getNow(emptyList()).associate { kv ->
                val tuple = Tuple.fromBytes(kv.key)
                val key = tuple.getString(tuple.size() - 1) // /fact-store/tags/{factId}/{key}
                val value = kv.value.toString(UTF_8)
                key to value
            }

            val fact = Fact(
                id = factId,
                type = typeBytes.toString(UTF_8),
                payload = payloadBytes,
                createdAt = createdAtInstant,
                subject = Subject(
                    type = subjectTypeBytes.toString(UTF_8),
                    id = subjectIdBytes.toString(UTF_8)
                ),
                metadata = metadata,
                tags = tags,
            )

            FdbFact(
                fact = fact,
                positionTuple = positionTuple
            )
        }
    }

    fun UUID.getPosition(transaction: ReadTransaction): CompletableFuture<Pair<Versionstamp, Long>> =
        transaction[positionSubspace.pack(Tuple.from(this))].thenApply {
            it?.let { bytes ->
                val positionTuple = Tuple.fromBytes(bytes)
                Pair(positionTuple.getVersionstamp(0), positionTuple.getLong(1))
            } ?: throw RuntimeException("Fact does not exist!")
        }

}

// utils

fun Tuple.getLastAsUuid(): UUID = getUUID(size() -1)


