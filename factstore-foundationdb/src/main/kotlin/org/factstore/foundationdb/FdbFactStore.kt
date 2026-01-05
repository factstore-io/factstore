package org.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_VALUE
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import org.factstore.core.*
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture

const val FACT_STORE = "fact-store"
const val DEFAULT_FACT_STORE_NAME = "default"

const val GLOBAL_FACT_POSITION_INDEX = 100
const val CREATED_AT_INDEX = 101
const val EVENT_TYPE_INDEX = 102
const val SUBJECT_INDEX = 103
const val METADATA_INDEX = 104
const val TAGS_INDEX = 105
const val TAGS_TYPE_INDEX = 106

const val IDEMPOTENCY_KEYS = 200

val EMPTY_BYTE_ARRAY = ByteArray(0)
const val DEFAULT_INDEX = 0

const val FACTS = 1
const val FACT_POSITIONS = 2

/**
 * A simple event/fact store implementation based on FoundationDB.
 *
 * FACT SPACES:
 *  /fact-store/facts/{factId} = serialized fact
 *  /fact-store/fact-positions/{factId} = fact position tuple (versionstamp+index)
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
    internal val db: Database,
    internal val name: String = DEFAULT_FACT_STORE_NAME
) {


    // DIRECTORIES
    internal val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(FACT_STORE, name)).get()

    // FACT SPACES
    internal val factsSubspace = root.subspace(Tuple.from(FACTS))
    internal val factPositionsSubspace = root.subspace(Tuple.from(FACT_POSITIONS))

    // INDEX SPACES
    internal val globalFactPositionSubspace = root.subspace(Tuple.from(GLOBAL_FACT_POSITION_INDEX))
    internal val eventTypeIndexSubspace = root.subspace(Tuple.from(EVENT_TYPE_INDEX))
    internal val createdAtIndexSubspace = root.subspace(Tuple.from(CREATED_AT_INDEX))
    internal val subjectIndexSubspace = root.subspace(Tuple.from(SUBJECT_INDEX))
    internal val metadataIndexSubspace = root.subspace(Tuple.from(METADATA_INDEX))
    internal val tagsIndexSubspace = root.subspace(Tuple.from(TAGS_INDEX))
    internal val tagsTypeIndexSubspace = root.subspace(Tuple.from(TAGS_TYPE_INDEX))

    // IDEMPOTENCY SPACES
    internal val idempotencySubspace = root.subspace(Tuple.from(IDEMPOTENCY_KEYS))


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
        val factIdTuple = fact.id.toTuple()

        // store fact itself
        val serializedFactBytes = fact.toSerializableFdbFact().encodeToByteArray()
        this[factsSubspace.pack(factIdTuple)] = serializedFactBytes

        // store fact position (we use versionstamp+custom index position for that)
        val positionKey = factPositionsSubspace.pack(factIdTuple)
        val positionValue = Tuple.from(Versionstamp.incomplete(), index).packWithVersionstamp()
        mutate(SET_VERSIONSTAMPED_VALUE, positionKey, positionValue)

    }

    private fun Transaction.storeIndexes(fact: Fact, index: Int) {
        val factId = fact.id.uuid

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
            Tuple.from(fact.subjectRef.type, fact.subjectRef.id, Versionstamp.incomplete(), index, factId)
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


    internal fun ReadTransaction.loadFactById(factId: FactId): CompletableFuture<FdbFact?> {
        val factIdTuple = factId.toTuple()
        val factKey = factsSubspace.pack(factIdTuple)
        val factPositionKey = factPositionsSubspace.pack(factIdTuple)

        // fetch fact data and position in parallel
        val factFuture = this[factKey]
        val factPositionFuture = this[factPositionKey]

        // one data is available, build and return FdbFact, or return null if not found
        return CompletableFuture.allOf(factFuture, factPositionFuture).thenApply {
            val factBytes = factFuture.getNow(null) ?: return@thenApply null
            val positionBytes = factPositionFuture.getNow(null) ?: return@thenApply null
            FdbFact(
                fact = factBytes.toSerializableFdbFact().toFact(),
                positionTuple = Tuple.fromBytes(positionBytes)
            )
        }
    }

    internal fun FactId.getPosition(transaction: ReadTransaction): CompletableFuture<Pair<Versionstamp, Long>> =
        transaction[factPositionsSubspace.pack(Tuple.from(this.uuid))].thenApply {
            it?.let { bytes ->
                val positionTuple = Tuple.fromBytes(bytes)
                Pair(positionTuple.getVersionstamp(0), positionTuple.getLong(1))
            } ?: throw RuntimeException("Fact does not exist!")
        }

}

// utils

internal fun Tuple.getLastAsUuid(): UUID = getUUID(size() - 1)

internal fun Tuple.getLastAsFactId(): FactId = getLastAsUuid().toFactId()

internal fun Fact.toSerializableFdbFact() = SerializableFdbFact(
    id = id.uuid,
    type = type,
    subjectType = subjectRef.type,
    subjectId = subjectRef.id,
    timeEpochSeconds = createdAt.epochSecond,
    timeNanos = createdAt.nano,
    metadata = metadata,
    tags = tags,
    payload = payload
)

internal fun SerializableFdbFact.encodeToByteArray() = Avro.encodeToByteArray(this)

internal fun ByteArray.toSerializableFdbFact() = Avro.decodeFromByteArray<SerializableFdbFact>(this)

internal fun SerializableFdbFact.toFact() = Fact(
    id = FactId(id),
    type = type,
    payload = payload,
    subjectRef = SubjectRef(
        type = subjectType,
        id = subjectId
    ),
    createdAt = Instant.ofEpochSecond(timeEpochSeconds, timeNanos.toLong()),
    metadata = metadata,
    tags = tags
)

internal fun FactId.toTuple() = Tuple.from(this.uuid)
