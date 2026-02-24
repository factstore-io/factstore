package io.factstore.foundationdb

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
import io.factstore.core.*
import java.time.Instant
import java.util.concurrent.CompletableFuture

const val FACT_STORE = "fact-store"
const val DEFAULT_FACT_STORE_NAME = "default"

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
 *  /fact-store/global/{versionstamp} = serialized fact
 *
 * INDEX SPACES
 * ```
 *  /fact-store/fact-position-index/{factId} = fact position tuple (versionstamp)
 *  /fact-store/type-index/{type}/{versionstamp} = (factId)
 *  /fact-store/created-at-index/{epochSecond}/{nano}/{versionstamp} = (factId)
 *  /fact-store/subject-index/{subjectType}/{subjectId}/{versionstamp} = (factId)
 *  /fact-store/metadata-index/{key}/{value}/{versionstamp} = (factId)
 *  /fact-store/tags-index/{key}/{value}/{versionstamp} = (factId)
 *  /fact-store/tags-type-index/{type}/{key}/{value}/{versionstamp} = (factId)
 *  ```
 *
 */
class FdbFactStore(
    internal val db: Database,
    internal val name: String = DEFAULT_FACT_STORE_NAME
) {


    // DIRECTORIES
    internal val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(FACT_STORE, name)).get()

    // FACT SPACES
    internal val globalFactPositionSubspace = root.subspace(Tuple.from(FACTS))

    // INDEX SPACES
    internal val factPositionsSubspace = root.subspace(Tuple.from(FACT_POSITIONS))
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
        val incompleteVersionstamp = Versionstamp.incomplete(index)

        // store fact itself
        val serializedFactBytes = fact.toSerializableFdbFact().encodeToByteArray()
        val globalPositionKey = globalFactPositionSubspace.packWithVersionstamp(
            Tuple.from(incompleteVersionstamp)
        )
        mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, serializedFactBytes)

        // store fact position (we use versionstamp for that)
        val positionKey = factPositionsSubspace.pack(factIdTuple)
        val positionValue = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        mutate(SET_VERSIONSTAMPED_VALUE, positionKey, positionValue)
    }

    private fun Transaction.storeIndexes(fact: Fact, index: Int) {
        val factIdTuple = fact.id.toTuple().pack()
        val incompleteVersionstamp = Versionstamp.incomplete(index)

        val eventTypeIndexKey = eventTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.type.value, incompleteVersionstamp)
        )
        mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, factIdTuple)

        val createdAtIndexKey = createdAtIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.appendedAt.epochSecond, fact.appendedAt.nano, incompleteVersionstamp)
        )
        mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, factIdTuple)

        val subjectIndex = subjectIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.subjectRef.type, fact.subjectRef.id, incompleteVersionstamp)
        )
        mutate(SET_VERSIONSTAMPED_KEY, subjectIndex, factIdTuple)

        fact.metadata.forEach { (key, value) ->
            val metadataEntryIndex = metadataIndexSubspace.packWithVersionstamp(
                Tuple.from(key, value, incompleteVersionstamp)
            )
            mutate(SET_VERSIONSTAMPED_KEY, metadataEntryIndex, factIdTuple)
        }

        fact.tags.forEach { (key, value) ->
            val tagsEntryIndex = tagsIndexSubspace.packWithVersionstamp(
                Tuple.from(key.value, value.value, incompleteVersionstamp)
            )
            mutate(SET_VERSIONSTAMPED_KEY, tagsEntryIndex, factIdTuple)

            val tagTypeIndex = tagsTypeIndexSubspace.packWithVersionstamp(
                Tuple.from(fact.type.value, key.value, value.value, incompleteVersionstamp)
            )
            mutate(SET_VERSIONSTAMPED_KEY, tagTypeIndex, factIdTuple)
        }
    }

    internal fun ReadTransaction.loadFactByPosition(position: FactPosition): CompletableFuture<FdbFact?> {
        val positionTuple = Tuple.from(position)
        return this[globalFactPositionSubspace.pack(positionTuple)].thenApply { factBytes ->
            if (factBytes == null) {
                return@thenApply null
            }

            val fact = factBytes.toSerializableFdbFact().toFact()
            FdbFact(
                fact = fact,
                factPosition = position
            )
        }
    }

    internal fun ReadTransaction.loadFactById(factId: FactId): CompletableFuture<FdbFact?> {
        val factIdTuple = factId.toTuple()
        val factPositionKey = factPositionsSubspace.pack(factIdTuple)

        // fetch fact data and position in parallel
        val factPositionFuture = this[factPositionKey].thenCompose { factPosition ->
            if (factPosition == null) {
                return@thenCompose CompletableFuture.completedFuture(null)
            }

            val positionTuple = Tuple.fromBytes(factPosition)
            val factPosition = positionTuple.getLastAsFactPosition()

            // now that we have the position,
            // we can look up the fact
            val globalPositionKey = globalFactPositionSubspace.pack(positionTuple)

            this[globalPositionKey].thenApply { factBytes ->
                if (positionTuple == null) {
                    return@thenApply null
                }
                FdbFact(
                    fact = factBytes.toSerializableFdbFact().toFact(),
                    factPosition = factPosition
                )
            }

        }

        return factPositionFuture
    }

    internal fun FactId.getPosition(transaction: ReadTransaction): CompletableFuture<FactPosition> =
        transaction[factPositionsSubspace.pack(Tuple.from(this.uuid))].thenApply {
            it?.let { bytes ->
                val positionTuple = Tuple.fromBytes(bytes)
                positionTuple.getVersionstamp(0)
            } ?: throw RuntimeException("Fact does not exist!")
        }

}

// utils

typealias FactPosition = Versionstamp

internal fun Tuple.getFirstAsFactId(): FactId = getUUID(0).toFactId()
internal fun Tuple.getLastAsFactPosition(): FactPosition = getVersionstamp(size() - 1)

internal fun Fact.toSerializableFdbFact() = SerializableFdbFact(
    id = id.uuid,
    type = type.value,
    subjectType = subjectRef.type,
    subjectId = subjectRef.id,
    timeEpochSeconds = appendedAt.epochSecond,
    timeNanos = appendedAt.nano,
    metadata = metadata,
    tags = tags.entries.associate { it.key.value to it.value.value },
    payload = SerializableFactPayload(
        data = payload.data,
        format = payload.format?.value,
        schema = payload.schema?.value
    )
)

internal fun SerializableFdbFact.encodeToByteArray() = Avro.encodeToByteArray(this)

internal fun ByteArray.toSerializableFdbFact() = Avro.decodeFromByteArray<SerializableFdbFact>(this)

internal fun SerializableFdbFact.toFact() = Fact(
    id = FactId(id),
    type = FactType(type),
    payload = FactPayload(
        data = payload.data,
        format = payload.format?.toPayloadFormat(),
        schema = payload.format?.toPayloadSchemaRef()
    ),
    subjectRef = SubjectRef(
        type = subjectType,
        id = subjectId
    ),
    appendedAt = Instant.ofEpochSecond(timeEpochSeconds, timeNanos.toLong()),
    metadata = metadata,
    tags = tags.entries.associate { it.key.toTagKey() to it.value.toTagValue() }
)

internal fun FactId.toTuple() = Tuple.from(this.uuid)
