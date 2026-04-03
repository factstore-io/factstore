package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_VALUE
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import io.factstore.core.*
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture

const val FACT_STORE = "fact-store"
const val DEFAULT_FACT_STORE_NAME = "default"

const val HEAD_INDEX = 100
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
 *  /fact-store/head = {vs}
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
data class FdbFactStore(
    val db: Database,
    val context: FdbFactStoreContext,
) {

    context(transaction: Transaction, factStoreId: FactStoreId)
    fun List<Fact>.store() {
        forEachIndexed { index, fact ->
            fact.store(index)
        }
    }

    context(transaction: Transaction, factStoreId: FactStoreId)
    fun Fact.store(index: Int = DEFAULT_INDEX) {
        this.storeFact(index)
        this.storeIndexes(index)
    }

    context(transaction: Transaction, factStoreId: FactStoreId)
    private fun Fact.storeFact(index: Int) {
        val factIdTuple = Tuple.from(id.uuid).pack()
        val incompleteVersionstamp = Versionstamp.incomplete(index)

        // store fact itself
        val serializedFactBytes = toSerializableFdbFact().encodeToByteArray()
        val globalPositionKey = context.globalFactPositionSubspace.packWithVersionstamp(
            Tuple.from(factStoreId.uuid, incompleteVersionstamp)
        )
        transaction.mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, serializedFactBytes)

        // store fact position (we use versionstamp for that)
        val positionKey = context.factPositionsSubspace.pack(Tuple.from(factStoreId.uuid, id.uuid))
        val positionValue = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        transaction.mutate(SET_VERSIONSTAMPED_VALUE, positionKey, positionValue)
    }

    context(transaction: Transaction, factStoreId: FactStoreId)
    private fun Fact.storeIndexes(index: Int) {
        val factIdTuple = Tuple.from(id.uuid).pack()
        val incompleteVersionstamp = Versionstamp.incomplete(index)

        val headKey = context.headSubspace.pack(factStoreId.uuid)
        val positionValue = Tuple.from(incompleteVersionstamp).packWithVersionstamp()
        transaction.mutate(SET_VERSIONSTAMPED_VALUE, headKey, positionValue)

        val eventTypeIndexKey = context.eventTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(factStoreId.uuid, type.value, incompleteVersionstamp)
        )
        transaction.mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, factIdTuple)

        val createdAtIndexKey = context.createdAtIndexSubspace.packWithVersionstamp(
            Tuple.from(factStoreId.uuid, appendedAt.epochSecond, appendedAt.nano, incompleteVersionstamp)
        )
        transaction.mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, factIdTuple)

        val subjectIndex = context.subjectIndexSubspace.packWithVersionstamp(
            Tuple.from(factStoreId.uuid, subjectRef.type, subjectRef.id, incompleteVersionstamp)
        )
        transaction.mutate(SET_VERSIONSTAMPED_KEY, subjectIndex, factIdTuple)

        metadata.forEach { (key, value) ->
            val metadataEntryIndex = context.metadataIndexSubspace.packWithVersionstamp(
                Tuple.from(factStoreId.uuid, key, value, incompleteVersionstamp)
            )
            transaction.mutate(SET_VERSIONSTAMPED_KEY, metadataEntryIndex, factIdTuple)
        }

        tags.forEach { (key, value) ->
            val tagsEntryIndex = context.tagsIndexSubspace.packWithVersionstamp(
                Tuple.from(factStoreId.uuid, key.value, value.value, incompleteVersionstamp)
            )
            transaction.mutate(SET_VERSIONSTAMPED_KEY, tagsEntryIndex, factIdTuple)

            val tagTypeIndex = context.tagsTypeIndexSubspace.packWithVersionstamp(
                Tuple.from(factStoreId.uuid, type.value, key.value, value.value, incompleteVersionstamp)
            )
            transaction.mutate(SET_VERSIONSTAMPED_KEY, tagTypeIndex, factIdTuple)
        }
    }

    context(transaction: ReadTransaction, factStoreId: FactStoreId)
    fun FactPosition.loadFactByPosition(): CompletableFuture<FdbFact?> {
        val positionTuple = Tuple.from(factStoreId.uuid, this)
        return transaction[context.globalFactPositionSubspace.pack(positionTuple)].thenApply { factBytes ->
            if (factBytes == null) {
                return@thenApply null
            }

            val fact = factBytes.toSerializableFdbFact().toFact()
            FdbFact(
                fact = fact,
                factPosition = this
            )
        }
    }

    context(transaction: ReadTransaction, factStoreId: FactStoreId)
    fun FactId.loadFactById(): CompletableFuture<FdbFact?> {
        val factIdTuple = Tuple.from(factStoreId.uuid, this.uuid)
        val factPositionKey = context.factPositionsSubspace.pack(factIdTuple)

        // fetch fact data and position in parallel
        val factPositionFuture = transaction[factPositionKey].thenCompose { factPosition ->
            if (factPosition == null) {
                return@thenCompose CompletableFuture.completedFuture(null)
            }

            val positionTuple = Tuple.fromBytes(factPosition)
            val factPosition = positionTuple.getLastAsFactPosition()

            // now that we have the position,
            // we can look up the fact
            val globalPositionKey = context.globalFactPositionSubspace.pack(Tuple.from(factStoreId.uuid, factPosition))

            transaction[globalPositionKey].thenApply { factBytes ->
                if (factBytes == null) {
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

    fun FactId.getPosition(factStoreId: FactStoreId, transaction: ReadTransaction): CompletableFuture<FactPosition?> =
        transaction[context.factPositionsSubspace.pack(Tuple.from(factStoreId.uuid, this.uuid))].thenApply {
            it?.let { bytes ->
                val positionTuple = Tuple.fromBytes(bytes)
                positionTuple.getVersionstamp(0)
            }
        }

    fun getHead(factStoreId: FactStoreId, transaction: ReadTransaction): CompletableFuture<FactPosition?> =
        transaction[context.headSubspace.pack(factStoreId.uuid)].thenApply { bytes ->
            bytes?.let {
                val positionTuple = Tuple.fromBytes(it)
                positionTuple.getVersionstamp(0)
            }
        }

}

// utils

typealias FactPosition = Versionstamp

fun Tuple.getFirstAsFactId(): FactId = getUUID(0).toFactId()
fun Tuple.getLastAsFactPosition(): FactPosition = getVersionstamp(size() - 1)
fun Tuple.getLastAsUuid(): UUID = getUUID(size() - 1)

fun Fact.toSerializableFdbFact() = SerializableFdbFact(
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

fun SerializableFdbFact.encodeToByteArray() = Avro.encodeToByteArray(this)

fun ByteArray.toSerializableFdbFact() = Avro.decodeFromByteArray<SerializableFdbFact>(this)

fun SerializableFdbFact.toFact() = Fact(
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

fun FactId.toTuple(): Tuple = Tuple.from(this.uuid)
