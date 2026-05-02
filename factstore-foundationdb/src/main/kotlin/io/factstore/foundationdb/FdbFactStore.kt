package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.github.avrokotlin.avro4k.Avro
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import io.factstore.core.*
import java.time.Instant
import java.util.concurrent.CompletableFuture

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

    context(transaction: Transaction, storeId: StoreId)
    fun List<Fact>.store() {
        forEachIndexed { index, fact ->
            fact.store(index)
        }
    }

    context(transaction: Transaction, storeId: StoreId)
    fun Fact.store(index: Int = DEFAULT_INDEX) {
        this.storeFact(index)
        this.storeIndexes(index)
    }

    context(transaction: Transaction, storeId: StoreId)
    private fun Fact.storeFact(index: Int) {
        val incompleteVersionstamp = Versionstamp.incomplete(index)

        // store fact itself
        val serializedFactBytes = toSerializableFdbFact().encodeToByteArray()
        context.factSubspace.saveFact(storeId, incompleteVersionstamp, serializedFactBytes)

        // store fact position (we use versionstamp for that)
        context.factPositionIndexSubspace.savePosition(storeId, id, incompleteVersionstamp)
    }

    context(transaction: Transaction, storeId: StoreId)
    private fun Fact.storeIndexes(index: Int) {
        val incompleteVersionstamp = Versionstamp.incomplete(index)

        context.headSubspace.save(storeId, incompleteVersionstamp)
        context.eventTypeIndexSubspace.save(storeId, id, type, incompleteVersionstamp)
        context.createdAtIndexSubspace.save(storeId, id, appendedAt, incompleteVersionstamp)
        context.subjectIndexSubspace.save(storeId, id, subjectRef, incompleteVersionstamp)
        context.metadataIndexSubspace.save(storeId, id, metadata, incompleteVersionstamp)
        context.tagsIndexSubspace.save(storeId, id, tags, incompleteVersionstamp)
        context.tagsTypeIndexSubspace.save(storeId, id, type, tags, incompleteVersionstamp)
    }

    context(transaction: ReadTransaction, storeId: StoreId)
    fun FactPosition.loadFactByPosition(): CompletableFuture<FdbFact?> {
        return context.factSubspace.findFact(storeId, this).thenApply { factBytes ->
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

    context(transaction: ReadTransaction, storeId: StoreId)
    fun FactId.loadFactById(): CompletableFuture<FdbFact?> {
        // fetch fact data and position in parallel
        val factPositionFuture = context.factPositionIndexSubspace.getPosition(storeId, this).thenCompose { factPosition ->
            if (factPosition == null) {
                return@thenCompose CompletableFuture.completedFuture(null)
            }

            // now that we have the position,
            // we can look up the fact
            context.factSubspace
                .findFact(storeId, factPosition)
                .thenApply { factBytes ->
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

    fun getHead(storeId: StoreId, transaction: ReadTransaction): CompletableFuture<FactPosition?> =
        with(transaction) {
            context.headSubspace.head(storeId)
        }

}

// utils

typealias FactPosition = Versionstamp

fun Tuple.getFirstAsFactId(): FactId = getUUID(0).toFactId()
fun Tuple.getLastAsFactPosition(): FactPosition = getVersionstamp(size() - 1)

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
