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
import kotlinx.coroutines.future.await

internal const val HEAD_INDEX = 100
internal const val EVENT_TYPE_INDEX = 102
internal const val SUBJECT_INDEX = 103
internal const val TAGS_INDEX = 105
internal const val TAGS_TYPE_INDEX = 106

internal const val IDEMPOTENCY_KEYS = 200

internal val EMPTY_BYTE_ARRAY = ByteArray(0)
private const val DEFAULT_INDEX = 0

internal const val FACTS = 1
internal const val FACT_POSITIONS = 2

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
 *  /fact-store/subject-index/{subjectType}/{subjectId}/{versionstamp} = (factId)
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
        context.subjectIndexSubspace.save(storeId, id, subject, incompleteVersionstamp)
        context.tagsIndexSubspace.save(storeId, id, tags, incompleteVersionstamp)
        context.tagsTypeIndexSubspace.save(storeId, id, type, tags, incompleteVersionstamp)
    }


    /** The fact with this id and its position, or `null` if the store holds no such fact. */
    context(transaction: ReadTransaction, storeId: StoreId)
    suspend fun FactId.loadFactById(): FdbFact? {
        val position = context.factPositionIndexSubspace.getPosition(storeId, this) ?: return null
        val factBytes = context.factSubspace.findFact(storeId, position).await() ?: return null
        return FdbFact(fact = factBytes.toSerializableFdbFact().toFact(), factPosition = position)
    }

    /** The position of the store's newest fact, or `null` if it has none. */
    context(transaction: ReadTransaction)
    suspend fun getHead(storeId: StoreId): FactPosition? = context.headSubspace.head(storeId)

}

// utils

typealias FactPosition = Versionstamp

fun Tuple.getFirstAsFactId(): FactId = getUUID(0).toFactId()
fun Tuple.getLastAsFactPosition(): FactPosition = getVersionstamp(size() - 1)

fun Fact.toSerializableFdbFact() = SerializableFdbFact(
    id = id.uuid,
    type = type.value,
    subject = subject.value,
    timeEpochSeconds = appendedAt.epochSecond,
    timeNanos = appendedAt.nano,
    metadata = metadata.entries.associate { it.key.value to it.value.value },
    tags = tags.entries.associate { it.key.value to it.value.value },
    payload = payload.data,
)

fun SerializableFdbFact.encodeToByteArray() = Avro.encodeToByteArray(this)

fun ByteArray.toSerializableFdbFact() = Avro.decodeFromByteArray<SerializableFdbFact>(this)

fun SerializableFdbFact.toFact() = Fact(
    id = FactId(id),
    type = FactType(type),
    payload = FactPayload(data = payload),
    subject = Subject(subject),
    appendedAt = Instant.ofEpochSecond(timeEpochSeconds, timeNanos.toLong()),
    metadata = metadata.entries.associate { it.key.toMetadataKey() to it.value.toMetadataValue() },
    tags = tags.entries.associate { it.key.toTagKey() to it.value.toTagValue() }
)
