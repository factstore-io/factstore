package com.cassisi.openeventstore.core.dcb

import com.apple.foundationdb.Database
import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import kotlinx.coroutines.future.await
import java.time.Instant
import java.util.*
import kotlin.text.Charsets.UTF_8

const val FACT_STORE = "fact-store"
const val FACT_ID = "id"
const val FACT_TYPE = "type"
const val FACT_PAYLOAD = "payload"
const val CREATED_AT = "created-at"

const val GLOBAL_FACT_POSITION_INDEX = "global"
const val CREATED_AT_INDEX = "created-at-index"
const val EVENT_TYPE_INDEX = "type-index"

private val EMPTY_BYTE_ARRAY = ByteArray(0)
private const val DEFAULT_INDEX = 0

/**
 * A simple event/fact store implementation based on FoundationDB.
 *
 * FACT SPACES:
 *  /fact-store/id/{factId} = ∅   (existence / deduplication anchor)
 *  /fact-store/type/{factId} = type
 *  /fact-store/payload/{factId} = payload
 *  /fact-store/created-at/{factId} = timestamp in UTC
 *
 * INDEX SPACES
 *  /fact-store/global/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/type-index/{type}/{versionstamp}/{index}/{factId} = ∅
 *  /fact-store/created-at-index/{epochSecond}/{nano}/{vs}/{index}/{factId} = ∅
 *
 */
class FactStore(
    private val db: Database,
) {

    // DIRECTORIES
    private val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(FACT_STORE)).get()

    // FACT SPACES
    private val factIdSubspace = root.subspace(Tuple.from(FACT_ID))
    private val factTypeSubspace = root.subspace(Tuple.from(FACT_TYPE))
    private val factPayloadSubspace = root.subspace(Tuple.from(FACT_PAYLOAD))
    private val createdAtSubspace = root.subspace(Tuple.from(CREATED_AT))

    // INDEX SPACES
    private val globalFactPositionSubspace = root.subspace(Tuple.from(GLOBAL_FACT_POSITION_INDEX))
    private val eventTypeIndexSubspace = root.subspace(Tuple.from(EVENT_TYPE_INDEX))
    private val createdAtIndexSubspace = root.subspace(Tuple.from(CREATED_AT_INDEX))

    fun append(facts: List<Fact>): List<Fact> {
        db.run { tr -> tr.store(facts) }
        return facts
    }

    fun append(fact: Fact): Fact {
        db.run { tr -> tr.store(fact) }
        return fact
    }

    private fun Transaction.store(facts: List<Fact>) {
        facts.forEachIndexed { index, fact ->
            storeFactData(fact)
            storeIndexes(fact, index)
        }
    }

    private fun Transaction.store(fact: Fact) {
        storeFactData(fact)
        storeIndexes(fact)
    }

    private fun Transaction.storeFactData(fact: Fact) {
        val factId = fact.id // stable key (fact identifier)
        val factIdTuple = Tuple.from(factId)

        // idempotence/existence check
        val idKey = factIdSubspace.pack(factIdTuple)
        check(this[idKey].join() == null) { "Fact with ID $factId already exists!" }

        // store fact ID
        this[idKey] = EMPTY_BYTE_ARRAY

        // store fact type
        val typeKey = factTypeSubspace.pack(factIdTuple)
        this[typeKey] = fact.type.toByteArray(UTF_8)

        // store fact payload
        val factPayloadKey = factPayloadSubspace.pack(factIdTuple)
        this[factPayloadKey] = fact.payload.toByteArray(UTF_8)

        // store createdAt
        val createdAtKey = createdAtSubspace.pack(factIdTuple)
        this[createdAtKey] = Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano).pack()
    }

    private fun Transaction.storeIndexes(fact: Fact, index: Int = DEFAULT_INDEX) {
        val factId = fact.id

        // global position index
        // this will add an index like this:
        // /fact-store/global/{versionstamp}/{index}/{factId} = ∅
        val globalPositionKey = globalFactPositionSubspace.packWithVersionstamp(
            Tuple.from(Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, EMPTY_BYTE_ARRAY)

        // event type index
        // this will add an index like this:
        // /fact-store/type-index/{type}/{versionstamp}/{index}/{factId} = ∅
        val eventTypeIndexKey = eventTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.type, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, EMPTY_BYTE_ARRAY)

        // createdAt index
        // this will add an index like this:
        // /fact-store/created-at-index/{epochSecond}/{nano}/{vs}/{index}/{factId} = ∅
        val createdAtIndexKey = createdAtIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, EMPTY_BYTE_ARRAY)
    }

    fun findById(factId: UUID): Fact? {
        return db.read { tr ->
            val factIdTuple = Tuple.from(factId)
            val factIdKey = factIdSubspace.pack(factIdTuple)
            if (tr[factIdKey].join() != null) {
                val typeKey = factTypeSubspace.pack(factIdTuple)
                val type = tr[typeKey].join()

                val createdAtKey = createdAtSubspace.pack(factIdTuple)
                val createdAtBytes = tr[createdAtKey].join()
                val createdAtTuple = Tuple.fromBytes(createdAtBytes)
                val epochSecond = createdAtTuple.getLong(0)
                val nano = createdAtTuple.getLong(1)
                val createdAt = Instant.ofEpochSecond(epochSecond, nano)

                val payloadKey = factPayloadSubspace.pack(factIdTuple)
                val payload = tr[payloadKey].join()

                Fact(
                    id = factId,
                    type = type.toString(UTF_8),
                    payload = payload.toString(UTF_8),
                    createdAt = createdAt
                )
            } else {
                null
            }
        }
    }

    suspend fun existsById(factId: UUID): Boolean {
        return db.readAsync { tr ->
            val factIdKey = factIdSubspace.pack(Tuple.from(factId))
            tr[factIdKey]
        }.await() != null
    }

    suspend fun findInTimeRange(start: Instant, end: Instant = Instant.now()): List<Fact> {
        val startTuple = Tuple.from(start.epochSecond, start.nano)
        val endTuple = Tuple.from(end.epochSecond, end.nano)

        return db.readAsync { tr ->
            val begin = createdAtIndexSubspace.pack(startTuple)
            val endKey = createdAtIndexSubspace.pack(endTuple)

            tr.getRange(begin, endKey)
                .asList()
                .thenApply { kvs ->
                    kvs.mapNotNull { kv ->
                        val tuple = createdAtIndexSubspace.unpack(kv.key)
                        val factId = tuple.getUUID(tuple.size() - 1)
                        val factIdTuple = Tuple.from(factId)

                        val typeBytes = tr[factTypeSubspace.pack(factIdTuple)].join() ?: return@mapNotNull null
                        val payloadBytes = tr[factPayloadSubspace.pack(factIdTuple)].join() ?: return@mapNotNull null
                        val createdAtBytes = tr[createdAtSubspace.pack(factIdTuple)].join() ?: return@mapNotNull null

                        val createdAtTuple = Tuple.fromBytes(createdAtBytes)
                        val createdAtInstant = Instant.ofEpochSecond(
                            createdAtTuple.getLong(0),
                            createdAtTuple.getLong(1)
                        )

                        Fact(
                            id = factId,
                            type = typeBytes.toString(UTF_8),
                            payload = payloadBytes.toString(UTF_8),
                            createdAt = createdAtInstant
                        )
                    }
                }
        }.await()
    }


    internal fun reset() {
        db.run { tr ->
            val range = root.range()
            tr.clear(range.begin, range.end)
        }
    }
}

data class Fact(
    val id: UUID,
    val type: String,
    val payload: String, // assume JSON
    val createdAt: Instant,
)