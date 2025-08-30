package com.cassisi.openeventstore.core.classic

import com.apple.foundationdb.Database
import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import java.time.Instant
import java.util.*
import kotlin.text.Charsets.UTF_8

const val EVENT_STORE = "event-store"
const val EVENT_ID = "event-id"
const val EVENT_DATA = "event-data"
const val SUBJECT_TYPE = "subject-type"
const val SUBJECT_ID = "subject"
const val EVENT_TYPE = "event-type"
const val CREATED_AT = "created-at"

const val GLOBAL_EVENT_POSITION = "global"
const val CREATED_AT_INDEX = "created-at-index"
const val EVENT_TYPE_INDEX = "event-type-index"
const val SUBJECT_INDEX = "subjectIdx"

/**
 *
 * A simple event store implementation based on FoundationDB.
 *
 *
 * EVENT KEYS
 * /event-id/{eventId}   = ∅   (existence / deduplication anchor)
 * /event-data/{eventId} = payload
 * /event-type/{eventId} = type
 * /subject-type/{eventId} = subjectType
 * /subject-id/{eventId} = subjectId
 * /created-at/{eventId} = timestamp in UTC
 *
 *
 *
 * INDEXES (PLAN)
 * /global/{versionstamp}/{index}/{eventId} = ∅
 * /subject/{subjectId}/{versionstamp}/{index}/{eventId} = ∅
 * /type/{type}/{versionstamp}/{index}/{eventId} = ∅
 * /created-at-index/{createdAt}/{vs}/{index}/{eventId} = ∅
 *
 */
class ClassicEventStore(
    private val db: Database,
) {

    private val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(EVENT_STORE)).get()

    // EVENT DATA
    private val eventIdSubspace = root.subspace(Tuple.from(EVENT_ID))
    private val eventDataSubspace = root.subspace(Tuple.from(EVENT_DATA))
    private val eventTypeSubspace = root.subspace(Tuple.from(EVENT_TYPE))
    private val subjectTypeSubspace = root.subspace(Tuple.from(SUBJECT_TYPE))
    private val subjectIdSubspace = root.subspace(Tuple.from(SUBJECT_ID))
    private val createdAtSubspace = root.subspace(Tuple.from(CREATED_AT))

    // INDEXES
    private val subjectIndexSubspace = root.subspace(Tuple.from(SUBJECT_INDEX))
    private val createdAtIndexSubspace = root.subspace(Tuple.from(CREATED_AT_INDEX))
    private val eventTypeIndexSubspace = root.subspace(Tuple.from(EVENT_TYPE_INDEX))
    private val globalEventPositionSubspace = root.subspace(Tuple.from(GLOBAL_EVENT_POSITION))


    class ConcurrencyException : RuntimeException("Subject version mismatch")

    fun appendWithExpectedVersion(
        subjectType: String,
        subjectId: String,
        expectedVersion: Pair<Versionstamp, Long>?,
        events: List<Event>
    ) {
        db.run { tr ->
            // Find last version in DB
            val subjectRange = subjectIndexSubspace.range(Tuple.from(subjectType, subjectId))
            val lastKv = tr.getRange(subjectRange, 1, true).asList().get().firstOrNull()
            val currentVersion = lastKv?.let {
                val tup = subjectIndexSubspace.unpack(it.key)
                tup.getVersionstamp(2) to tup.getLong(3)
            }

            if (currentVersion != expectedVersion) {
//                throw ConcurrencyException()
            }

            // safe to append
            events.forEachIndexed { index, event ->
                val eventId = event.id.toString() // stable key (event identifier)

                val idKey = eventIdSubspace.pack(Tuple.from(eventId))
                tr[idKey] = ByteArray(0)

                val dataKey = eventDataSubspace.pack(Tuple.from(eventId))
                tr[dataKey] = event.data

                val typeKey = eventTypeSubspace.pack(Tuple.from(eventId))
                tr[typeKey] = event.type.toByteArray(UTF_8)

                val subjectTypeKey = subjectTypeSubspace.pack(Tuple.from(eventId))
                tr[subjectTypeKey] = event.subjectType.toByteArray(UTF_8)

                val subjectIdKey = subjectIdSubspace.pack(Tuple.from(eventId))
                tr[subjectIdKey] = event.subjectId.toByteArray(UTF_8)

                val createdAtKey = createdAtSubspace.pack(Tuple.from(eventId))
                tr[createdAtKey] = Tuple.from(event.createdAt.toEpochMilli()).pack()

                // BUILD INDEXES

                // subject index
                val subjectKey = subjectIndexSubspace.packWithVersionstamp(
                    Tuple.from(subjectType, subjectId, Versionstamp.incomplete(), index, eventId)
                )
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subjectKey, ByteArray(0))

                // global position index
                val globalPositionKey = globalEventPositionSubspace.packWithVersionstamp(
                    Tuple.from(Versionstamp.incomplete(), index, eventId)
                )
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, globalPositionKey, ByteArray(0))

                // CreatedAt index: query events between time ranges
                val createdAtIndexKey = createdAtIndexSubspace.packWithVersionstamp(
                    Tuple.from(event.createdAt.toEpochMilli(), Versionstamp.incomplete(), index, eventId)
                )
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, createdAtIndexKey, ByteArray(0))

                val eventTypeIndexKex = eventTypeIndexSubspace.packWithVersionstamp(
                    Tuple.from(event.type, Versionstamp.incomplete(), index, eventId)
                )
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, eventTypeIndexKex, ByteArray(0))

                // this makes sure that any append will trigger a change in /event-store/notify
                val notifyKey = root.subspace(Tuple.from("notify")).key
                tr[notifyKey] = Tuple.from(Instant.now().toString().toByteArray(UTF_8)).pack() // using instant as value for now, versionstamp would be better

            }
        }
    }

    fun fetchEvents(subjectType: String, subjectId: String): SubjectEvents {
        return db.read { tr ->
            val subjectRange = subjectIndexSubspace.range(Tuple.from(subjectType, subjectId))
            val kvs = tr.getRange(subjectRange).asList().get()

            val seen = mutableSetOf<UUID>()
            val events = kvs.mapNotNull { kv ->
                val keyTuple = subjectIndexSubspace.unpack(kv.key)
                val eventId = UUID.fromString(keyTuple.getString(4))

                if (eventId in seen) return@mapNotNull null
                seen += eventId

                val eventDataKey = eventDataSubspace.pack(Tuple.from(eventId.toString()))
                val eventData = tr[eventDataKey].join() ?: return@mapNotNull null

                val eventTypeKey = eventTypeSubspace.pack(Tuple.from(eventId.toString()))
                val eventType = tr[eventTypeKey].join() ?: return@mapNotNull null

                val createdAtKey = createdAtSubspace.pack(Tuple.from(eventId.toString()))
                val millis = Tuple.fromBytes(tr[createdAtKey].join()).getLong(0)
                val createdAt = Instant.ofEpochMilli(millis)

                Event(
                    id = eventId,
                    subjectId = subjectId,
                    subjectType = subjectType,
                    type = eventType.toString(UTF_8),
                    data = eventData,
                    createdAt = createdAt,
                )
            }

            val last = kvs.lastOrNull()?.let { kv ->
                val tup = subjectIndexSubspace.unpack(kv.key)
                tup.getVersionstamp(2) to tup.getLong(3)
            }

            SubjectEvents(subjectId, events, last)
        }
    }

    fun fetchAll(): List<Event> {
        val range = globalEventPositionSubspace.range()
        return db.read { tr ->
            val kvs = tr.getRange(range).asList().get()

            val events = kvs.mapNotNull { kv ->

                val keyTuple = globalEventPositionSubspace.unpack(kv.key)
                val eventId = UUID.fromString(keyTuple.getString(2))

                val eventDataKey = eventDataSubspace.pack(Tuple.from(eventId.toString()))
                val eventData = tr[eventDataKey].join() ?: return@mapNotNull null

                val eventTypeKey = eventTypeSubspace.pack(Tuple.from(eventId.toString()))
                val eventType = tr[eventTypeKey].join() ?: return@mapNotNull null

                val subjectIdKey = subjectIdSubspace.pack(Tuple.from(eventId.toString()))
                val subjectId = tr[subjectIdKey].join() ?: return@mapNotNull null

                val subjectTypeKey = subjectTypeSubspace.pack(Tuple.from(eventId.toString()))
                val subjectType = tr[subjectTypeKey].join() ?: return@mapNotNull null

                val createdAtKey = createdAtSubspace.pack(Tuple.from(eventId.toString()))
                val test: ByteArray = tr[createdAtKey].join()
                val millis = Tuple.fromBytes(test).getLong(0)
                val createdAt = Instant.ofEpochMilli(millis)

                Event(
                    id = eventId,
                    subjectType = subjectType.toString(UTF_8),
                    subjectId = subjectId.toString(UTF_8),
                    type = eventType.toString(UTF_8),
                    data = eventData,
                    createdAt = createdAt,
                )
            }
            events
        }

    }

    fun streamEvents(
        batchSize: Int = 128,
        pollDelayMs: Long = 250L
    ): Flow<Event> = flow {
        // Cursor over the global index (key = /global/{vs}/{idx}/{eventId})
        var lastSeenKey: ByteArray? = null
        val globalRange = globalEventPositionSubspace.range()

        while (currentCoroutineContext().isActive) {
            // 1) Read the next batch of global positions after the cursor
            val batch = db.read { tr ->
                val beginSel =
                    if (lastSeenKey == null)
                        KeySelector.firstGreaterOrEqual(globalRange.begin)
                    else
                        KeySelector.firstGreaterThan(lastSeenKey)

                val endSel = KeySelector.firstGreaterOrEqual(globalRange.end)

                tr.getRange(beginSel, endSel, batchSize).asList().get()
            }

            if (batch.isEmpty()) {
                // Nothing new yet; avoid a hot loop
                delay(pollDelayMs)
                continue
            }

            // 2) Materialize Events for the batch (single read tx for efficiency)
            val events = db.read { tr ->
                batch.mapNotNull { kv ->
                    val k = globalEventPositionSubspace.unpack(kv.key)
                    val eventId = UUID.fromString(k.getString(2))

                    val dataKey = eventDataSubspace.pack(Tuple.from(eventId.toString()))
                    val eventData = tr[dataKey].join() ?: return@mapNotNull null

                    val typeKey = eventTypeSubspace.pack(Tuple.from(eventId.toString()))
                    val eventType = tr[typeKey].join() ?: return@mapNotNull null

                    val subjectIdKey = subjectIdSubspace.pack(Tuple.from(eventId.toString()))
                    val subjectId = tr[subjectIdKey].join() ?: return@mapNotNull null

                    val subjectTypeKey = subjectTypeSubspace.pack(Tuple.from(eventId.toString()))
                    val subjectType = tr[subjectTypeKey].join() ?: return@mapNotNull null

                    val createdAtKey = createdAtSubspace.pack(Tuple.from(eventId.toString()))
                    val millis = Tuple.fromBytes(tr[createdAtKey].join()).getLong(0)
                    val createdAt = Instant.ofEpochMilli(millis)

                    Event(
                        id = eventId,
                        subjectType = subjectType.toString(UTF_8),
                        subjectId = subjectId.toString(UTF_8),
                        type = eventType.toString(UTF_8),
                        data = eventData,
                        createdAt = createdAt,
                    )
                }
            }

            // 3) Advance the cursor and emit in order
            for ((i, kv) in batch.withIndex()) {
                lastSeenKey = kv.key
                emit(events[i])
            }
        }
    }

    fun streamEventsWithWatch(batchSize: Int = 128): Flow<Event> = flow {
        var lastSeenKey: ByteArray? = null
        val globalRange = globalEventPositionSubspace.range()
        val notifyKey = root.subspace(Tuple.from("notify")).pack()

        while (currentCoroutineContext().isActive) {
            // 1) Read any new events since lastSeenKey
            val batch = db.read { tr ->
                val beginSel =
                    if (lastSeenKey == null)
                        KeySelector.firstGreaterOrEqual(globalRange.begin)
                    else
                        KeySelector.firstGreaterThan(lastSeenKey)

                val endSel = KeySelector.firstGreaterOrEqual(globalRange.end)
                tr.getRange(beginSel, endSel, batchSize).asList().get()
            }

            if (batch.isNotEmpty()) {
                val events = db.read { tr ->
                    batch.mapNotNull { kv ->
                        val k = globalEventPositionSubspace.unpack(kv.key)
                        val eventId = UUID.fromString(k.getString(2))

                        val dataKey = eventDataSubspace.pack(Tuple.from(eventId.toString()))
                        val eventData = tr[dataKey].join() ?: return@mapNotNull null

                        val typeKey = eventTypeSubspace.pack(Tuple.from(eventId.toString()))
                        val eventType = tr[typeKey].join() ?: return@mapNotNull null

                        val subjectIdKey = subjectIdSubspace.pack(Tuple.from(eventId.toString()))
                        val subjectId = tr[subjectIdKey].join() ?: return@mapNotNull null

                        val subjectTypeKey = subjectTypeSubspace.pack(Tuple.from(eventId.toString()))
                        val subjectType = tr[subjectTypeKey].join() ?: return@mapNotNull null

                        val createdAtKey = createdAtSubspace.pack(Tuple.from(eventId.toString()))
                        val millis = Tuple.fromBytes(tr[createdAtKey].join()).getLong(0)
                        val createdAt = Instant.ofEpochMilli(millis)

                        Event(
                            id = eventId,
                            subjectType = subjectType.toString(UTF_8),
                            subjectId = subjectId.toString(UTF_8),
                            type = eventType.toString(UTF_8),
                            data = eventData,
                            createdAt = createdAt,
                        )
                    }
                }

                for ((i, kv) in batch.withIndex()) {
                    lastSeenKey = kv.key
                    emit(events[i])
                }
            } else {
                // 2) Nothing new — set a watch and block until it fires
                val watchFuture = db.run { tr -> tr.watch(notifyKey) }
                // suspend until the watch completes
                watchFuture.await()
            }
        }
    }


    internal fun reset() {
        db.run { tr ->
            val range = root.range()
            tr.clear(range.begin, range.end)
        }
    }

    data class SubjectEvents(
        val subject: String,
        val events: List<Event>,
        val lastVersion: Pair<Versionstamp, Long>?, // (versionstamp, index)
    )

    data class Event(
        val id: UUID = UUID.randomUUID(),
        val subjectType: String,
        val subjectId: String,
        val type: String,
        val data: ByteArray,
        val createdAt: Instant = Instant.now(),
    ) {
        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Event

            if (id != other.id) return false
            if (subjectType != other.subjectType) return false
            if (subjectId != other.subjectId) return false
            if (type != other.type) return false
            if (!data.contentEquals(other.data)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = id.hashCode()
            result = 31 * result + subjectType.hashCode()
            result = 31 * result + subjectId.hashCode()
            result = 31 * result + type.hashCode()
            result = 31 * result + data.contentHashCode()
            return result
        }

    }
}
