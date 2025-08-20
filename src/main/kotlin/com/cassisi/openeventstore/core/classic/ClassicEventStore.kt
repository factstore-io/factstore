package com.cassisi.openeventstore.core.classic

import com.apple.foundationdb.Database
import com.apple.foundationdb.FDB
import com.apple.foundationdb.MutationType
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import kotlinx.serialization.json.Json
import java.util.*
import kotlin.text.Charsets.UTF_8

const val EVENT_STORE = "event-store"
const val EVENT_DATA = "event-data"
const val SUBJECT = "subject"
const val EVENT_TYPE = "event-type"

class ClassicEventStore(
    private val db: Database,
) {

    private val root = DirectoryLayer.getDefault().createOrOpen(db, listOf(EVENT_STORE)).get()
    private val eventDataSubspace = root.subspace(Tuple.from(EVENT_DATA))
    private val eventTypeSubspace = root.subspace(Tuple.from(EVENT_TYPE))
    private val subjectSubspace = root.subspace(Tuple.from(SUBJECT))

    fun append(events: List<Event>) {
        db.run { tr ->


            // Write new events
            events.forEachIndexed { index, event ->

                // SAVE EVENT DATA

                // save event data
                val eventData = event.data
                val eventKey = eventDataSubspace.packWithVersionstamp(Tuple.from(Versionstamp.incomplete(), index))
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, eventKey, eventData)

                // save event type
                val type = event.type.toByteArray(UTF_8)
                val typeKey = eventTypeSubspace.packWithVersionstamp(Tuple.from(Versionstamp.incomplete(), index))
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, typeKey, type)

                // BUILD INDEXES

                // save subject
                val subject = event.subject
                val subjectKey =
                    subjectSubspace.packWithVersionstamp(Tuple.from(subject, Versionstamp.incomplete(), index))
                tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, subjectKey, ByteArray(0))

            }

        }
    }

    fun fetchEvents(subjectId: String): List<Event> {

        return db.read { tr ->

            val subjectRange = subjectSubspace.range(Tuple.from(subjectId))

            tr.getRange(subjectRange).asList().get().mapNotNull { kv ->
                val keyTuple = subjectSubspace.unpack(kv.key)
                val versionstamp = keyTuple.getVersionstamp(1)
                val index = keyTuple.getLong(2)

                // event-data key
                val eventDataKey = eventDataSubspace.pack(Tuple.from(versionstamp, index))
                val eventData = tr[eventDataKey].join() ?: return@mapNotNull null

                // event type
                val eventTypeKey = eventTypeSubspace.pack(Tuple.from(versionstamp, index))
                val eventType = tr[eventTypeKey].join() ?: return@mapNotNull null

                Event(
                    subject = subjectId,
                    type = eventType.toString(UTF_8),
                    data = eventData
                )

            }

        }

    }

}

data class Event(
    val subject: String,
    val type: String,
    val data: ByteArray,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Event

        if (type != other.type) return false
        if (subject != other.subject) return false
        if (!data.contentEquals(other.data)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + subject.hashCode()
        result = 31 * result + data.contentHashCode()
        return result
    }
}

fun main() {
    FDB.selectAPIVersion(730)
    val fdb = FDB.instance()
    val db = fdb.open("/etc/foundationdb/fdb.cluster")

    val classicEventStore = ClassicEventStore(db)

    val randomSubject = "subject:${UUID.randomUUID()}"

    val eventsToSave = listOf(
        Event(
            subject = randomSubject,
            type = "USER_ONBOARDED",
            data = """{ "username": "test123" }""".toByteArray(UTF_8)
        ),
        Event(
            subject = randomSubject,
            type = "USER_LOCKED",
            data = """{ "locked": true }""".toByteArray(UTF_8)
        )
    )

    classicEventStore.append(eventsToSave)

    classicEventStore.fetchEvents(randomSubject).forEach { println(Json.parseToJsonElement(it.data.toString(UTF_8))) }
}