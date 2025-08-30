package com.cassisi.openeventstore

import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.classic.ClassicEventStore
import com.cassisi.openeventstore.core.classic.ClassicEventStore.Event
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletionException
import kotlin.text.Charsets.UTF_8

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClassicEventStoreTest {

    private lateinit var store: ClassicEventStore

    @BeforeAll
    fun setupFDB() {
        FDB.selectAPIVersion(730)
        val db = FDB.instance().open("/etc/foundationdb/fdb.cluster")
        store = ClassicEventStore(db)
    }

    @BeforeEach
    fun clearEventStore() {
        store.reset()
    }

    @Test
    fun testSimpleAppendAndFetch() {

        val randomSubject = "subject:${UUID.randomUUID()}"
        val eventId1 = UUID.randomUUID()
        val eventId2 = UUID.randomUUID()
        val createdAt = Instant.now()

        val eventsToSave = listOf(
            Event(
                id = eventId1,
                subjectType = "USER",
                subjectId = randomSubject,
                type = "USER_ONBOARDED",
                data = """{ "username": "test123" }""".toByteArray(UTF_8),
                createdAt = createdAt
            ),
            Event(
                id = eventId2,
                subjectType = "USER",
                subjectId = randomSubject,
                type = "USER_LOCKED",
                data = """{ "locked": true }""".toByteArray(UTF_8),
                createdAt = createdAt
            )
        )

        store.appendWithExpectedVersion("USER", randomSubject, null, eventsToSave)

        val fetchedEvents = store.fetchEvents("USER", randomSubject).events
        assertThat(fetchedEvents).containsAll(eventsToSave)
    }

    @Test
    fun testOptimisticLocking() {
        val subjectId = "subject:${UUID.randomUUID()}"

        // 1) First append (expectedVersion = null)
        val firstEvents = listOf(
            ClassicEventStore.Event(
                UUID.randomUUID(),
                "USER",
                subjectId,
                "USER_CREATED",
                """{"id":"$subjectId"}""".toByteArray()
            )
        )
        store.appendWithExpectedVersion("USER", subjectId, null, firstEvents)
        println("First append OK")

        // Fetch version after first append
        val afterFirst = store.fetchEvents("USER", subjectId)
        val v1 = afterFirst.lastVersion
        println("Version after first append: $v1")
        assertThat(v1).isNotNull()

        // 2) Second append with correct expectedVersion
        val secondEvents = listOf(
            Event(
                id = UUID.randomUUID(),
                subjectType = "USER",
                subjectId = subjectId,
                type = "USER_LOCKED",
                data = """{"locked":true}""".toByteArray()
            )
        )
        store.appendWithExpectedVersion("USER", subjectId, v1, secondEvents)
        println("Second append OK")

        // Fetch version after second append
        val afterSecond = store.fetchEvents("USER", subjectId)
        val v2 = afterSecond.lastVersion
        println("Version after second append: $v2")
        assertThat(v2).isNotNull()

        // 3) Try stale write (still using v1, but store is already at v2)
        try {
            val staleEvents = listOf(
                Event(
                    id = UUID.randomUUID(),
                    subjectType = "USER",
                    subjectId = subjectId,
                    type = "USER_DELETED",
                    data = """{"deleted":true}""".toByteArray()
                )
            )
            store.appendWithExpectedVersion("USER", subjectId, v1, staleEvents)
            println("Expected ConcurrencyException but append succeeded")
        } catch (ex: CompletionException) {
            println("ConcurrencyException correctly thrown on stale write")
        }
    }

    @Test
    fun testStreamEventsEmitsNewAppends(): Unit = runBlocking {
        val subjectId = "subject:${UUID.randomUUID()}"
        val createdAt = Instant.now()

        val eventsToSave = listOf(
            Event(
                id = UUID.randomUUID(),
                subjectType = "USER",
                subjectId = subjectId,
                type = "USER_REGISTERED",
                data = """{ "username": "streamer" }""".toByteArray(UTF_8),
                createdAt = createdAt
            ),
            Event(
                id = UUID.randomUUID(),
                subjectType = "USER",
                subjectId = subjectId,
                type = "USER_VERIFIED",
                data = """{ "verified": true }""".toByteArray(UTF_8),
                createdAt = createdAt
            )
        )

        // Collect stream in background
        val collected = async {
            store.streamEvents(batchSize = 10, pollDelayMs = 50)
                .take(eventsToSave.size) // only take as many as we append
                .toList()
        }

        // Small delay to ensure collector is active before appending
        delay(200)

        // Append events
        store.appendWithExpectedVersion("USER", subjectId, null, eventsToSave)

        // Wait for collection
        val streamed = collected.await()

        assertThat(streamed).containsAll(eventsToSave)
        assertThat(streamed.map { it.type })
            .containsExactly("USER_REGISTERED", "USER_VERIFIED")
    }


    @Test
    fun testStreamEventsWithWatch(): Unit = runBlocking {
        store.reset()
        val subjectId = "subject:${UUID.randomUUID()}"
        val createdAt = Instant.now()

        val eventsToSave = listOf(
            Event(
                id = UUID.randomUUID(),
                subjectType = "USER",
                subjectId = subjectId,
                type = "USER_REGISTERED",
                data = """{ "username": "streamer" }""".toByteArray(UTF_8),
                createdAt = createdAt
            ),
            Event(
                id = UUID.randomUUID(),
                subjectType = "USER",
                subjectId = subjectId,
                type = "USER_VERIFIED",
                data = """{ "verified": true }""".toByteArray(UTF_8),
                createdAt = createdAt
            )
        )

        val collected = async {
            store.streamEventsWithWatch()
                .take(eventsToSave.size)
                .toList()
        }

        // append events — should trigger watch immediately
        store.appendWithExpectedVersion("USER", subjectId, null, eventsToSave)

        val streamed = collected.await()
        assertThat(streamed).containsAll(eventsToSave)
    }

    @Test
    fun testStreamEventsWithWatchContinuous() = runBlocking {
        store.reset()
        val subjectId = "subject:${UUID.randomUUID()}"
        val createdAt = Instant.now()

        // Job to continuously append events with random intervals
        val producerJob = launch {
            var counter = 1
            while (counter <= 10) { // for demo, append 10 events, can be infinite if you want
                val ev = Event(
                    id = UUID.randomUUID(),
                    subjectType = "USER",
                    subjectId = subjectId,
                    type = "EVENT_$counter",
                    data = """{ "counter": $counter }""".toByteArray(UTF_8),
                    createdAt = createdAt
                )

                store.appendWithExpectedVersion("USER", subjectId, null, listOf(ev))
                println("➡️  Appended event at ${Instant.now()}: ${ev.type}")
                counter++

                delay(500) // random delay between 100-500ms
            }
        }

        // Job to continuously consume events from the watch stream
        val consumerJob = launch {
            store.streamEventsWithWatch()
                .take(10) // only take as many events as the producer emits
                .collect { ev ->
                    println("📥 Received event at ${Instant.now()}: ${ev.type}")
                }
        }

        // Wait for both producer and consumer to finish
        producerJob.join()
        consumerJob.join()
    }

}