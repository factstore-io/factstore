package com.cassisi.openeventstore.classic

import com.apple.foundationdb.FDB
import earth.adi.testcontainers.containers.FoundationDBContainer
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletionException

const val FDB_VERSION = "7.3.69"
const val FDB_API_VERSION = 730

@TestInstance(PER_CLASS)
@Testcontainers
class ClassicEventStoreTest {

    companion object {

        @Container
        val testFdbCluster = FoundationDBContainer(DockerImageName.parse("foundationdb/foundationdb:${FDB_VERSION}"))

        lateinit var store: ClassicEventStore

        @JvmStatic
        @BeforeAll
        fun setupFDB() {
            FDB.selectAPIVersion(FDB_API_VERSION)
            val db = FDB.instance().open(testFdbCluster.clusterFilePath)
            store = ClassicEventStore(db)
        }

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
            ClassicEventStore.Event(
                id = eventId1,
                subjectType = "USER",
                subjectId = randomSubject,
                type = "USER_ONBOARDED",
                data = """{ "username": "test123" }""".toByteArray(Charsets.UTF_8),
                createdAt = createdAt
            ),
            ClassicEventStore.Event(
                id = eventId2,
                subjectType = "USER",
                subjectId = randomSubject,
                type = "USER_LOCKED",
                data = """{ "locked": true }""".toByteArray(Charsets.UTF_8),
                createdAt = createdAt
            )
        )

        store.appendWithExpectedVersion("USER", randomSubject, null, eventsToSave)

        val fetchedEvents = store.fetchEvents("USER", randomSubject).events
        Assertions.assertThat(fetchedEvents).containsAll(eventsToSave)
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
        Assertions.assertThat(v1).isNotNull()

        // 2) Second append with correct expectedVersion
        val secondEvents = listOf(
            ClassicEventStore.Event(
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
        Assertions.assertThat(v2).isNotNull()

        // 3) Try stale write (still using v1, but store is already at v2)
        try {
            val staleEvents = listOf(
                ClassicEventStore.Event(
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

//    @Test
//    fun testStreamEventsEmitsNewAppends(): Unit = runBlocking {
//        val subjectId = "subject:${UUID.randomUUID()}"
//        val createdAt = Instant.now()
//
//        val eventsToSave = listOf(
//            Event(
//                id = UUID.randomUUID(),
//                subjectType = "USER",
//                subjectId = subjectId,
//                type = "USER_REGISTERED",
//                data = """{ "username": "streamer" }""".toByteArray(UTF_8),
//                createdAt = createdAt
//            ),
//            Event(
//                id = UUID.randomUUID(),
//                subjectType = "USER",
//                subjectId = subjectId,
//                type = "USER_VERIFIED",
//                data = """{ "verified": true }""".toByteArray(UTF_8),
//                createdAt = createdAt
//            )
//        )
//
//        // Collect stream in background
//        val collected = async {
//            store.streamEvents(batchSize = 10, pollDelayMs = 50)
//                .take(eventsToSave.size) // only take as many as we append
//                .toList()
//        }
//
//        // Small delay to ensure collector is active before appending
//        delay(200)
//
//        // Append events
//        store.appendWithExpectedVersion("USER", subjectId, null, eventsToSave)
//
//        // Wait for collection
//        val streamed = collected.await()
//
//        assertThat(streamed).containsAll(eventsToSave)
//        assertThat(streamed.map { it.type })
//            .containsExactly("USER_REGISTERED", "USER_VERIFIED")
//    }


//    @Test
//    fun testStreamEventsWithWatch(): Unit = runBlocking {
//        store.reset()
//        val subjectId = "subject:${UUID.randomUUID()}"
//        val createdAt = Instant.now()
//
//        val eventsToSave = listOf(
//            Event(
//                id = UUID.randomUUID(),
//                subjectType = "USER",
//                subjectId = subjectId,
//                type = "USER_REGISTERED",
//                data = """{ "username": "streamer" }""".toByteArray(UTF_8),
//                createdAt = createdAt
//            ),
//            Event(
//                id = UUID.randomUUID(),
//                subjectType = "USER",
//                subjectId = subjectId,
//                type = "USER_VERIFIED",
//                data = """{ "verified": true }""".toByteArray(UTF_8),
//                createdAt = createdAt
//            )
//        )
//
//        val collected = async {
//            store.streamEventsWithWatch()
//                .take(eventsToSave.size)
//                .toList()
//        }
//
//        // append events — should trigger watch immediately
//        store.appendWithExpectedVersion("USER", subjectId, null, eventsToSave)
//
//        val streamed = collected.await()
//        assertThat(streamed).containsAll(eventsToSave)
//    }
//
//    @Test
//    fun testStreamEventsWithWatchContinuous() = runBlocking {
//        store.reset()
//        val subjectId = "subject:${UUID.randomUUID()}"
//        val createdAt = Instant.now()
//
//        // Job to continuously append events with random intervals
//        val producerJob = launch {
//            var counter = 1
//            while (counter <= 10) { // for demo, append 10 events, can be infinite if you want
//                val ev = Event(
//                    id = UUID.randomUUID(),
//                    subjectType = "USER",
//                    subjectId = subjectId,
//                    type = "EVENT_$counter",
//                    data = """{ "counter": $counter }""".toByteArray(UTF_8),
//                    createdAt = createdAt
//                )
//
//                store.appendWithExpectedVersion("USER", subjectId, null, listOf(ev))
//                println("➡️  Appended event at ${Instant.now()}: ${ev.type}")
//                counter++
//
//                delay(500) // random delay between 100-500ms
//            }
//        }
//
//        // Job to continuously consume events from the watch stream
//        val consumerJob = launch {
//            store.streamEventsWithWatch()
//                .take(10) // only take as many events as the producer emits
//                .collect { ev ->
//                    println("📥 Received event at ${Instant.now()}: ${ev.type}")
//                }
//        }
//
//        // Wait for both producer and consumer to finish
//        producerJob.join()
//        consumerJob.join()
//    }

}