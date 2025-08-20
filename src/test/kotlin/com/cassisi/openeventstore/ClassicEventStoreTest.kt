package com.cassisi.openeventstore

import org.junit.jupiter.api.Assertions.assertEquals
import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.classic.ClassicEventStore
import com.cassisi.openeventstore.core.classic.ClassicEventStore.Event
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.assertNotEquals
import java.util.*
import java.util.concurrent.CompletionException

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ClassicEventStoreTest {

    private lateinit var store: ClassicEventStore

    @BeforeAll
    fun setupFDB() {
        FDB.selectAPIVersion(730)
        val db = FDB.instance().open("/etc/foundationdb/fdb.cluster")
        store = ClassicEventStore(db)
    }

    @Test
    fun `optimistic locking works`() {
        val subjectId = "subject:${UUID.randomUUID()}"

        // 1️⃣ First append with expectedVersion = null
        val firstEvents = listOf(
            Event(subjectId, "USER_CREATED", """{"id":"$subjectId"}""".toByteArray())
        )
        store.appendWithExpectedVersion(subjectId, null, firstEvents)

        val afterFirst = store.fetchEvents(subjectId)
        assertEquals(1, afterFirst.events.size)
        assertNotNull(afterFirst.lastVersion)

        val v1 = afterFirst.lastVersion

        // 2️⃣ Second append with correct expectedVersion
        val secondEvents = listOf(
            Event(subjectId, "USER_LOCKED", """{"locked":true}""".toByteArray())
        )
        store.appendWithExpectedVersion(subjectId, v1, secondEvents)

        val afterSecond = store.fetchEvents(subjectId)
        assertEquals(2, afterSecond.events.size)
        val v2 = afterSecond.lastVersion
        assertNotNull(v2)
        assertNotEquals(v1, v2)

        // 3️⃣ Try stale write (still using v1, but store is at v2 now)
        val staleEvents = listOf(
            Event(subjectId, "USER_DELETED", """{"deleted":true}""".toByteArray())
        )

        val ex = assertThrows<CompletionException> {
            store.appendWithExpectedVersion(subjectId, v1, staleEvents)
        }
        val cause = ex.cause!!
        assertEquals("Subject version mismatch", cause.message)
    }
}