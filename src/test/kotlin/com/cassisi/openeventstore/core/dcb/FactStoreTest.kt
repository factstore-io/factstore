package com.cassisi.openeventstore.core.dcb

import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactAppender
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactFinder
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactStore
import com.cassisi.openeventstore.core.dcb.fdb.FdbFactStoreResetHelper
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.util.*

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FactStoreTest {

    private lateinit var store: FactStore
    private lateinit var resetHelper: FdbFactStoreResetHelper

    @BeforeAll
    fun setupFDB() {
        FDB.selectAPIVersion(730)
        val db = FDB.instance().open("/etc/foundationdb/fdb.cluster")
        val fdbFactStore = FdbFactStore(db)
        store = FactStore(
            factAppender = FdbFactAppender(fdbFactStore),
            factFinder = FdbFactFinder(fdbFactStore)
        )
        resetHelper = FdbFactStoreResetHelper(fdbFactStore)
    }

    @BeforeEach
    fun clearEventStore() {
        resetHelper.reset()
    }

    @Test
    fun testSimpleAppend(): Unit = runBlocking {
        val id = UUID.randomUUID()
        val payload = """ { "username": "Peter" } """
        val createdAt = Instant.now()

        val fact = Fact(
            id = id,
            type = "USER_ONBOARDED",
            payload = payload,
            createdAt = createdAt
        )

        store.append(fact)

        // validate existence of fact
        assertThat(store.existsById(id)).isTrue()

        // find fact by ID
        val findResult = store.findById(id)
        assertThat(findResult).isNotNull().isEqualTo(fact)
    }

    @Test
    fun testExists(): Unit = runBlocking {
        val nonExistingFactId = UUID.randomUUID()
        assertThat(store.existsById(nonExistingFactId)).isFalse()
    }

    @Test
    fun testFindInTimeRange(): Unit = runBlocking {
        val now = Instant.now()

        val fact1 = Fact(
            id = UUID.randomUUID(),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""",
            createdAt = now.minusSeconds(60) // 1 minute ago
        )

        val fact2 = Fact(
            id = UUID.randomUUID(),
            type = "USER_UPDATED",
            payload = """{ "username": "Alice", "status": "active" }""",
            createdAt = now
        )

        val fact3 = Fact(
            id = UUID.randomUUID(),
            type = "USER_DELETED",
            payload = """{ "username": "Bob" }""",
            createdAt = now.plusSeconds(60) // 1 minute in the future
        )

        // Append all three
        store.append(listOf(fact1, fact2, fact3))

        // Query range covering fact1 + fact2, but excluding fact3
        val results = store.findInTimeRange(
            start = now.minusSeconds(120),
            end = now.plusSeconds(10)
        )

        assertThat(results).containsExactlyInAnyOrder(fact1, fact2)
        assertThat(results).doesNotContain(fact3)
    }

}