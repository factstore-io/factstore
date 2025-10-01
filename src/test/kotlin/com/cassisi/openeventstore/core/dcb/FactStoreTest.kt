package com.cassisi.openeventstore.core.dcb

import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.dcb.fdb.*
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
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
            factFinder = FdbFactFinder(fdbFactStore),
            conditionalSubjectFactAppender = ConditionalFdbFactAppender(fdbFactStore)
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
        val payload = """ { "username": "Peter" } """.toByteArray()
        val createdAt = Instant.now()

        val fact = Fact(
            id = id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
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
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = now.minusSeconds(60) // 1 minute ago
        )

        val fact2 = Fact(
            id = UUID.randomUUID(),
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_UPDATED",
            payload = """{ "username": "Alice", "status": "active" }""".toByteArray(),
            createdAt = now
        )

        val fact3 = Fact(
            id = UUID.randomUUID(),
            subject = Subject(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_DELETED",
            payload = """{ "username": "Bob" }""".toByteArray(),
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

    @Test
    fun testConditionalAppendWithSubject(): Unit = runBlocking {
        // append first event without an append condition
        val fact1Id = UUID.randomUUID()
        val fact1 = Fact(
            id = fact1Id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        // append fact1
        val firstPreCondition = SubjectAppendCondition(null)
        store.append(fact1, firstPreCondition)


        // append fact2
        val fact2Id = UUID.randomUUID()
        val fact2 = Fact(
            id = fact2Id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val secondPreCondition = SubjectAppendCondition(fact1Id)
        store.append(fact2, secondPreCondition)

        // appending a third fact with the same fact ID in the append condition should fail
        val fact3 = fact2.copy(id = UUID.randomUUID())
        assertThatThrownBy {
            runBlocking { store.append(fact3, SubjectAppendCondition(fact1Id)) }
        }.isNotNull()
    }

    @Test
    fun testMultipleFactsOptimisticAppend(): Unit = runBlocking {

        val fact1Id = UUID.randomUUID()
        val fact2Id = UUID.randomUUID()
        val fact3Id = UUID.randomUUID()

        val fact1 = Fact(
            id = fact1Id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact2 = Fact(
            id = fact2Id,
            subject = Subject(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact3 = Fact(
            id = fact3Id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val factsToAppend = listOf(fact1, fact2, fact3)

        val appendCondition: Map<Pair<String, String>, UUID?> = mapOf(
            Pair("USER", "ALICE") to null,
            Pair("USER", "BOB") to null,
        )
        val batchCondition = MultiSubjectAppendCondition(
            appendCondition
        )
        store.append(factsToAppend, batchCondition)

    }

    @Test
    fun testSubjectQueries(): Unit = runBlocking {

        val fact1Id = UUID.randomUUID()
        val fact2Id = UUID.randomUUID()
        val fact3Id = UUID.randomUUID()

        val fact1 = Fact(
            id = fact1Id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact2 = Fact(
            id = fact2Id,
            subject = Subject(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact3 = Fact(
            id = fact3Id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val factsToAppend = listOf(fact1, fact2, fact3)

        store.append(factsToAppend)

        assertThat(store.findBySubject("USER", "ALICE"))
            .containsExactly(fact1, fact3)

        assertThat(store.findBySubject("USER", "BOB"))
            .containsExactly(fact2)

        assertThat(store.findBySubject("USER", "PETER")).isEmpty()
        assertThat(store.findBySubject("UNKNOWN", "UNKNOWN")).isEmpty()
    }

    @Test
    fun testWithMetadata(): Unit = runBlocking {

        val fact1Id = UUID.randomUUID()
        val fact2Id = UUID.randomUUID()

        val fact1 = Fact(
            id = fact1Id,
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = mapOf("test" to "123", "loc" to "world")
        )

        val fact2 = Fact(
            id = fact2Id,
            subject = Subject(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val factsToAppend = listOf(fact1, fact2)

        store.append(factsToAppend)

        assertThat(store.findById(fact1Id)).isEqualTo(fact1)
        assertThat(store.findById(fact2Id)).isEqualTo(fact2)
    }

    @Test
    fun appendEventsWithTagsAndFindThem(): Unit = runBlocking {
        val fact1 = Fact(
            id = UUID.randomUUID(),
            subject = Subject(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("role" to "admin", "region" to "eu")
        )

        val fact2 = Fact(
            id = UUID.randomUUID(),
            subject = Subject(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Bob" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("role" to "user", "region" to "us")
        )

        val fact3 = Fact(
            id = UUID.randomUUID(),
            subject = Subject(
                type = "USER",
                id = "CHARLIE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Charlie" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("role" to "admin", "region" to "us")
        )

        store.append(listOf(fact1, fact2, fact3))

        // --- Query 1: Find all role=admin (OR semantics → fact1 + fact3)
        val adminFacts = store.findByTags(listOf("role" to "admin"))
        assertThat(adminFacts).containsExactly(fact1, fact3)

        // --- Query 2: Find all region=us (OR semantics → fact2 + fact3)
        val usFacts = store.findByTags(listOf("region" to "us"))
        assertThat(usFacts).containsExactly(fact2, fact3)

        // --- Query 3: Find all role=admin OR region=eu (OR semantics → fact1 + fact3)
        val adminOrEuFacts = store.findByTags(listOf("role" to "admin", "region" to "eu"))
        assertThat(adminOrEuFacts).containsExactly(fact1, fact3)

        // --- Query 4: Non-existent tag → empty
        val noFacts = store.findByTags(listOf("region" to "asia"))
        assertThat(noFacts).isEmpty()

        // --- Query 5: Union of all queries (just to validate coverage)

        val fact1Loaded = store.findById(fact1.id)
        println(fact1Loaded)

        val allFacts = store.findByTags(listOf("role" to "admin", "role" to "user", "region" to "eu", "region" to "us"))
        assertThat(allFacts).containsExactly(fact1, fact2, fact3)
    }

}