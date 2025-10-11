package com.cassisi.openeventstore.core.dcb

import com.apple.foundationdb.FDB
import com.cassisi.openeventstore.core.dcb.fdb.*
import com.github.avrokotlin.avro4k.Avro
import com.github.avrokotlin.avro4k.schema
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.timeout
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.util.*
import kotlin.time.Duration.Companion.seconds

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
            factStreamer = FdbFactStreamer(fdbFactStore),
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

    @Test
    fun storeAvroType(): Unit = runBlocking {
        val objectToSerialize = SomethingHappened("Hello", 5)
        val serializedObject = Avro.encodeToByteArray(objectToSerialize)
        val deserializedObject = Avro.decodeFromByteArray<SomethingHappened>(serializedObject)

        assertThat(deserializedObject).isEqualTo(objectToSerialize)

        val schema = Avro.schema<SomethingHappened>()
        println(schema)
    }

    @Test
    fun testAvroFdbStore(): Unit = runBlocking {

        val avroStore = AvroFdbStore(store)

        FactRegistry.register(createAvroFactDescriptor<UserOnboarded>("USER_ONBOARDED"))
        FactRegistry.register(createAvroFactDescriptor<UsernameChanged>("USERNAME_CHANGED"))


        val userId = UUID.randomUUID()

        avroStore.append(
            UserOnboarded(
                userId = userId,
                username = "domenic",
                onboardedAt = Instant.now()
            )
        )

        avroStore.append(
            UsernameChanged(
                userId = userId,
                username = "domenic2",
                onboardedAt = Instant.now()
            )
        )

        avroStore.readSubject("USER", userId.toString()).forEach {
            println("${it!!::class.simpleName} $it")
        }
    }

    @Serializable
    data class SomethingHappened(
        val text: String,
        val number: Int
    )

    @OptIn(FlowPreview::class)
    @Test
    fun testFactStreaming(): Unit = runBlocking {
        // test global fact streaming

        launch {
            store.streamAll().timeout(5.seconds).collect {
                println(it)
            }
        }


        println("launching...")
        launch {

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

            println("appending...")
            store.append(fact1)
            delay(1000)
            store.append(fact2)
            store.append(fact3)
            delay(1000)
        }
    }
}