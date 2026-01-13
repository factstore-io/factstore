package org.factstore.foundationdb

import com.apple.foundationdb.FDB
import earth.adi.testcontainers.containers.FoundationDBContainer
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.timeout
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.factstore.core.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Instant
import java.util.*
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.seconds

const val FDB_VERSION = "7.3.69"
const val FDB_API_VERSION = 730

@TestInstance(PER_CLASS)
@Testcontainers
class FactStoreTest {

    companion object {

        lateinit var store: FactStore
        lateinit var resetHelper: FdbFactStoreResetHelper
        lateinit var clusterFilePath: String

        @Container
        val testFdbCluster = FoundationDBContainer(DockerImageName.parse("foundationdb/foundationdb:$FDB_VERSION"))

        @JvmStatic
        @BeforeAll
        fun setupFDB() {
            FDB.selectAPIVersion(FDB_API_VERSION)
            clusterFilePath = testFdbCluster.clusterFilePath
            val db = FDB.instance().open(clusterFilePath)
            store = buildFdbFactStore(
                clusterFilePath = testFdbCluster.clusterFilePath,
                name = "integration-test"
            )
            resetHelper = FdbFactStoreResetHelper(db)
        }

    }

    @BeforeEach
    fun clearEventStore() {
        resetHelper.reset()
    }

    @Test
    fun testSimpleAppend(): Unit = runBlocking {
        val id = FactId.generate()
        val payload = """ { "username": "Peter" } """.toByteArray()
        val createdAt = Instant.now()

        val fact = Fact(
            id = id,
            subjectRef = SubjectRef(
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
        val nonExistingFactId = FactId.generate()
        assertThat(store.existsById(nonExistingFactId)).isFalse()
    }

    @Test
    fun testFindInTimeRange(): Unit = runBlocking {
        val now = Instant.now()

        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = now.minusSeconds(60) // 1 minute ago
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_UPDATED",
            payload = """{ "username": "Alice", "status": "active" }""".toByteArray(),
            createdAt = now
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
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
        val fact1Id = FactId.generate()
        val fact1 = Fact(
            id = fact1Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        // append fact1
        val appendRequestWithEmptySubjectCondition = AppendRequest(
            facts = listOf(fact1),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.ExpectedLastFact(
                subjectRef = SubjectRef(
                    type = "USER",
                    id = "ALICE"
                ),
                expectedLastFactId = null
            )
        )

        store.append(appendRequestWithEmptySubjectCondition).also {
            assertThat(it).isInstanceOf(AppendResult.Appended::class.java)
        }


        // append fact2
        val fact2Id = FactId.generate()
        val fact2 = Fact(
            id = fact2Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val appendRequestWithFirstFactSubjectCondition = AppendRequest(
            facts = listOf(fact2),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.ExpectedLastFact(
                subjectRef = SubjectRef(
                    type = "USER",
                    id = "ALICE"
                ),
                expectedLastFactId = fact1Id
            )
        )

        store.append(appendRequestWithFirstFactSubjectCondition).also {
            assertThat(it).isInstanceOf(AppendResult.Appended::class.java)
        }

        // appending a third fact with the same fact ID in the append condition should fail
        // this simulates two concurrent/conflicting append requests
        val fact3 = fact2.copy(id = FactId.generate())
        val appendRequestWithViolatingSubjectCondition = AppendRequest(
            facts = listOf(fact3),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.ExpectedLastFact(
                subjectRef = SubjectRef(
                    type = "USER",
                    id = "ALICE"
                ),
                expectedLastFactId = fact1Id // <-- this will cause the violation
            )
        )

        store.append(appendRequestWithViolatingSubjectCondition).also {
            assertThat(it).isInstanceOf(AppendResult.AppendConditionViolated::class.java)
        }
    }

    @Test
    fun testMultipleFactsOptimisticAppend(): Unit = runBlocking {

        val fact1Id = FactId.generate()
        val fact2Id = FactId.generate()
        val fact3Id = FactId.generate()

        val fact1 = Fact(
            id = fact1Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact2 = Fact(
            id = fact2Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact3 = Fact(
            id = fact3Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val appendRequest = AppendRequest(
            facts = listOf(fact1, fact2, fact3),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.ExpectedMultiSubjectLastFact(
                expectations = mapOf(
                    SubjectRef("USER", "ALICE") to null,
                    SubjectRef("USER", "BOB") to null,
                )
            )
        )

        store.append(appendRequest).also {
            assertThat(it).isInstanceOf(AppendResult.Appended::class.java)
        }

    }

    @Test
    fun testSubjectQueries(): Unit = runBlocking {

        val fact1Id = FactId.generate()
        val fact2Id = FactId.generate()
        val fact3Id = FactId.generate()

        val fact1 = Fact(
            id = fact1Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact2 = Fact(
            id = fact2Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val fact3 = Fact(
            id = fact3Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now()
        )

        val factsToAppend = listOf(fact1, fact2, fact3)

        store.append(factsToAppend)

        assertThat(store.findBySubject(SubjectRef("USER", "ALICE")))
            .containsExactly(fact1, fact3)

        assertThat(store.findBySubject(SubjectRef("USER", "BOB")))
            .containsExactly(fact2)

        assertThat(store.findBySubject(SubjectRef("USER", "PETER"))).isEmpty()
        assertThat(store.findBySubject(SubjectRef("UNKNOWN", "UNKNOWN"))).isEmpty()
    }

    @Test
    fun testWithMetadata(): Unit = runBlocking {

        val fact1Id = FactId.generate()
        val fact2Id = FactId.generate()

        val fact1 = Fact(
            id = fact1Id,
            subjectRef = SubjectRef(
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
            subjectRef = SubjectRef(
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
            id = FactId.generate(),
            subjectRef = SubjectRef(
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
            id = FactId.generate(),
            subjectRef = SubjectRef(
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
            id = FactId.generate(),
            subjectRef = SubjectRef(
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
                id = FactId.generate(),
                subjectRef = SubjectRef(
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
                id = FactId.generate(),
                subjectRef = SubjectRef(
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
                id = FactId.generate(),
                subjectRef = SubjectRef(
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

            // start another stream but start after fact1,
            // so expect to read fact2 and fact3
            val streamedEvents = store.streamAll(StreamingOptionSet(lastSeenId = fact1.id))
                .take(2)
                .toList()

            assertThat(streamedEvents).containsExactly(fact2, fact3)

            // request to stream from a fact that does not exist should throw an error
            val nonExistingFactId = FactId.generate()
            assertThatThrownBy {
                runBlocking { store.streamAll(StreamingOptionSet(lastSeenId = nonExistingFactId)).collect() }
            }
                .isInstanceOf(FactIdNotFoundException::class.java)
                .matches { (it as FactIdNotFoundException).factId == nonExistingFactId }
        }
    }

    @Test
    fun testFindByTagQuery(): Unit = runBlocking {

        // define facts to append

        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "alice", "region" to "eu")
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Bob" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "bob", "region" to "us")
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "CHARLIE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Charlie" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "charlie", "region" to "us")
        )

        store.append(listOf(fact1, fact2, fact3))


        // Test 1: Query with a single tag (username = "bob")
        val bobQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("username" to "bob")
                )
            )
        )

        val bobFacts = store.findByTagQuery(bobQuery)
        assertThat(bobFacts).containsExactly(fact2)

        // Test 2: Query with multiple tags (AND condition: username = "bob" and region = "us")
        val multipleTagsQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("username" to "bob", "region" to "us")
                )
            )
        )

        val multipleTagsFacts = store.findByTagQuery(multipleTagsQuery)
        assertThat(multipleTagsFacts).containsExactly(fact2)

        // Test 3: Query with multiple tags but one does not match (username = "bob" and region = "eu")
        val noMatchQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("username" to "bob", "region" to "eu")
                )
            )
        )

        val noMatchFacts = store.findByTagQuery(noMatchQuery)
        assertThat(noMatchFacts).isEmpty()

        // Test 4: Query with multiple types (USER_CREATED or USER_DELETED)
        val multipleTypesQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED", "USER_DELETED"),
                    tags = listOf("username" to "bob")
                )
            )
        )

        val multipleTypesFacts = store.findByTagQuery(multipleTypesQuery)
        assertThat(multipleTypesFacts).containsExactly(fact2)

        // Test 5: Query with multiple tags and multiple types (AND for tags, OR for types)
        val complexQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED", "USER_DELETED"),
                    tags = listOf("username" to "bob", "region" to "us")
                )
            )
        )

        val complexFacts = store.findByTagQuery(complexQuery)
        assertThat(complexFacts).containsExactly(fact2)

        // Test 6: Query with tags but no matching facts (tags = "username" to "dave")
        val noMatchingTagQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("username" to "dave")
                )
            )
        )

        val noMatchingTagFacts = store.findByTagQuery(noMatchingTagQuery)
        assertThat(noMatchingTagFacts).isEmpty()

        // Test 7: Query with types but no matching facts (types = "USER_DELETED" but no such facts exist)
        val noMatchingTypeQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_DELETED"),
                    tags = listOf("username" to "bob")
                )
            )
        )

        val noMatchingTypeFacts = store.findByTagQuery(noMatchingTypeQuery)
        assertThat(noMatchingTypeFacts).isEmpty()

        // Test 8: Query with tags but no facts that have these tags
        val tagsNoFactsQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("username" to "david", "region" to "asia")
                )
            )
        )

        val tagsNoFacts = store.findByTagQuery(tagsNoFactsQuery)
        assertThat(tagsNoFacts).isEmpty()

        // Test 9: Query for facts with different tags
        val differentTagsQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("username" to "charlie", "region" to "us")
                )
            )
        )

        val differentTagsFacts = store.findByTagQuery(differentTagsQuery)
        assertThat(differentTagsFacts).containsExactly(fact3)

    }

    @Test
    fun testMultipleFactTypesOneQueryItem(): Unit = runBlocking {
        // Create facts with different types
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "alice", "region" to "eu")
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "BOB"),
            type = "USER_UPDATED",
            payload = """{ "username": "Bob" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "bob", "region" to "us")
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "CHARLIE"),
            type = "USER_CREATED",
            payload = """{ "username": "Charlie" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "charlie", "region" to "us")
        )

        store.append(listOf(fact1, fact2, fact3))

        // Query for facts with types "USER_CREATED" or "USER_UPDATED" and with tags "username" = "alice"
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED", "USER_UPDATED"),
                    tags = listOf("username" to "alice")
                )
            )
        )

        val result = store.findByTagQuery(query)

        // Expecting fact1 only (username = alice, type = "USER_CREATED")
        assertThat(result).containsExactly(fact1)
    }

    @Test
    fun testMultipleQueryItems(): Unit = runBlocking {
        // Create facts with different types and tags
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "alice", "region" to "eu")
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "BOB"),
            type = "USER_UPDATED",
            payload = """{ "username": "Bob" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "bob", "region" to "us")
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "CHARLIE"),
            type = "USER_CREATED",
            payload = """{ "username": "Charlie" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "charlie", "region" to "us")
        )

        store.append(listOf(fact1, fact2, fact3))

        // Query for facts with type "USER_CREATED" or "USER_UPDATED", tagged with "username" = "bob"
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED", "USER_UPDATED"),
                    tags = listOf("username" to "bob")
                ),
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("region" to "us")
                )
            )
        )

        val result = store.findByTagQuery(query)

        // Expecting fact2 (username = bob, region = us, type = USER_UPDATED)
        // Expecting fact3 (region = us, type = USER_CREATED, but no username filter for 'bob')
        assertThat(result).containsExactlyInAnyOrder(fact2, fact3)
    }

    @Test
    fun testMixedTypesAndTagsQueryItems(): Unit = runBlocking {
        // Create facts with different types and tags
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "alice", "region" to "eu")
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "BOB"),
            type = "USER_UPDATED",
            payload = """{ "username": "Bob" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "bob", "region" to "us")
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "CHARLIE"),
            type = "USER_CREATED",
            payload = """{ "username": "Charlie" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "charlie", "region" to "us")
        )

        store.append(listOf(fact1, fact2, fact3))

        // Query with multiple query items
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED", "USER_UPDATED"),
                    tags = listOf("username" to "bob", "region" to "us")
                ),
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("region" to "eu", "username" to "alice")
                )
            )
        )

        val result = store.findByTagQuery(query)

        // Expecting fact2 (username = bob, region = us, type = USER_UPDATED)
        // Expecting fact1 (username = alice, region = eu, type = USER_CREATED)
        assertThat(result).containsExactlyInAnyOrder(fact2, fact1)
    }

    @Test
    fun testNoMatchingFacts(): Unit = runBlocking {
        // Create facts with different types and tags
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf("username" to "alice", "region" to "eu")
        )

        store.append(listOf(fact1))

        // Query for facts with a non-matching type and tag
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_UPDATED"),
                    tags = listOf("username" to "bob")
                )
            )
        )

        val result = store.findByTagQuery(query)

        // Expecting no facts because no fact matches the query
        assertThat(result).isEmpty()
    }

    @Test
    fun testLargeDatasetQueryByTags(): Unit = runBlocking {
        // Step 1: Append 10,000 events with varying tags
        val events = (1..10_000).map { index ->
            val tag = if (index % 2 == 0) "user" else "admin" // Use alternating tags
            val region = if (index % 2 == 0) "us" else "eu" // Use alternating regions

            Fact(
                id = FactId.generate(),
                subjectRef = SubjectRef(
                    type = "USER",
                    id = "user-$index"
                ),
                type = "USER_CREATED",
                payload = """{ "username": "user$index" }""".toByteArray(),
                createdAt = Instant.now(),
                metadata = emptyMap(),
                tags = mapOf(
                    "role" to tag,
                    "region" to region
                )
            )
        }

        // Append all events to the store (in chunks to avoid exceeding byte limit in FoundationDB)
        events
            .chunked(500)
            .forEach { store.append(it) }

        // append a few more events
        store.append(
            Fact(
                id = FactId.generate(),
                subjectRef = SubjectRef(
                    type = "USER",
                    id = "user-${FactId.generate()}"
                ),
                type = "USER_CREATED",
                payload = """{ "username": "user" }""".toByteArray(),
                createdAt = Instant.now(),
                metadata = emptyMap(),
                tags = mapOf(
                    "role" to "custom",
                    "region" to "eu"
                )
            )
        )


        // Step 2: Query for only a handful of the events (e.g., 100 events with tag "user" and region "us")
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("role" to "user", "region" to "us")
                )
            )
        )

        val result = store.findByTagQuery(query)


        measureTimeMillis {
            store.findByTagQuery(query)
        }.also { println(it) }

        measureTimeMillis {
            store.findByTagQuery(
                TagQuery(
                    listOf(
                        TagTypeItem(
                            types = listOf("USER_CREATED"),
                            tags = listOf("role" to "custom")
                        )
                    )
                )
            )
        }.also { println(it) }


        // Step 3: Verify that the result only contains facts with the expected tags
        // The number of events matching "role=user" and "region=us" should be around half of 10,000 (i.e., ~5000).
        val expectedCount = events.count { it.tags["role"] == "user" && it.tags["region"] == "us" }
        assertThat(result).hasSize(expectedCount) // Ensure the correct number of matching events
        assertThat(result).allMatch { it.tags["role"] == "user" && it.tags["region"] == "us" } // Ensure correct tags

        // Step 4: Optionally, print out some details to confirm the query works (only if needed)
        println("Found ${result.size} events with 'role=user' and 'region=us'.")
    }


    @Test
    fun testConditionalAppendWithTagQuery(): Unit = runBlocking {

        // append first event without an append condition
        val fact1Id = FactId.generate()
        val fact1 = Fact(
            id = fact1Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "Alice" }""".toByteArray(),
            createdAt = Instant.now(),
            tags = mapOf(
                "user" to "ALICE",
            )
        )


        val tagQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("user" to "ALICE"),
                )
            )
        )

        println("appending $fact1Id")
        store.append(
            AppendRequest(
                facts = listOf(fact1),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.TagQueryBased(
                    failIfEventsMatch = tagQuery,
                    after = null
                )
            )
        ).also { assertThat(it).isInstanceOf(AppendResult.Appended::class.java) }

        val fact2Id = FactId.generate()
        val fact2 = Fact(
            id = fact2Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "ALICE" }""".toByteArray(),
            createdAt = Instant.now(),
            tags = mapOf(
                "user" to "ALICE",
            )
        )

        val appendRequest2 = AppendRequest(
            facts = listOf(fact2),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.TagQueryBased(
                failIfEventsMatch = tagQuery,
                after = fact1Id
            )
        )

        store.append(appendRequest2).also { assertThat(it).isInstanceOf(AppendResult.Appended::class.java) }

        val appendRequest3 = AppendRequest(
            facts = listOf(fact2.copy(id = FactId.generate())),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.TagQueryBased(
                failIfEventsMatch = tagQuery,
                after = null
            )
        )

        store.append(appendRequest3).also {
            assertThat(it).isInstanceOf(AppendResult.AppendConditionViolated::class.java)
        }


        // append another user fact with another tag
        val fact3Id = FactId.generate()
        val fact3 = Fact(
            id = fact3Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now(),
            tags = mapOf(
                "user" to "BOB",
            )
        )

        val tagQuery2 = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED"),
                    tags = listOf("user" to "BOB"),
                )
            )
        )

        println("appending $fact3Id")
        val appendRequest4 = AppendRequest(
            facts = listOf(fact3),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.TagQueryBased(
                failIfEventsMatch = tagQuery2,
                after = null
            )
        )
        store.append(appendRequest4).also { assertThat(it).isInstanceOf(AppendResult.Appended::class.java) }

        val fact4Id = FactId.generate()
        val fact4 = Fact(
            id = fact4Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now(),
            tags = mapOf(
                "user" to "BOB",
            )
        )

        val appendRequestThatShouldAppendFact4 = AppendRequest(
            facts = listOf(fact4),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.TagQueryBased(
                failIfEventsMatch = tagQuery2,
                after = fact3Id
            )
        )

        store.append(appendRequestThatShouldAppendFact4).also {
            assertThat(it).isInstanceOf(AppendResult.Appended::class.java)
        }

    }

    @Test
    fun testIsolationOfFactStoreInstances(): Unit = runBlocking {

        // two instances of fact stores should be treated separately
        // facts belong to one and only one fact store and cannot be "shared"
        // two fact store instances should be treated as two logical database instances
        // even if they share underlying infrastructure, like the same FoundationDB cluster

        val factStore1Name = "factstore1"
        val factStore2Name = "factstore2"

        val factStore1 = buildFdbFactStore(clusterFilePath = clusterFilePath, name = factStore1Name)
        val factStore2 = buildFdbFactStore(clusterFilePath = clusterFilePath, name = factStore2Name)

        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "BOB" }""".toByteArray(),
            createdAt = Instant.now(),
            tags = emptyMap()
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "ALICE" }""".toByteArray(),
            createdAt = Instant.now(),
            tags = emptyMap()
        )

        factStore1.append(fact1)
        factStore2.append(fact2)

        assertThat(factStore1.existsById(fact1.id)).isTrue()
        assertThat(factStore1.existsById(fact2.id)).isFalse()

        assertThat(factStore2.existsById(fact1.id)).isFalse()
        assertThat(factStore2.existsById(fact2.id)).isTrue()

        // instantiating a third instance with the same name as factstore1 should point to the same logical fact store

        val factStore3 = buildFdbFactStore(clusterFilePath = clusterFilePath, name = factStore1Name)
        assertThat(factStore3.existsById(fact1.id)).isTrue()
        assertThat(factStore3.existsById(fact2.id)).isFalse()
    }

    @Test
    fun testNewAppend(): Unit = runBlocking {

        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "DOMI",
            ),
            type = "USER_LOCKED",
            payload = """{ "username": "DOMI" }""".toByteArray(),
            createdAt = Instant.now(),
            tags = emptyMap()
        )
        val idempotencyKey = IdempotencyKey(UUID.randomUUID())
        val appendRequest = AppendRequest(
            facts = listOf(fact1),
            idempotencyKey = idempotencyKey,
            condition = AppendCondition.ExpectedLastFact(
                subjectRef = SubjectRef(
                    type = "USER",
                    id = "DOMI",
                ),
                expectedLastFactId = null
            )
        )

        val appendResult = store.append(appendRequest)

        assertThat(appendResult).isInstanceOf(AppendResult.Appended::class.java)

        // trying to append again should return early

        val appendResult2 = store.append(appendRequest)

        assertThat(appendResult2).isInstanceOf(AppendResult.AlreadyApplied::class.java)


    }

    @Test
    fun testEnforceUniquenessOfFactIds(): Unit = runBlocking {
        val factId = FactId.generate()

        val fact1 = Fact(
            id = factId,
            subjectRef = SubjectRef(
                type = "TEST_TYPE",
                id = "TEST_ID",
            ),
            type = "TEST_FACT_TYPE",
            payload = """DATA""".toByteArray(),
            createdAt = Instant.now(),
            tags = emptyMap()
        )

        val fact2 = fact1.copy()

        store.append(fact1)

        assertDuplicateFactIds(
            expected = listOf(factId)
        ) {
            store.append(fact2)
        }

        assertDuplicateFactIds(
            expected = listOf(factId)
        ) {
            store.append(listOf(fact2))
        }

        assertDuplicateFactIds(
            expected = listOf(factId)
        ) {
            store.append(
                AppendRequest(
                    facts = listOf(fact2),
                    idempotencyKey = IdempotencyKey(),
                    condition = AppendCondition.None
                )
            )
        }
    }

    private fun assertDuplicateFactIds(
        expected: List<FactId>,
        block: suspend () -> Unit
    ) {
        val exception = catchThrowable {
            runBlocking { block() }
        }

        assertThat(exception)
            .isInstanceOf(DuplicateFactIdException::class.java)

        val duplicateException = exception as DuplicateFactIdException

        assertThat(duplicateException.factIds)
            .containsExactlyElementsOf(expected)
    }

}
