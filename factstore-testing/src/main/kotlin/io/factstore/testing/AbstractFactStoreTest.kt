package io.factstore.testing

import io.factstore.core.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.*
import java.time.Instant
import java.util.UUID
import kotlin.system.measureTimeMillis

abstract class AbstractFactStoreTest {

    private var testStore = StoreName("default-test-store")
    private val nonExistingStore = StoreName("non-existing-store")
    private lateinit var store: FactStore

    abstract fun reset()

    abstract fun initializeFactStore(): FactStore


    @BeforeEach
    fun clearEventStore(): Unit = runBlocking {
        store = initializeFactStore()
        reset()
        val createStoreRequest = CreateStoreRequest(storeName = testStore)
        val result = store.handle(createStoreRequest)

        assertThat(result).isInstanceOf(CreateStoreResult.Created::class.java)
    }


    @Test
    fun testCreateFactStore(): Unit = runBlocking {
        val name = StoreName("test")
        val request = CreateStoreRequest(name)

        val result = store.handle(request)

        assertThat(result)
            .isNotNull()
            .isInstanceOf(CreateStoreResult.Created::class.java)

        // creating another fact store with the same name should be rejected
        val secondResult = store.handle(request)

        assertThat(secondResult)
            .isNotNull()
            .isInstanceOf(CreateStoreResult.NameAlreadyExists::class.java)

        // creating another fact store with a different name should work

        val anotherName = StoreName("another-store")
        val anotherRequest = CreateStoreRequest(anotherName)

        val thirdResult = store.handle(anotherRequest)

        assertThat(thirdResult)
            .isNotNull()
            .isInstanceOf(CreateStoreResult.Created::class.java)

        assertThat(store.existsByName(name)).isTrue()
        assertThat(store.existsByName(anotherName)).isTrue()
        assertThat(store.existsByName(StoreName("non-existing"))).isFalse()

        assertThat(store.listAll()).size().isEqualTo(3)
    }

    @Test
    fun testSimpleAppend(): Unit = runBlocking {
        val id = FactId.generate()
        val payload = """ { "username": "Peter" } """.toFactPayload()
        val createdAt = Instant.now()

        val fact = Fact(
            id = id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_ONBOARDED".toFactType(),
            payload = payload,
            appendedAt = createdAt
        )

        store.append(testStore, fact)

        store.stream(testStore).let { (it as StreamResult.FactStream).stream }.take(1).collect {
            println("Streamed fact: $it")
        }

        // validate existence of fact
        assertThat(store.existsById(testStore, id)).isEqualTo(ExistsByIdResult.Exists)

        // find fact by ID
        val findResult = store.findById(testStore, id)
        assertThat(findResult).isInstanceOf(FindByIdResult.Found::class.java)
        val foundFact = (findResult as FindByIdResult.Found).fact
        assertThat(foundFact).isEqualTo(fact)
    }

    @Test
    fun testExists(): Unit = runBlocking {
        val nonExistingFactId = FactId.generate()
        assertThat(store.existsById(testStore, nonExistingFactId)).isEqualTo(ExistsByIdResult.DoesNotExist)
    }

    @Test
    fun testExistsForNonExistingFactstore(): Unit = runBlocking {
        assertThat(
            store.existsById(
                nonExistingStore,
                FactId.generate()
            )
        ).isEqualTo(ExistsByIdResult.StoreNotFound)
    }

    @Test
    fun testFindByIdWithNonExistingFactStore(): Unit = runBlocking {
        val result = store.findById(nonExistingStore, FactId.generate())
        assertThat(result).isInstanceOf(FindByIdResult.StoreNotFound::class.java)
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = now.minusSeconds(60) // 1 minute ago
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_UPDATED".toFactType(),
            payload = """{ "username": "Alice", "status": "active" }""".toFactPayload(),
            appendedAt = now
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_DELETED".toFactType(),
            payload = """{ "username": "Bob" }""".toFactPayload(),
            appendedAt = now.plusSeconds(60) // 1 minute in the future
        )

        // Append all three
        store.append(testStore, listOf(fact1, fact2, fact3))

        // Query range covering fact1 + fact2, but excluding fact3
        val result = store.findInTimeRange(
            storeName = testStore,
            TimeRange(
                start = now.minusSeconds(120),
                end = now.plusSeconds(10)
            )
        )

        assertThat(result).isInstanceOf(FindInTimeRangeResult.Found::class.java)
        val foundFacts = (result as FindInTimeRangeResult.Found).facts
        assertThat(foundFacts).containsExactlyInAnyOrder(fact1, fact2)
        assertThat(foundFacts).doesNotContain(fact3)
    }

    @Test
    fun testFindInTimeRangeForNonExistingStore(): Unit = runBlocking {
        val result = store.findInTimeRange(
            storeName = nonExistingStore,
            TimeRange(
                start = Instant.now().minusSeconds(120),
                end = Instant.now().plusSeconds(10)
            )
        )

        assertThat(result).isInstanceOf(FindInTimeRangeResult.StoreNotFound::class.java)
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        // append fact1
        val appendRequestWithEmptySubjectCondition = AppendRequest(
            storeName = testStore,
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
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val appendRequestWithFirstFactSubjectCondition = AppendRequest(
            storeName = testStore,
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
            storeName = testStore,
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val fact2 = Fact(
            id = fact2Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "BOB" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val fact3 = Fact(
            id = fact3Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val appendRequest = AppendRequest(
            storeName = testStore,
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val fact2 = Fact(
            id = fact2Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "BOB" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val fact3 = Fact(
            id = fact3Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val factsToAppend = listOf(fact1, fact2, fact3)

        store.append(testStore, factsToAppend)

        val aliceResult = store.findBySubject(testStore, SubjectRef("USER", "ALICE"))
        assertThat(aliceResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((aliceResult as FindBySubjectResult.Found).facts)
            .containsExactly(fact1, fact3)

        val bobResult = store.findBySubject(testStore, SubjectRef("USER", "BOB"))
        assertThat(bobResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((bobResult as FindBySubjectResult.Found).facts)
            .containsExactly(fact2)

        val peterResult = store.findBySubject(testStore, SubjectRef("USER", "PETER"))
        assertThat(peterResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((peterResult as FindBySubjectResult.Found).facts).isEmpty()

        val unknownResult = store.findBySubject(testStore, SubjectRef("UNKNOWN", "UNKNOWN"))
        assertThat(unknownResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((unknownResult as FindBySubjectResult.Found).facts).isEmpty()
    }

    @Test
    fun findBySubjectForNonExistingFactstore(): Unit = runBlocking {
        val result = store.findBySubject(nonExistingStore, SubjectRef("SOME_TYPE", "SOME_ID"))
        assertThat(result).isInstanceOf(FindBySubjectResult.StoreNotFound::class.java)
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = mapOf("test" to "123", "loc" to "world")
        )

        val fact2 = Fact(
            id = fact2Id,
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "BOB" }""".toFactPayload(),
            appendedAt = Instant.now()
        )

        val factsToAppend = listOf(fact1, fact2)

        store.append(testStore, factsToAppend)

        assertThat(store.findById(testStore, fact1Id)).isInstanceOf(FindByIdResult.Found::class.java)
        assertThat((store.findById(testStore, fact1Id) as FindByIdResult.Found).fact).isEqualTo(fact1)
        assertThat(store.findById(testStore, fact2Id)).isInstanceOf(FindByIdResult.Found::class.java)
        assertThat((store.findById(testStore, fact2Id) as FindByIdResult.Found).fact).isEqualTo(fact2)
    }

    @Test
    fun appendEventsWithTagsAndFindThem(): Unit = runBlocking {
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Bob" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("role") to TagValue("user"), TagKey("region") to TagValue("us"))
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "CHARLIE",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Charlie" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("us"))
        )

        store.append(testStore, listOf(fact1, fact2, fact3))

        // --- Query 1: Find all role=admin (OR semantics → fact1 + fact3)
        val adminResult = store.findByTags(testStore, listOf(TagKey("role") to TagValue("admin")))
        assertThat(adminResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((adminResult as FindByTagsResult.Found).facts).containsExactly(fact1, fact3)

        // --- Query 2: Find all region=us (OR semantics → fact2 + fact3)
        val usResult = store.findByTags(testStore, listOf(TagKey("region") to TagValue("us")))
        assertThat(usResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((usResult as FindByTagsResult.Found).facts).containsExactly(fact2, fact3)

        // --- Query 3: Find all role=admin OR region=eu (OR semantics → fact1 + fact3)
        val adminOrEuResult = store.findByTags(
            testStore,
            listOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))
        )
        assertThat(adminOrEuResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((adminOrEuResult as FindByTagsResult.Found).facts).containsExactly(fact1, fact3)

        // --- Query 4: Non-existent tag → empty
        val noFactsResult = store.findByTags(testStore, listOf(TagKey("region") to TagValue("asia")))
        assertThat(noFactsResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((noFactsResult as FindByTagsResult.Found).facts).isEmpty()

        // --- Query 5: Union of all queries (just to validate coverage)

        val fact1Loaded = store.findById(testStore, fact1.id)
        assertThat(fact1Loaded).isInstanceOf(FindByIdResult.Found::class.java)
        println(fact1Loaded)

        val allFactsResult = store.findByTags(
            testStore,
            listOf(
                TagKey("role") to TagValue("admin"),
                TagKey("role") to TagValue("user"),
                TagKey("region") to TagValue("eu"),
                TagKey("region") to TagValue("us")
            )
        )
        assertThat(allFactsResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((allFactsResult as FindByTagsResult.Found).facts).containsExactly(fact1, fact2, fact3)
    }

    @Test
    fun testFindByTagsWithNonExistingFactstore(): Unit = runBlocking {
        val result = store.findByTags(
            nonExistingStore,
            listOf(TagKey("region") to TagValue("asia"))
        )

        assertThat(result).isInstanceOf(FindByTagsResult.StoreNotFound::class.java)
    }

    @OptIn(FlowPreview::class)
    @Test
    fun testFactStreaming(): Unit = runBlocking {

        val collectedFacts = mutableListOf<Fact>()
        val firstThreeReceived = CompletableDeferred<Unit>()

        // Start streaming from beginning
        val streamJob = launch {
            store.stream(testStore)
                .let { (it as StreamResult.FactStream).stream }
                .take(3)
                .collect {
                    collectedFacts += it
                    if (collectedFacts.size == 3) {
                        firstThreeReceived.complete(Unit)
                    }
                }
        }

        // Create facts
        val fact1 = createUserFact("ALICE", "Alice", "admin", "eu")
        val fact2 = createUserFact("BOB", "Bob", "user", "us")
        val fact3 = createUserFact("CHARLIE", "Charlie", "admin", "us")

        // Append
        store.append(testStore, fact1)
        store.append(testStore, fact2)
        store.append(testStore, fact3)

        // Wait deterministically until 3 facts are received
        firstThreeReceived.await()

        assertThat(collectedFacts).containsExactly(fact1, fact2, fact3)

        streamJob.cancelAndJoin()

        // ---- Test StartPosition.After ----

        val streamedEvents = store.stream(
            testStore,
            StreamingOptions(startPosition = StartPosition.After(fact1.id))
        ).let { (it as StreamResult.FactStream).stream }
            .take(2)
            .toList()

        assertThat(streamedEvents).containsExactly(fact2, fact3)

        // ---- Test non-existing fact ----

        val nonExistingFactId = FactId.generate()

        val streamResult = store.stream(
            testStore,
            StreamingOptions(startPosition = StartPosition.After(nonExistingFactId))
        )

        assertThat(streamResult).isInstanceOf(StreamResult.InvalidStartPosition::class.java)
    }

    @OptIn(FlowPreview::class)
    @Test
    fun testFactStreaming_startPositionEnd() = runBlocking {

        // Append initial facts BEFORE starting the stream
        val initialFact1 = createUserFact("ALICE", "Alice", "admin", "eu")
        val initialFact2 = createUserFact("BOB", "Bob", "user", "us")

        store.append(testStore, initialFact1)
        store.append(testStore, initialFact2)

        val received = mutableListOf<Fact>()
        val receivedLatch = CompletableDeferred<Unit>()
        val streamStartedLatch = CompletableDeferred<Unit>()

        // Start stream from END (should NOT see initialFact1/2)
        val job = launch {
            store.stream(
                testStore,
                StreamingOptions(startPosition = StartPosition.End)
            )
                .let { (it as StreamResult.FactStream).stream }
                .take(2)
                .onStart { streamStartedLatch.complete(Unit) }
                .collect {
                    received += it
                    if (received.size == 2) {
                        receivedLatch.complete(Unit)
                    }
                }
        }

        // wait for the streaming to start
        streamStartedLatch.await()

        // Append facts AFTER stream started
        val newFact1 = createUserFact("CHARLIE", "Charlie", "admin", "us")
        val newFact2 = createUserFact("DAVID", "David", "user", "eu")

        store.append(testStore, newFact1)
        store.append(testStore, newFact2)

        // Wait deterministically until 2 facts received
        receivedLatch.await()

        assertThat(received)
            .containsExactly(newFact1, newFact2)

        job.cancelAndJoin()
    }

    private fun createUserFact(
        subjectId: String,
        username: String,
        role: String,
        region: String
    ): Fact =
        Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = subjectId),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "$username" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(
                TagKey("role") to TagValue(role),
                TagKey("region") to TagValue(region)
            )
        )

    @Test
    fun testStreamingNonExistingFactstore(): Unit = runBlocking {
        val streamResult = store.stream(nonExistingStore)
        assertThat(streamResult).isInstanceOf(StreamResult.StoreNotFound::class.java)
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Bob" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "CHARLIE",
            ),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Charlie" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))
        )

        store.append(testStore, listOf(fact1, fact2, fact3))


        // Test 1: Query with a single tag (username = "bob")
        val bobQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("username") to TagValue("bob"))
                )
            )
        )

        val bobFacts = store.findByTagQuery(testStore, bobQuery)
        assertThat(bobFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((bobFacts as FindByTagQueryResult.Found).facts).containsExactly(fact2)

        // Test 2: Query with multiple tags (AND condition: username = "bob" and region = "us")
        val multipleTagsQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))
                )
            )
        )

        val multipleTagsFacts = store.findByTagQuery(testStore, multipleTagsQuery)
        assertThat(multipleTagsFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((multipleTagsFacts as FindByTagQueryResult.Found).facts).containsExactly(fact2)

        // Test 3: Query with multiple tags but one does not match (username = "bob" and region = "eu")
        val noMatchQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("eu"))
                )
            )
        )

        val noMatchFacts = store.findByTagQuery(testStore, noMatchQuery)
        assertThat(noMatchFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((noMatchFacts as FindByTagQueryResult.Found).facts).isEmpty()

        // Test 4: Query with multiple types (USER_CREATED or USER_DELETED)
        val multipleTypesQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType(), "USER_DELETED".toFactType()),
                    tags = listOf(TagKey("username") to TagValue("bob"))
                )
            )
        )

        val multipleTypesFacts = store.findByTagQuery(testStore, multipleTypesQuery)
        assertThat(multipleTypesFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((multipleTypesFacts as FindByTagQueryResult.Found).facts).containsExactly(fact2)

        // Test 5: Query with multiple tags and multiple types (AND for tags, OR for types)
        val complexQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType(), "USER_DELETED".toFactType()),
                    tags = listOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))
                )
            )
        )

        val complexFacts = store.findByTagQuery(testStore, complexQuery)
        assertThat(complexFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((complexFacts as FindByTagQueryResult.Found).facts).containsExactly(fact2)

        // Test 6: Query with tags but no matching facts (tags = "username" to "dave")
        val noMatchingTagQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("username") to TagValue("dave"))
                )
            )
        )

        val noMatchingTagFacts = store.findByTagQuery(testStore, noMatchingTagQuery)
        assertThat(noMatchingTagFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((noMatchingTagFacts as FindByTagQueryResult.Found).facts).isEmpty()

        // Test 7: Query with types but no matching facts (types = "USER_DELETED" but no such facts exist)
        val noMatchingTypeQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_DELETED")),
                    tags = listOf(TagKey("username") to TagValue("bob"))
                )
            )
        )

        val noMatchingTypeFacts = store.findByTagQuery(testStore, noMatchingTypeQuery)
        assertThat(noMatchingTypeFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((noMatchingTypeFacts as FindByTagQueryResult.Found).facts).isEmpty()

        // Test 8: Query with tags but no facts that have these tags
        val tagsNoFactsQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("username") to TagValue("david"), TagKey("region") to TagValue("asia"))
                )
            )
        )

        val tagsNoFacts = store.findByTagQuery(testStore, tagsNoFactsQuery)
        assertThat(tagsNoFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((tagsNoFacts as FindByTagQueryResult.Found).facts).isEmpty()

        // Test 9: Query for facts with different tags
        val differentTagsQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))
                )
            )
        )

        val differentTagsFacts = store.findByTagQuery(testStore, differentTagsQuery)
        assertThat(differentTagsFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((differentTagsFacts as FindByTagQueryResult.Found).facts).containsExactly(fact3)

    }

    @Test
    fun testMultipleFactTypesOneQueryItem(): Unit = runBlocking {
        // Create facts with different types
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "BOB"),
            type = "USER_UPDATED".toFactType(),
            payload = """{ "username": "Bob" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "CHARLIE"),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Charlie" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))
        )

        store.append(testStore, listOf(fact1, fact2, fact3))

        // Query for facts with types "USER_CREATED" or "USER_UPDATED" and with tags "username" = "alice"
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType(), "USER_UPDATED".toFactType()),
                    tags = listOf(TagKey("username") to TagValue("alice"))
                )
            )
        )

        val result = store.findByTagQuery(testStore, query)

        // Expecting fact1 only (username = alice, type = "USER_CREATED")
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts).containsExactly(fact1)
    }

    @Test
    fun testMultipleQueryItems(): Unit = runBlocking {
        // Create facts with different types and tags
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "BOB"),
            type = "USER_UPDATED".toFactType(),
            payload = """{ "username": "Bob" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "CHARLIE"),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Charlie" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))
        )

        store.append(testStore, listOf(fact1, fact2, fact3))

        // Query for facts with type "USER_CREATED" or "USER_UPDATED", tagged with "username" = "bob"
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType(), "USER_UPDATED".toFactType()),
                    tags = listOf(TagKey("username") to TagValue("bob"))
                ),
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType()),
                    tags = listOf(TagKey("region") to TagValue("us"))
                )
            )
        )

        val result = store.findByTagQuery(testStore, query)

        // Expecting fact2 (username = bob, region = us, type = USER_UPDATED)
        // Expecting fact3 (region = us, type = USER_CREATED, but no username filter for 'bob')
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts).containsExactlyInAnyOrder(fact2, fact3)

    }

    @Test
    fun testMixedTypesAndTagsQueryItems(): Unit = runBlocking {
        // Create facts with different types and tags
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "BOB"),
            type = "USER_UPDATED".toFactType(),
            payload = """{ "username": "Bob" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))
        )

        val fact3 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "CHARLIE"),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Charlie" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))
        )

        store.append(testStore, listOf(fact1, fact2, fact3))

        // Query with multiple query items
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType(), "USER_UPDATED".toFactType()),
                    tags = listOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))
                ),
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType()),
                    tags = listOf(TagKey("region") to TagValue("eu"), TagKey("username") to TagValue("alice"))
                )
            )
        )

        val result = store.findByTagQuery(testStore, query)

        // Expecting fact2 (username = bob, region = us, type = USER_UPDATED)
        // Expecting fact1 (username = alice, region = eu, type = USER_CREATED)
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts).containsExactlyInAnyOrder(fact2, fact1)
    }

    @Test
    fun testNoMatchingFacts(): Unit = runBlocking {
        // Create facts with different types and tags
        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(type = "USER", id = "ALICE"),
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            metadata = emptyMap(),
            tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))
        )

        store.append(testStore, listOf(fact1))

        // Query for facts with a non-matching type and tag
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_UPDATED")),
                    tags = listOf(TagKey("username") to TagValue("bob"))
                )
            )
        )

        val result = store.findByTagQuery(testStore, query)

        // Expecting no facts because no fact matches the query
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts).isEmpty()
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
                type = "USER_CREATED".toFactType(),
                payload = """{ "username": "user$index" }""".toFactPayload(),
                appendedAt = Instant.now(),
                metadata = emptyMap(),
                tags = mapOf(
                    TagKey("role") to TagValue(tag),
                    TagKey("region") to TagValue(region)
                )
            )
        }

        // Append all events to the store (in chunks to avoid exceeding byte limit in FoundationDB)
        events
            .chunked(500)
            .forEach { store.append(testStore, it) }

        // append a few more events
        store.append(
            testStore,
            Fact(
                id = FactId.generate(),
                subjectRef = SubjectRef(
                    type = "USER",
                    id = "user-${FactId.generate()}"
                ),
                type = "USER_CREATED".toFactType(),
                payload = """{ "username": "user" }""".toFactPayload(),
                appendedAt = Instant.now(),
                metadata = emptyMap(),
                tags = mapOf(
                    TagKey("role") to TagValue("custom"),
                    TagKey("region") to TagValue("eu")
                )
            )
        )


        // Step 2: Query for only a handful of the events (e.g., 100 events with tag "user" and region "us")
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("role") to TagValue("user"), TagKey("region") to TagValue("us"))
                )
            )
        )

        val result = store.findByTagQuery(testStore, query)


        measureTimeMillis {
            store.findByTagQuery(testStore, query)
        }.also { println(it) }

        measureTimeMillis {
            store.findByTagQuery(
                testStore,
                TagQuery(
                    listOf(
                        TagTypeItem(
                            types = listOf(FactType("USER_CREATED")),
                            tags = listOf(TagKey("role") to TagValue("custom"))
                        )
                    )
                )
            )
        }.also { println(it) }


        // Step 3: Verify that the result only contains facts with the expected tags
        // The number of events matching "role=user" and "region=us" should be around half of 10,000 (i.e., ~5000).
        val expectedCount =
            events.count { it.tags[TagKey("role")] == TagValue("user") && it.tags[TagKey("region")] == TagValue("us") }
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts)
            .hasSize(expectedCount) // Ensure the correct number of matching events
            .allMatch {
                it.tags[TagKey("role")] == TagValue("user") &&
                        it.tags[TagKey("region")] == TagValue("us")
            }.also {
                println("Found ${result.facts.size} events with 'role=user' and 'region=us'.")
            }

    }

    @Test
    fun testFindByTagQueryWithNonExistingFactstore(): Unit = runBlocking {
        assertThat(
            store.findByTagQuery(
                nonExistingStore,
                TagQuery(
                    listOf(
                        TagTypeItem(
                            types = listOf(FactType("USER_CREATED")),
                            tags = listOf(TagKey("role") to TagValue("custom"))
                        )
                    )
                )
            )
        )
            .isInstanceOf(FindByTagQueryResult.StoreNotFound::class.java)
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "Alice" }""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = mapOf(
                TagKey("user") to TagValue("ALICE"),
            )
        )


        val tagQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("user") to TagValue("ALICE")),
                )
            )
        )

        println("appending $fact1Id")
        store.append(
            AppendRequest(
                storeName = testStore,
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
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "ALICE" }""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = mapOf(
                TagKey("user") to TagValue("ALICE"),
            )
        )

        val appendRequest2 = AppendRequest(
            storeName = testStore,
            facts = listOf(fact2),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.TagQueryBased(
                failIfEventsMatch = tagQuery,
                after = fact1Id
            )
        )

        store.append(appendRequest2).also { assertThat(it).isInstanceOf(AppendResult.Appended::class.java) }

        val appendRequest3 = AppendRequest(
            storeName = testStore,
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
            type = "USER_CREATED".toFactType(),
            payload = """{ "username": "BOB" }""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = mapOf(
                TagKey("user") to TagValue("BOB"),
            )
        )

        val tagQuery2 = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("user") to TagValue("BOB")),
                )
            )
        )

        println("appending $fact3Id")
        val appendRequest4 = AppendRequest(
            storeName = testStore,
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
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "BOB" }""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = mapOf(
                TagKey("user") to TagValue("BOB"),
            )
        )

        val appendRequestThatShouldAppendFact4 = AppendRequest(
            storeName = testStore,
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

        val storeName1 = StoreName("store-1")
        val storeName2 = StoreName("store-2")

        store.handle(CreateStoreRequest(storeName1))
        store.handle(CreateStoreRequest(storeName2))

        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "BOB",
            ),
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "BOB" }""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = emptyMap()
        )

        val fact2 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "ALICE",
            ),
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "ALICE" }""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = emptyMap()
        )

        store.append(storeName1, fact1)
        store.append(storeName2, fact2)

        assertThat(store.existsById(storeName1, fact1.id)).isEqualTo(ExistsByIdResult.Exists)
        assertThat(store.existsById(storeName1, fact2.id)).isEqualTo(ExistsByIdResult.DoesNotExist)

        assertThat(store.existsById(storeName2, fact1.id)).isEqualTo(ExistsByIdResult.DoesNotExist)
        assertThat(store.existsById(storeName2, fact2.id)).isEqualTo(ExistsByIdResult.Exists)
    }

    @Test
    fun testAppendWithoutFactStore(): Unit = runBlocking {
        val result = store.append(nonExistingStore, createUserFact("TEST", "Test User", "user", "eu"))
        assertThat(result).isInstanceOf(AppendResult.StoreNotFound::class.java)
    }

    @Test
    fun testNewAppend(): Unit = runBlocking {

        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "USER",
                id = "DOMI",
            ),
            type = "USER_LOCKED".toFactType(),
            payload = """{ "username": "DOMI" }""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = emptyMap()
        )
        val idempotencyKey = IdempotencyKey(UUID.randomUUID())
        val appendRequest = AppendRequest(
            storeName = testStore,
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
            type = "TEST_FACT_TYPE".toFactType(),
            payload = """DATA""".toFactPayload(),
            appendedAt = Instant.now(),
            tags = emptyMap()
        )

        val fact2 = fact1.copy()

        store.append(testStore, fact1)

        assertDuplicateFactIds(
            expected = listOf(factId)
        ) {
            store.append(testStore, fact2)
        }

        assertDuplicateFactIds(
            expected = listOf(factId)
        ) {
            store.append(testStore, listOf(fact2))
        }

        assertDuplicateFactIds(
            expected = listOf(factId)
        ) {
            store.append(
                AppendRequest(
                    storeName = testStore,
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
