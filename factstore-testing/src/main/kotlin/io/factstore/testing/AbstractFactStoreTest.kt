package io.factstore.testing

import io.factstore.core.*
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.onStart
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.transform
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.*
import java.time.Instant
import java.util.UUID
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val ALICE_SUBJECT_VALUE = "USER:ALICE"
private const val BOB_SUBJECT_VALUE = "USER:BOB"
private const val CHARLIE_SUBJECT_VALUE = "USER:CHARLIE"
private const val EVEN_FACT_TYPE = "FACT_EVEN"
private const val ODD_FACT_TYPE = "FACT_ODD"

abstract class AbstractFactStoreTest {

    private var testStore = StoreName("default-test-store")
    private val nonExistingStore = StoreName("non-existing-store")
    private lateinit var store: FactStore

    private val alicePayload = """{ "username": "Alice" }""".toFactPayload()
    private val bobPayload = """{ "username": "Bob" }""".toFactPayload()
    private val charliePayload = """{ "username": "Charlie" }""".toFactPayload()

    abstract fun reset()

    abstract fun initializeFactStore(): FactStore

    // ===== Test helpers =====
    //
    // Facts are appended as FactInputs; the store assigns their id and appendedAt.
    // These helpers append inputs and read back the stored facts (by their
    // server-assigned ids) so tests can assert on the canonical stored shape.

    private fun input(
        subject: String,
        type: String,
        payload: FactPayload,
        metadata: Map<MetadataKey, MetadataValue> = emptyMap(),
        tags: Map<TagKey, TagValue> = emptyMap(),
    ) = FactInput(
        type = FactType(type),
        subject = Subject(subject),
        payload = payload,
        metadata = metadata,
        tags = tags,
    )

    private fun userInput(
        subjectId: String,
        username: String,
        role: String,
        region: String,
    ) = input(
        subject = "USER:$subjectId",
        type = "USER_CREATED",
        payload = """{ "username": "$username" }""".toFactPayload(),
        tags = mapOf(TagKey("role") to TagValue(role), TagKey("region") to TagValue(region)),
    )

    private suspend fun appendStored(inputs: List<FactInput>, storeName: StoreName = testStore): List<Fact> {
        val result = store.append(AppendRequest(storeName, inputs, IdempotencyKey()))
        val ids = (result as AppendResult.Appended).factIds
        return ids.map { (store.findById(FindByIdRequest(storeName, it)) as FindByIdResult.Found).fact }
    }

    private suspend fun appendStored(input: FactInput, storeName: StoreName = testStore): Fact =
        appendStored(listOf(input), storeName).single()


    @BeforeEach
    fun clearEventStore(): Unit = runBlocking {
        store = initializeFactStore()
        reset()
        val createStoreRequest = CreateStoreRequest(storeName = testStore)
        val result = store.create(createStoreRequest)

        assertThat(result).isInstanceOf(CreateStoreResult.Created::class.java)
    }


    @Test
    fun testCreateFactStore(): Unit = runBlocking {
        val name = StoreName("test")
        val request = CreateStoreRequest(name)

        val result = store.create(request)

        assertThat(result)
            .isNotNull()
            .isInstanceOf(CreateStoreResult.Created::class.java)

        // creating another fact store with the same name should be rejected
        val secondResult = store.create(request)

        assertThat(secondResult)
            .isNotNull()
            .isInstanceOf(CreateStoreResult.NameAlreadyExists::class.java)

        // creating another fact store with a different name should work

        val anotherName = StoreName("another-store")
        val anotherRequest = CreateStoreRequest(anotherName)

        val thirdResult = store.create(anotherRequest)

        assertThat(thirdResult)
            .isNotNull()
            .isInstanceOf(CreateStoreResult.Created::class.java)

        assertThat(store.existsByName(ExistsStoreByNameRequest(name))).isInstanceOf(ExistsStoreByNameResult.StoreExists::class.java)
        assertThat(store.existsByName(ExistsStoreByNameRequest(anotherName))).isInstanceOf(ExistsStoreByNameResult.StoreExists::class.java)
        assertThat(store.existsByName(ExistsStoreByNameRequest(StoreName("non-existing")))).isInstanceOf(ExistsStoreByNameResult.StoreAbsent::class.java)

        assertThat(store.listAll()).size().isEqualTo(3)
    }

    @Test
    fun testSimpleAppend(): Unit = runBlocking {
        val payload = """ { "username": "Peter" } """.toFactPayload()

        val fact = appendStored(input(ALICE_SUBJECT_VALUE, "USER_ONBOARDED", payload))
        val id = fact.id

        store.subscribe(SubscribeRequest(testStore)).let { (it as SubscribeResult.FactStream).stream }.transform { batch -> batch.forEach { emit(it) } }.take(1).collect {
            println("Streamed fact: $it")
        }

        // validate existence of fact
        assertThat(store.existsById(ExistsByIdRequest(testStore, id))).isEqualTo(ExistsByIdResult.Exists)

        // find fact by ID
        val findResult = store.findById(FindByIdRequest(testStore, id))
        assertThat(findResult).isInstanceOf(FindByIdResult.Found::class.java)
        val foundFact = (findResult as FindByIdResult.Found).fact
        assertThat(foundFact).isEqualTo(fact)
    }

    @Test
    fun testExists(): Unit = runBlocking {
        val nonExistingFactId = FactId.generate()
        assertThat(store.existsById(ExistsByIdRequest(testStore, nonExistingFactId))).isEqualTo(ExistsByIdResult.DoesNotExist)
    }

    @Test
    fun testExistsForNonExistingFactstore(): Unit = runBlocking {
        assertThat(
            store.existsById(ExistsByIdRequest(nonExistingStore, FactId.generate()))
        ).isEqualTo(ExistsByIdResult.StoreNotFound(nonExistingStore))
    }

    @Test
    fun testFindByIdWithNonExistingFactStore(): Unit = runBlocking {
        val result = store.findById(FindByIdRequest(nonExistingStore, FactId.generate()))
        assertThat(result).isInstanceOf(FindByIdResult.StoreNotFound::class.java)
    }








    @Test
    fun testConditionalAppendWithSubject(): Unit = runBlocking {
        // append fact1 expecting no prior fact for the subject
        val appended1 = store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.ExpectedLastFact(
                    subject = Subject(ALICE_SUBJECT_VALUE),
                    expectedLastFactId = null
                )
            )
        )
        assertThat(appended1).isInstanceOf(AppendResult.Appended::class.java)
        val fact1Id = (appended1 as AppendResult.Appended).factIds.single()

        // append fact2 expecting fact1 to be the last fact
        val appended2 = store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.ExpectedLastFact(
                    subject = Subject(ALICE_SUBJECT_VALUE),
                    expectedLastFactId = fact1Id
                )
            )
        )
        assertThat(appended2).isInstanceOf(AppendResult.Appended::class.java)

        // appending again with the now-stale expected last fact id should fail;
        // this simulates two concurrent/conflicting append requests
        val violated = store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.ExpectedLastFact(
                    subject = Subject(ALICE_SUBJECT_VALUE),
                    expectedLastFactId = fact1Id // <-- this will cause the violation
                )
            )
        )
        assertThat(violated).isInstanceOf(AppendResult.AppendConditionViolated::class.java)
    }

    @Test
    fun testConcurrentConditionalAppendsOnlyOneWins(): Unit = runBlocking {
        // Seed an initial fact; all contenders will expect it to still be the last fact.
        val seed = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))

        val contenders = 8

        // Fire many conditional appends at the same subject concurrently, each expecting
        // `seed` to be the last fact. This is the optimistic-concurrency race that
        // ExpectedLastFact exists to arbitrate.
        val results = coroutineScope {
            (1..contenders).map {
                async(Dispatchers.Default) {
                    store.append(
                        AppendRequest(
                            storeName = testStore,
                            facts = listOf(input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload)),
                            idempotencyKey = IdempotencyKey(),
                            condition = AppendCondition.ExpectedLastFact(
                                subject = Subject(ALICE_SUBJECT_VALUE),
                                expectedLastFactId = seed.id,
                            ),
                        )
                    )
                }
            }.awaitAll()
        }

        // Exactly one contender may win; every other must be rejected as a condition violation.
        assertThat(results.filterIsInstance<AppendResult.Appended>()).hasSize(1)
        assertThat(results.count { it is AppendResult.AppendConditionViolated }).isEqualTo(contenders - 1)

        // The subject history therefore contains exactly the seed plus the single winner.
        val subjectFacts = streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None))
        assertThat(subjectFacts).hasSize(2)
    }

    @Test
    fun testMultipleFactsOptimisticAppend(): Unit = runBlocking {
        val appendRequest = AppendRequest(
            storeName = testStore,
            facts = listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            ),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.All(
                conditions = listOf(
                    AppendCondition.ExpectedLastFact(Subject(ALICE_SUBJECT_VALUE), null),
                    AppendCondition.ExpectedLastFact(Subject(BOB_SUBJECT_VALUE), null),
                )
            )
        )

        store.append(appendRequest).also {
            assertThat(it).isInstanceOf(AppendResult.Appended::class.java)
        }

    }

    @Test
    fun testCompositeConditionViolatedWhenOneConditionFails(): Unit = runBlocking {

        // Seed Alice with a fact, so expecting her last fact to be null no longer holds.
        appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))

        val appendRequest = AppendRequest(
            storeName = testStore,
            facts = listOf(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload)),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.All(
                conditions = listOf(
                    // Bob has no facts yet -> satisfied
                    AppendCondition.ExpectedLastFact(Subject(BOB_SUBJECT_VALUE), null),
                    // Alice already has a fact -> expecting null is violated
                    AppendCondition.ExpectedLastFact(Subject(ALICE_SUBJECT_VALUE), null),
                )
            )
        )

        store.append(appendRequest).also {
            assertThat(it).isInstanceOf(AppendResult.AppendConditionViolated::class.java)
        }

    }







    @Test
    fun testWithMetadata(): Unit = runBlocking {

        val (fact1, fact2) = appendStored(
            listOf(
                input(
                    ALICE_SUBJECT_VALUE,
                    "USER_CREATED",
                    alicePayload,
                    metadata = mapOf("test".toMetadataKey() to "123".toMetadataValue(), "loc".toMetadataKey() to "world".toMetadataValue()),
                ),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
            )
        )

        assertThat(store.findById(FindByIdRequest(testStore, fact1.id))).isInstanceOf(FindByIdResult.Found::class.java)
        assertThat((store.findById(FindByIdRequest(testStore, fact1.id)) as FindByIdResult.Found).fact).isEqualTo(fact1)
        assertThat(store.findById(FindByIdRequest(testStore, fact2.id))).isInstanceOf(FindByIdResult.Found::class.java)
        assertThat((store.findById(FindByIdRequest(testStore, fact2.id)) as FindByIdResult.Found).fact).isEqualTo(fact2)
    }

    @Test
    fun testEmptyPayloadRoundTrips(): Unit = runBlocking {
        val input = input(ALICE_SUBJECT_VALUE, "USER_LOGGED_OUT", FactPayload(ByteArray(0)))

        val stored = appendStored(input)

        assertThat(stored).isEqualTo(input.toFact(stored.id, stored.appendedAt))
    }

    @Test
    fun testFactOfMaximumSizeRoundTrips(): Unit = runBlocking {
        // Every byte value in sequence, so the payload also proves it is stored as
        // opaque bytes, including the NUL bytes that string-oriented encodings mishandle.
        val payload = FactPayload(ByteArray(FactPayload.MAX_SIZE) { (it % 256).toByte() })

        val input = FactInput(
            type = FactType("t".repeat(FactType.MAX_LENGTH)),
            subject = Subject("s".repeat(Subject.MAX_LENGTH)),
            payload = payload,
            metadata = (1..FactInput.MAX_METADATA_ENTRIES).associate { i ->
                MetadataKey("m$i".padEnd(MetadataKey.MAX_LENGTH, 'x')) to
                        MetadataValue("v".repeat(MetadataValue.MAX_LENGTH))
            },
            tags = (1..FactInput.MAX_TAGS).associate { i ->
                TagKey("t$i".padEnd(TagKey.MAX_LENGTH, 'x')) to TagValue("v".repeat(TagValue.MAX_LENGTH))
            },
        )

        val stored = appendStored(input)

        assertThat(stored).isEqualTo(input.toFact(stored.id, stored.appendedAt))
    }

    @Test
    fun testAppendOfMaximumFactCountRoundTrips(): Unit = runBlocking {
        val inputs = (1..AppendRequest.MAX_FACTS).map { i ->
            input(ALICE_SUBJECT_VALUE, "USER_UPDATED", """{ "revision": $i }""".toFactPayload())
        }

        val stored = appendStored(inputs)

        assertThat(stored).hasSize(AppendRequest.MAX_FACTS)
        assertThat(stored.map { it.payload }).containsExactlyElementsOf(inputs.map { it.payload })
    }

    @Test
    fun testAppendOfMaximumSizeRoundTrips(): Unit = runBlocking {
        // Full-length subjects, types and tags are the costliest facts for a backend, because those
        // values are written into its indexes as well. As many of them as fit, with a payload making
        // up the remainder, bring the append to exactly its maximum size.
        val envelopeSize = Subject.MAX_LENGTH + FactType.MAX_LENGTH +
                FactInput.MAX_TAGS * (TagKey.MAX_LENGTH + TagValue.MAX_LENGTH)
        val factCount = AppendRequest.MAX_SIZE / envelopeSize
        val remainder = AppendRequest.MAX_SIZE - factCount * envelopeSize

        val inputs = (0 until factCount).map { i ->
            FactInput(
                type = FactType("t".repeat(FactType.MAX_LENGTH)),
                subject = Subject("s$i".padEnd(Subject.MAX_LENGTH, 'x')),
                payload = FactPayload(ByteArray(if (i == 0) remainder else 0)),
                tags = (1..FactInput.MAX_TAGS).associate { t ->
                    TagKey("t$t".padEnd(TagKey.MAX_LENGTH, 'x')) to TagValue("v".repeat(TagValue.MAX_LENGTH))
                },
            )
        }
        assertThat(inputs.sumOf { it.byteSize }).isEqualTo(AppendRequest.MAX_SIZE)

        val result = store.append(AppendRequest(testStore, inputs, IdempotencyKey()))

        assertThat(result).isInstanceOf(AppendResult.Appended::class.java)
        val ids = (result as AppendResult.Appended).factIds
        assertThat(ids).hasSize(factCount)
        val last = (store.findById(FindByIdRequest(testStore, ids.last())) as FindByIdResult.Found).fact
        assertThat(last).isEqualTo(inputs.last().toFact(last.id, last.appendedAt))
    }








    @OptIn(FlowPreview::class)
    @Test
    fun testFactStreaming(): Unit = runBlocking {

        val collectedFacts = mutableListOf<Fact>()
        val firstThreeReceived = CompletableDeferred<Unit>()

        // Subscribe from the beginning
        val streamJob = launch {
            store.subscribe(SubscribeRequest(testStore))
                .let { (it as SubscribeResult.FactStream).stream }
                .transform { batch -> batch.forEach { emit(it) } }
                .take(3)
                .collect {
                    collectedFacts += it
                    if (collectedFacts.size == 3) {
                        firstThreeReceived.complete(Unit)
                    }
                }
        }

        // Create + append (the store assigns ids/appendedAt and returns the stored facts)
        val fact1 = appendStored(userInput("ALICE", "Alice", "admin", "eu"))
        val fact2 = appendStored(userInput("BOB", "Bob", "user", "us"))
        val fact3 = appendStored(userInput("CHARLIE", "Charlie", "admin", "us"))

        // Wait deterministically until 3 facts are received
        firstThreeReceived.await()

        assertThat(collectedFacts).containsExactly(fact1, fact2, fact3)

        streamJob.cancelAndJoin()

        // ---- Test StartPosition.After ----

        val streamedEvents = store.subscribe(
            SubscribeRequest(testStore, startPosition = StartPosition.After(fact1.id))
        ).let { (it as SubscribeResult.FactStream).stream }
            .transform { batch -> batch.forEach { emit(it) } }
            .take(2)
            .toList()

        assertThat(streamedEvents).containsExactly(fact2, fact3)

        // ---- Test non-existing fact ----

        val nonExistingFactId = FactId.generate()

        val subscribeResult = store.subscribe(
            SubscribeRequest(testStore, startPosition = StartPosition.After(nonExistingFactId))
        )

        assertThat(subscribeResult).isInstanceOf(SubscribeResult.FactIdNotFound::class.java)
    }

    @OptIn(FlowPreview::class)
    @Test
    fun testFactStreamingStartPositionEnd() = runBlocking {

        // Append initial facts BEFORE starting the stream
        appendStored(userInput("ALICE", "Alice", "admin", "eu"))
        appendStored(userInput("BOB", "Bob", "user", "us"))

        val received = mutableListOf<Fact>()
        val receivedLatch = CompletableDeferred<Unit>()
        val streamStartedLatch = CompletableDeferred<Unit>()

        // Start stream from END (should NOT see initialFact1/2)
        val job = launch {
            store.subscribe(
                SubscribeRequest(testStore, startPosition = StartPosition.End)
            )
                .let { (it as SubscribeResult.FactStream).stream }
                .transform { batch -> batch.forEach { emit(it) } }
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
        val newFact1 = appendStored(userInput("CHARLIE", "Charlie", "admin", "us"))
        val newFact2 = appendStored(userInput("DAVID", "David", "user", "eu"))

        // Wait deterministically until 2 facts received
        receivedLatch.await()

        assertThat(received)
            .containsExactly(newFact1, newFact2)

        job.cancelAndJoin()
    }

    @Test
    fun testSubscribeNonExistingFactstore(): Unit = runBlocking {
        val result = store.subscribe(SubscribeRequest(nonExistingStore))
        assertThat(result).isInstanceOf(SubscribeResult.StoreNotFound::class.java)
    }

    // ===== FactStreamer: streamFacts =====

    @Test
    fun testStreamFactsForward(): Unit = runBlocking {
        val facts = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        val streamed = streamToList(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.None))

        assertThat(streamed).containsExactlyElementsOf(facts)
    }

    @Test
    fun testStreamFactsBackward(): Unit = runBlocking {
        val facts = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        val streamed = streamToList(StreamFactsRequest(testStore, null, ReadDirection.Backward, Limit.None))

        assertThat(streamed).containsExactlyElementsOf(facts.reversed())
    }

    @Test
    fun testStreamFactsWithLimit(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        // Forward + limit 2 → the two oldest facts
        assertThat(streamToList(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.of(2))))
            .containsExactly(fact1, fact2)

        // Backward + limit 2 → the two newest facts, newest first
        assertThat(streamToList(StreamFactsRequest(testStore, null, ReadDirection.Backward, Limit.of(2))))
            .containsExactly(fact3, fact2)
    }

    @Test
    fun testStreamFactsWithLimitLargerThanStore(): Unit = runBlocking {
        val facts = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
            )
        )

        val streamed = streamToList(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.of(10)))

        assertThat(streamed).containsExactlyElementsOf(facts)
    }

    @Test
    fun testStreamFactsOfEmptyStore(): Unit = runBlocking {
        val streamed = streamToList(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.None))

        assertThat(streamed).isEmpty()
    }

    @Test
    fun testStreamFactsOfNonExistingStore(): Unit = runBlocking {
        val result = store.streamFacts(StreamFactsRequest(nonExistingStore, null, ReadDirection.Forward, Limit.None))

        assertThat(result).isEqualTo(StreamFactsResult.StoreNotFound(nonExistingStore))
    }

    @Test
    fun testStreamFactsOnlyContainsFactsOfTheRequestedStore(): Unit = runBlocking {
        val otherStore = StoreName("other-store")
        store.create(CreateStoreRequest(otherStore))

        val fact = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload), otherStore)

        val streamed = streamToList(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.None))

        assertThat(streamed).containsExactly(fact)
    }

    @Test
    fun testStreamFactsExcludesFactsAppendedAfterCall(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        val fact2 = appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))

        // The head is pinned when the stream is requested, not when it is collected.
        val stream = (store.streamFacts(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.None))
                as StreamFactsResult.FactStream).facts

        appendStored(input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload))

        assertThat(withTimeout(10.seconds) { stream.toList() }).containsExactly(fact1, fact2)
    }

    @Test
    fun testStreamFactsBackwardExcludesFactsAppendedAfterCall(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        val fact2 = appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))

        // Reading backward starts at the pinned head, not at the newest fact at collection time.
        val stream = (store.streamFacts(StreamFactsRequest(testStore, null, ReadDirection.Backward, Limit.None))
                as StreamFactsResult.FactStream).facts

        appendStored(input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload))

        assertThat(withTimeout(10.seconds) { stream.toList() }).containsExactly(fact2, fact1)
    }

    @Test
    fun testStreamFactsIsRepeatable(): Unit = runBlocking {
        val facts = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
            )
        )

        val stream = (store.streamFacts(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.None))
                as StreamFactsResult.FactStream).facts

        val first = withTimeout(10.seconds) { stream.toList() }
        appendStored(input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload))
        val second = withTimeout(10.seconds) { stream.toList() }

        assertThat(first).containsExactlyElementsOf(facts)
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun testStreamFactsOfLargeStore(): Unit = runBlocking {
        val facts = appendLargeHistory()

        assertThat(streamToList(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.None)))
            .containsExactlyElementsOf(facts)
        assertThat(streamToList(StreamFactsRequest(testStore, null, ReadDirection.Backward, Limit.None)))
            .containsExactlyElementsOf(facts.reversed())
        assertThat(streamToList(StreamFactsRequest(testStore, null, ReadDirection.Forward, Limit.of(777))))
            .containsExactlyElementsOf(facts.take(777))
        assertThat(streamToList(StreamFactsRequest(testStore, null, ReadDirection.Backward, Limit.of(777))))
            .containsExactlyElementsOf(facts.reversed().take(777))
    }

    // ===== FactStreamer: continuing a stream =====

    @Test
    fun testStreamFactsContinuesAfterAFact(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload),
            )
        )

        assertThat(streamToList(StreamFactsRequest(testStore, fact1.id, ReadDirection.Forward, Limit.None)))
            .containsExactly(fact2, fact3)
        // Backward, continuing after a fact means the ones appended before it.
        assertThat(streamToList(StreamFactsRequest(testStore, fact3.id, ReadDirection.Backward, Limit.None)))
            .containsExactly(fact2, fact1)
        // With a limit, counted from the continuation.
        assertThat(streamToList(StreamFactsRequest(testStore, fact1.id, ReadDirection.Forward, Limit.of(1))))
            .containsExactly(fact2)
    }

    @Test
    fun testStreamFactsContinuedAfterTheNewestFactIsEmpty(): Unit = runBlocking {
        val facts = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
            )
        )

        assertThat(streamToList(StreamFactsRequest(testStore, facts.last().id, ReadDirection.Forward, Limit.None)))
            .isEmpty()
        // Reading backward, the oldest fact is the end of the stream.
        assertThat(streamToList(StreamFactsRequest(testStore, facts.first().id, ReadDirection.Backward, Limit.None)))
            .isEmpty()
    }

    @Test
    fun testStreamFactsContinuedAfterAnUnknownFact(): Unit = runBlocking {
        appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        val unknown = FactId.generate()

        val result = store.streamFacts(StreamFactsRequest(testStore, unknown, ReadDirection.Forward, Limit.None))

        assertThat(result).isEqualTo(StreamFactsResult.ContinuationNotFound(unknown))
    }

    @Test
    fun testStreamFactsBySubjectContinuesAfterAFactOfAnotherSubject(): Unit = runBlocking {
        val (alice1, bob, alice2) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        // The continuation marks a position; it need not be a fact the stream itself emits.
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None, bob.id)))
            .containsExactly(alice2)
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None, alice1.id)))
            .containsExactly(alice2)
    }

    @Test
    fun testStreamFactsByTypeContinuesAfterAFact(): Unit = runBlocking {
        val (created1, locked, created2) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_LOCKED", bobPayload),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload),
            )
        )

        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.None, locked.id)))
            .containsExactly(created2)
        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Backward, Limit.None, created2.id)))
            .containsExactly(created1)
    }

    @Test
    fun testStreamFactsByTagsContinuesAfterAFact(): Unit = runBlocking {
        val (alice, bob, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "eu"),
                userInput("CHARLIE", "Charlie", role = "admin", region = "eu"),
            )
        )

        // One tag, and several tags, both continue from the same marker.
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin"), ReadDirection.Forward, Limit.None, bob.id)))
            .containsExactly(charlie)
        assertThat(
            streamToList(
                tagsRequest(mapOf("role" to "admin", "region" to "eu"), ReadDirection.Forward, Limit.None, alice.id)
            )
        ).containsExactly(charlie)
    }

    @Test
    fun testStreamFactsByQueryContinuesAfterAFact(): Unit = runBlocking {
        val (alice, bob, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                userInput("CHARLIE", "Charlie", role = "admin", region = "us"),
            )
        )

        val query = query(
            FactFilter(subjects = setOf(Subject(ALICE_SUBJECT_VALUE))),
            FactFilter(tags = mapOf(TagKey("role") to TagValue("admin"))),
        )

        assertThat(streamToList(query, continueAfter = bob.id)).containsExactly(charlie)
        assertThat(streamToList(query, continueAfter = alice.id)).containsExactly(charlie)
        assertThat(streamToList(query, ReadDirection.Backward, continueAfter = charlie.id))
            .containsExactly(alice)
    }

    @Test
    fun testContinuingAStreamCoversEveryFactExactlyOnce(): Unit = runBlocking {
        // Read a long history in pages, the way a resuming consumer does, and check that the pages
        // join up without a gap or a repetition, including across the backends' read batches.
        val facts = appendLargeHistory()

        val pages = mutableListOf<Fact>()
        var continueAfter: FactId? = null
        do {
            val page = streamToList(StreamFactsRequest(testStore, continueAfter, ReadDirection.Forward, Limit.of(300)))
            pages += page
            continueAfter = page.lastOrNull()?.id
        } while (page.isNotEmpty())

        assertThat(pages).containsExactlyElementsOf(facts)
    }

    @Test
    fun testStreamFactsByQueryContinuedAfterAnUnknownFact(): Unit = runBlocking {
        appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))
        val unknown = FactId.generate()

        val result = store.streamFactsByQuery(
            StreamFactsByQueryRequest(
                testStore,
                FactQuery(listOf(FactFilter(tags = mapOf(TagKey("role") to TagValue("admin"))))),
                unknown,
                ReadDirection.Forward,
                Limit.None,
            )
        )

        assertThat(result).isEqualTo(StreamFactsByQueryResult.ContinuationNotFound(unknown))
    }

    // ===== FactStreamer: streamFactsBySubject =====

    @Test
    fun testStreamFactsBySubjectForward(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None)))
            .containsExactly(fact1, fact3)
        assertThat(streamToList(subjectRequest(BOB_SUBJECT_VALUE, ReadDirection.Forward, Limit.None)))
            .containsExactly(fact2)
    }

    @Test
    fun testStreamFactsBySubjectBackward(): Unit = runBlocking {
        val (fact1, _, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Backward, Limit.None)))
            .containsExactly(fact3, fact1)
    }

    @Test
    fun testStreamFactsBySubjectWithLimit(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_UPDATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        // Forward + limit 2 → the subject's two oldest facts
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.of(2))))
            .containsExactly(fact1, fact2)

        // Backward + limit 2 → the subject's two newest facts, newest first
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Backward, Limit.of(2))))
            .containsExactly(fact3, fact2)
    }

    @Test
    fun testStreamFactsBySubjectLimitCountsOnlyTheSubjectsFacts(): Unit = runBlocking {
        val (fact1, _, _, fact4) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(BOB_SUBJECT_VALUE, "USER_UPDATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        // Bob's facts in between must neither be emitted nor count towards the limit.
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.of(2))))
            .containsExactly(fact1, fact4)
    }

    @Test
    fun testStreamFactsBySubjectWithoutFacts(): Unit = runBlocking {
        appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))

        assertThat(streamToList(subjectRequest("USER:PETER", ReadDirection.Forward, Limit.None))).isEmpty()
    }

    @Test
    fun testStreamFactsBySubjectOfNonExistingStore(): Unit = runBlocking {
        val result = store.streamFactsBySubject(
            StreamFactsBySubjectRequest(nonExistingStore, Subject(ALICE_SUBJECT_VALUE), null, ReadDirection.Forward, Limit.None)
        )

        assertThat(result).isEqualTo(StreamFactsBySubjectResult.StoreNotFound(nonExistingStore))
    }

    @Test
    fun testStreamFactsBySubjectOnlyContainsFactsOfTheRequestedStore(): Unit = runBlocking {
        val otherStore = StoreName("other-store")
        store.create(CreateStoreRequest(otherStore))

        val fact = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload), otherStore)

        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None)))
            .containsExactly(fact)
    }

    @Test
    fun testStreamFactsBySubjectExcludesFactsAppendedAfterCall(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))

        // The head is pinned when the stream is requested, not when it is collected.
        val stream = (store.streamFactsBySubject(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None))
                as StreamFactsBySubjectResult.FactStream).facts

        appendStored(input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload))

        assertThat(withTimeout(10.seconds) { stream.toList() }).containsExactly(fact1)
    }

    @Test
    fun testStreamFactsBySubjectIsRepeatable(): Unit = runBlocking {
        val (fact1, _, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        val stream = (store.streamFactsBySubject(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None))
                as StreamFactsBySubjectResult.FactStream).facts

        val first = withTimeout(10.seconds) { stream.toList() }
        appendStored(input(ALICE_SUBJECT_VALUE, "USER_UNLOCKED", alicePayload))
        val second = withTimeout(10.seconds) { stream.toList() }

        assertThat(first).containsExactly(fact1, fact3)
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun testStreamFactsBySubjectOfLargeHistory(): Unit = runBlocking {
        val aliceFacts = appendLargeHistory().filter { it.subject.value == ALICE_SUBJECT_VALUE }

        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.None)))
            .containsExactlyElementsOf(aliceFacts)
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Backward, Limit.None)))
            .containsExactlyElementsOf(aliceFacts.reversed())
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Forward, Limit.of(333))))
            .containsExactlyElementsOf(aliceFacts.take(333))
        assertThat(streamToList(subjectRequest(ALICE_SUBJECT_VALUE, ReadDirection.Backward, Limit.of(333))))
            .containsExactlyElementsOf(aliceFacts.reversed().take(333))
    }

    // ===== FactStreamer: streamFactsByType =====

    @Test
    fun testStreamFactsByTypeForward(): Unit = runBlocking {
        val (fact1, _, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_LOCKED", bobPayload),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload),
            )
        )

        // Facts of one type, across subjects.
        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.None)))
            .containsExactly(fact1, fact3)
    }

    @Test
    fun testStreamFactsByTypeBackward(): Unit = runBlocking {
        val (fact1, _, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_LOCKED", bobPayload),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload),
            )
        )

        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Backward, Limit.None)))
            .containsExactly(fact3, fact1)
    }

    @Test
    fun testStreamFactsByTypeWithLimit(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload),
            )
        )

        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.of(2))))
            .containsExactly(fact1, fact2)
        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Backward, Limit.of(2))))
            .containsExactly(fact3, fact2)
    }

    @Test
    fun testStreamFactsByTypeLimitCountsOnlyTheTypesFacts(): Unit = runBlocking {
        val (fact1, _, _, fact4) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_LOCKED", bobPayload),
                input(BOB_SUBJECT_VALUE, "USER_UNLOCKED", bobPayload),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload),
            )
        )

        // Facts of other types in between must neither be emitted nor count towards the limit.
        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.of(2))))
            .containsExactly(fact1, fact4)
    }

    @Test
    fun testStreamFactsByTypeMatchesTheTypeExactly(): Unit = runBlocking {
        val exact = appendStored(input(ALICE_SUBJECT_VALUE, "com.acme.OrderPlaced", alicePayload))
        appendStored(input(ALICE_SUBJECT_VALUE, "com.acme.OrderPlaced.V2", alicePayload))

        assertThat(streamToList(typeRequest("com.acme.OrderPlaced", ReadDirection.Forward, Limit.None)))
            .containsExactly(exact)
        assertThat(streamToList(typeRequest("com.acme", ReadDirection.Forward, Limit.None))).isEmpty()
    }

    @Test
    fun testStreamFactsByTypeWithoutFacts(): Unit = runBlocking {
        appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))

        assertThat(streamToList(typeRequest("USER_DELETED", ReadDirection.Forward, Limit.None))).isEmpty()
    }

    @Test
    fun testStreamFactsByTypeOfNonExistingStore(): Unit = runBlocking {
        val result = store.streamFactsByType(
            StreamFactsByTypeRequest(nonExistingStore, FactType("USER_CREATED"), null, ReadDirection.Forward, Limit.None)
        )

        assertThat(result).isEqualTo(StreamFactsByTypeResult.StoreNotFound(nonExistingStore))
    }

    @Test
    fun testStreamFactsByTypeOnlyContainsFactsOfTheRequestedStore(): Unit = runBlocking {
        val otherStore = StoreName("other-store")
        store.create(CreateStoreRequest(otherStore))

        val fact = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload), otherStore)

        assertThat(streamToList(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.None)))
            .containsExactly(fact)
    }

    @Test
    fun testStreamFactsByTypeExcludesFactsAppendedAfterCall(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))

        val stream = (store.streamFactsByType(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.None))
                as StreamFactsByTypeResult.FactStream).facts

        appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))

        assertThat(withTimeout(10.seconds) { stream.toList() }).containsExactly(fact1)
    }

    @Test
    fun testStreamFactsByTypeExcludesFactsAppendedWhileCollected(): Unit = runBlocking {
        // More facts than a backend is likely to read at once, so collecting spans several reads.
        val facts = appendStored((0 until 300).map { input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload) })

        val stream = (store.streamFactsByType(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.None))
                as StreamFactsByTypeResult.FactStream).facts

        val collected = mutableListOf<Fact>()
        withTimeout(10.seconds) {
            stream.collect { fact ->
                if (collected.isEmpty()) appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))
                collected += fact
            }
        }

        assertThat(collected).containsExactlyElementsOf(facts)
    }

    @Test
    fun testStreamFactsByTypeIsRepeatable(): Unit = runBlocking {
        val (fact1, _, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_LOCKED", bobPayload),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload),
            )
        )

        val stream = (store.streamFactsByType(typeRequest("USER_CREATED", ReadDirection.Forward, Limit.None))
                as StreamFactsByTypeResult.FactStream).facts

        val first = withTimeout(10.seconds) { stream.toList() }
        appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))
        val second = withTimeout(10.seconds) { stream.toList() }

        assertThat(first).containsExactly(fact1, fact3)
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun testStreamFactsByTypeOfLargeHistory(): Unit = runBlocking {
        val evenFacts = appendLargeHistory().filterIndexed { index, _ -> index % 2 == 0 }

        assertThat(streamToList(typeRequest(EVEN_FACT_TYPE, ReadDirection.Forward, Limit.None)))
            .containsExactlyElementsOf(evenFacts)
        assertThat(streamToList(typeRequest(EVEN_FACT_TYPE, ReadDirection.Backward, Limit.None)))
            .containsExactlyElementsOf(evenFacts.reversed())
        assertThat(streamToList(typeRequest(EVEN_FACT_TYPE, ReadDirection.Forward, Limit.of(333))))
            .containsExactlyElementsOf(evenFacts.take(333))
    }

    // ===== FactStreamer: streamFactsByTags =====

    @Test
    fun testStreamFactsByOneTag(): Unit = runBlocking {
        val (alice, _, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                userInput("CHARLIE", "Charlie", role = "admin", region = "us"),
            )
        )

        assertThat(streamToList(tagsRequest(mapOf("role" to "admin"), ReadDirection.Forward, Limit.None)))
            .containsExactly(alice, charlie)
    }

    @Test
    fun testStreamFactsByTagsRequiresAllTags(): Unit = runBlocking {
        val (_, _, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                userInput("CHARLIE", "Charlie", role = "admin", region = "us"),
            )
        )

        // Only Charlie carries both tags; Alice and Bob carry one each.
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin", "region" to "us"), ReadDirection.Forward, Limit.None)))
            .containsExactly(charlie)
    }

    @Test
    fun testStreamFactsByTagsMatchesTheTagValueExactly(): Unit = runBlocking {
        appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))

        assertThat(streamToList(tagsRequest(mapOf("role" to "admins"), ReadDirection.Forward, Limit.None))).isEmpty()
        assertThat(streamToList(tagsRequest(mapOf("role" to "adm"), ReadDirection.Forward, Limit.None))).isEmpty()
    }

    @Test
    fun testStreamFactsByTagsBackward(): Unit = runBlocking {
        val (alice, _, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                userInput("CHARLIE", "Charlie", role = "admin", region = "us"),
            )
        )

        assertThat(streamToList(tagsRequest(mapOf("role" to "admin"), ReadDirection.Backward, Limit.None)))
            .containsExactly(charlie, alice)
    }

    @Test
    fun testStreamFactsByTagsWithLimit(): Unit = runBlocking {
        val (alice, _, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "eu"),
                userInput("CHARLIE", "Charlie", role = "admin", region = "eu"),
            )
        )

        // The limit counts matching facts only, whatever lies between them.
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin"), ReadDirection.Forward, Limit.of(1))))
            .containsExactly(alice)
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin", "region" to "eu"), ReadDirection.Backward, Limit.of(1))))
            .containsExactly(charlie)
    }

    @Test
    fun testStreamFactsByTagsWithoutMatches(): Unit = runBlocking {
        appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))

        assertThat(streamToList(tagsRequest(mapOf("role" to "guest"), ReadDirection.Forward, Limit.None))).isEmpty()
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin", "region" to "us"), ReadDirection.Forward, Limit.None)))
            .isEmpty()
    }

    @Test
    fun testStreamFactsByTagsOfNonExistingStore(): Unit = runBlocking {
        val result = store.streamFactsByTags(
            StreamFactsByTagsRequest(
                nonExistingStore,
                mapOf(TagKey("role") to TagValue("admin")),
                null,
                ReadDirection.Forward,
                Limit.None,
            )
        )

        assertThat(result).isEqualTo(StreamFactsByTagsResult.StoreNotFound(nonExistingStore))
    }

    @Test
    fun testStreamFactsByTagsExcludesFactsAppendedAfterCall(): Unit = runBlocking {
        val alice = appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))

        val stream = (store.streamFactsByTags(tagsRequest(mapOf("role" to "admin"), ReadDirection.Forward, Limit.None))
                as StreamFactsByTagsResult.FactStream).facts

        appendStored(userInput("CHARLIE", "Charlie", role = "admin", region = "eu"))

        assertThat(withTimeout(10.seconds) { stream.toList() }).containsExactly(alice)
    }

    @Test
    fun testStreamFactsByTagsIsRepeatable(): Unit = runBlocking {
        val alice = appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))

        val stream = (store.streamFactsByTags(tagsRequest(mapOf("role" to "admin"), ReadDirection.Forward, Limit.None))
                as StreamFactsByTagsResult.FactStream).facts

        val first = withTimeout(10.seconds) { stream.toList() }
        appendStored(userInput("CHARLIE", "Charlie", role = "admin", region = "eu"))
        val second = withTimeout(10.seconds) { stream.toList() }

        assertThat(first).containsExactly(alice)
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun testStreamFactsBySeveralTagsIsRepeatable(): Unit = runBlocking {
        val alice = appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))
        appendStored(userInput("BOB", "Bob", role = "user", region = "eu"))

        val stream = (store.streamFactsByTags(
            tagsRequest(mapOf("role" to "admin", "region" to "eu"), ReadDirection.Forward, Limit.None)
        ) as StreamFactsByTagsResult.FactStream).facts

        val first = withTimeout(10.seconds) { stream.toList() }
        val second = withTimeout(10.seconds) { stream.toList() }

        assertThat(first).containsExactly(alice)
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun testStreamFactsByTagsOfLargeHistory(): Unit = runBlocking {
        // 900 facts: every third carries role=admin, every fifth region=eu, so the tags match
        // different, interleaved sets. The 300 admin facts are more than a backend reads at once,
        // so matching the two tags has to continue across several reads of that tag's index.
        val inputs = (0 until 900).map { i ->
            val tags = buildMap {
                if (i % 3 == 0) put(TagKey("role"), TagValue("admin"))
                if (i % 5 == 0) put(TagKey("region"), TagValue("eu"))
            }
            input(ALICE_SUBJECT_VALUE, "FACT_$i", alicePayload, tags = tags)
        }
        val facts = inputs.chunked(AppendRequest.MAX_FACTS).flatMap { appendStored(it) }

        val admins = facts.filterIndexed { i, _ -> i % 3 == 0 }
        val adminsInEu = facts.filterIndexed { i, _ -> i % 3 == 0 && i % 5 == 0 }

        assertThat(streamToList(tagsRequest(mapOf("role" to "admin"), ReadDirection.Forward, Limit.None)))
            .containsExactlyElementsOf(admins)
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin", "region" to "eu"), ReadDirection.Forward, Limit.None)))
            .containsExactlyElementsOf(adminsInEu)
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin", "region" to "eu"), ReadDirection.Backward, Limit.None)))
            .containsExactlyElementsOf(adminsInEu.reversed())
        assertThat(streamToList(tagsRequest(mapOf("role" to "admin", "region" to "eu"), ReadDirection.Forward, Limit.of(7))))
            .containsExactlyElementsOf(adminsInEu.take(7))
    }

    // ===== FactStreamer: streamFactsByQuery =====

    @Test
    fun testStreamFactsByQueryWithOneFilterMatchesTheShorthands(): Unit = runBlocking {
        val (alice, bob, aliceLocked) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
            )
        )

        // A single filter is exactly the shorthand it constrains.
        assertThat(streamToList(query(FactFilter(subjects = setOf(Subject(ALICE_SUBJECT_VALUE))))))
            .containsExactly(alice, aliceLocked)
        assertThat(streamToList(query(FactFilter(types = setOf(FactType("USER_CREATED"))))))
            .containsExactly(alice, bob)
        assertThat(streamToList(query(FactFilter(tags = mapOf(TagKey("role") to TagValue("admin"))))))
            .containsExactly(alice, aliceLocked)
    }

    @Test
    fun testStreamFactsByQueryCombinesPredicatesWithAnd(): Unit = runBlocking {
        val (_, _, aliceLocked) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "admin", region = "eu"),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
            )
        )

        // Subject AND type AND tag: only the third fact satisfies all three.
        val filter = FactFilter(
            subjects = setOf(Subject(ALICE_SUBJECT_VALUE)),
            types = setOf(FactType("USER_LOCKED")),
            tags = mapOf(TagKey("role") to TagValue("admin")),
        )

        assertThat(streamToList(query(filter))).containsExactly(aliceLocked)
    }

    @Test
    fun testStreamFactsByQueryMatchesAnyValueOfAPredicate(): Unit = runBlocking {
        val (alice, bob, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                input(CHARLIE_SUBJECT_VALUE, "USER_LOCKED", charliePayload),
            )
        )

        // Several types in one filter match any of them.
        assertThat(streamToList(query(FactFilter(types = setOf(FactType("USER_CREATED"), FactType("USER_LOCKED"))))))
            .containsExactly(alice, bob, charlie)

        // The same for subjects.
        assertThat(streamToList(query(FactFilter(subjects = setOf(Subject(ALICE_SUBJECT_VALUE), Subject(CHARLIE_SUBJECT_VALUE))))))
            .containsExactly(alice, charlie)
    }

    @Test
    fun testStreamFactsByQueryCombinesOneTypeWithSeveralTags(): Unit = runBlocking {
        val (alice, _, _) = appendStored(
            listOf(
                // Right type, both tags.
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = adminInEu),
                // Right type, one tag missing.
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                // Both tags, wrong type.
                input(CHARLIE_SUBJECT_VALUE, "USER_LOCKED", charliePayload, tags = adminInEu),
            )
        )

        val filter = FactFilter(types = setOf(FactType("USER_CREATED")), tags = adminInEu)

        assertThat(streamToList(query(filter))).containsExactly(alice)
    }

    @Test
    fun testStreamFactsByQueryCombinesSeveralTypesWithSeveralTags(): Unit = runBlocking {
        val (alice, _, _, charlie) = appendStored(
            listOf(
                // Alice: right type, both tags.
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = adminInEu),
                // Bob: right type, only one tag.
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                // Wrong type, both tags.
                input(BOB_SUBJECT_VALUE, "USER_DELETED", bobPayload, tags = adminInEu),
                // Charlie: the other allowed type, both tags.
                input(CHARLIE_SUBJECT_VALUE, "USER_LOCKED", charliePayload, tags = adminInEu),
            )
        )

        val filter = FactFilter(
            types = setOf(FactType("USER_CREATED"), FactType("USER_LOCKED")),
            tags = adminInEu,
        )

        assertThat(streamToList(query(filter))).containsExactly(alice, charlie)
    }

    @Test
    fun testStreamFactsByQueryCombinesFiltersWithOr(): Unit = runBlocking {
        val (alice, _, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                input(CHARLIE_SUBJECT_VALUE, "USER_LOCKED", charliePayload, tags = mapOf(TagKey("role") to TagValue("guest"))),
            )
        )

        val result = streamToList(
            query(
                FactFilter(subjects = setOf(Subject(ALICE_SUBJECT_VALUE))),
                FactFilter(tags = mapOf(TagKey("role") to TagValue("guest"))),
            )
        )

        assertThat(result).containsExactly(alice, charlie)
    }

    @Test
    fun testStreamFactsByQueryEmitsAFactMatchingSeveralFiltersOnce(): Unit = runBlocking {
        val (alice, bob) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "admin", region = "eu"),
            )
        )

        // Alice satisfies both filters, yet is one fact.
        val result = streamToList(
            query(
                FactFilter(subjects = setOf(Subject(ALICE_SUBJECT_VALUE))),
                FactFilter(tags = mapOf(TagKey("role") to TagValue("admin"))),
            )
        )

        assertThat(result).containsExactly(alice, bob)
    }

    @Test
    fun testStreamFactsByQueryBackwardAndWithLimit(): Unit = runBlocking {
        val (alice, bob, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                userInput("CHARLIE", "Charlie", role = "admin", region = "us"),
            )
        )

        val filters = arrayOf(
            FactFilter(subjects = setOf(Subject(BOB_SUBJECT_VALUE))),
            FactFilter(tags = mapOf(TagKey("role") to TagValue("admin"))),
        )

        assertThat(streamToList(query(*filters), ReadDirection.Backward))
            .containsExactly(charlie, bob, alice)
        // The limit counts the merged result, not each filter.
        assertThat(streamToList(query(*filters), ReadDirection.Forward, Limit.of(2)))
            .containsExactly(alice, bob)
    }

    @Test
    fun testStreamFactsByQueryWithoutMatches(): Unit = runBlocking {
        appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))

        assertThat(streamToList(query(FactFilter(types = setOf(FactType("USER_DELETED")))))).isEmpty()
    }

    @Test
    fun testStreamFactsByQueryOfNonExistingStore(): Unit = runBlocking {
        val result = store.streamFactsByQuery(
            StreamFactsByQueryRequest(
                nonExistingStore,
                FactQuery(listOf(FactFilter(types = setOf(FactType("USER_CREATED"))))),
                null,
                ReadDirection.Forward,
                Limit.None,
            )
        )

        assertThat(result).isEqualTo(StreamFactsByQueryResult.StoreNotFound(nonExistingStore))
    }

    @Test
    fun testStreamFactsByQueryExcludesFactsAppendedAfterCall(): Unit = runBlocking {
        val alice = appendStored(userInput("ALICE", "Alice", role = "admin", region = "eu"))

        val stream = (store.streamFactsByQuery(
            StreamFactsByQueryRequest(
                testStore,
                FactQuery(listOf(FactFilter(types = setOf(FactType("USER_CREATED"))))),
                null,
                ReadDirection.Forward,
                Limit.None,
            )
        ) as StreamFactsByQueryResult.FactStream).facts

        appendStored(userInput("BOB", "Bob", role = "user", region = "us"))

        assertThat(withTimeout(10.seconds) { stream.toList() }).containsExactly(alice)
    }

    @Test
    fun testStreamFactsByQueryIsRepeatable(): Unit = runBlocking {
        val (alice, _, charlie) = appendStored(
            listOf(
                userInput("ALICE", "Alice", role = "admin", region = "eu"),
                userInput("BOB", "Bob", role = "user", region = "us"),
                input(CHARLIE_SUBJECT_VALUE, "USER_LOCKED", charliePayload, tags = adminInEu),
            )
        )

        val stream = (store.streamFactsByQuery(
            StreamFactsByQueryRequest(
                testStore,
                FactQuery(
                    listOf(
                        FactFilter(subjects = setOf(Subject(ALICE_SUBJECT_VALUE))),
                        FactFilter(types = setOf(FactType("USER_LOCKED")), tags = adminInEu),
                    )
                ),
                null,
                ReadDirection.Forward,
                Limit.None,
            )
        ) as StreamFactsByQueryResult.FactStream).facts

        val first = withTimeout(10.seconds) { stream.toList() }
        val second = withTimeout(10.seconds) { stream.toList() }

        assertThat(first).containsExactly(alice, charlie)
        assertThat(second).isEqualTo(first)
    }

    @Test
    fun testStreamFactsByQueryOfLargeHistory(): Unit = runBlocking {
        // 900 facts: every third is an admin, every fifth is in the EU, and the types alternate,
        // so every part of the query spans more than one read of a backend.
        val inputs = (0 until 900).map { i ->
            val tags = buildMap {
                if (i % 3 == 0) put(TagKey("role"), TagValue("admin"))
                if (i % 5 == 0) put(TagKey("region"), TagValue("eu"))
            }
            val subject = if (i % 2 == 0) ALICE_SUBJECT_VALUE else BOB_SUBJECT_VALUE
            val type = if (i % 2 == 0) EVEN_FACT_TYPE else ODD_FACT_TYPE
            input(subject, type, alicePayload, tags = tags)
        }
        val facts = inputs.chunked(AppendRequest.MAX_FACTS).flatMap { appendStored(it) }

        // Admins in the EU (every fifteenth), or anything of Bob's subject (every odd one).
        val expected = facts.filterIndexed { i, _ -> (i % 3 == 0 && i % 5 == 0) || i % 2 == 1 }

        val request = query(
            FactFilter(tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))),
            FactFilter(subjects = setOf(Subject(BOB_SUBJECT_VALUE))),
        )

        assertThat(streamToList(request)).containsExactlyElementsOf(expected)
        assertThat(streamToList(request, ReadDirection.Backward)).containsExactlyElementsOf(expected.reversed())
        assertThat(streamToList(request, ReadDirection.Forward, Limit.of(17)))
            .containsExactlyElementsOf(expected.take(17))
    }

    private val adminInEu = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))

    private fun query(vararg filters: FactFilter) = FactQuery(filters.toList())

    private suspend fun streamToList(
        query: FactQuery,
        direction: ReadDirection = ReadDirection.Forward,
        limit: Limit = Limit.None,
        continueAfter: FactId? = null,
    ): List<Fact> {
        val request = StreamFactsByQueryRequest(testStore, query, continueAfter, direction, limit)
        val stream = (store.streamFactsByQuery(request) as StreamFactsByQueryResult.FactStream).facts
        return withTimeout(10.seconds) { stream.toList() }
    }

    private fun tagsRequest(
        tags: Map<String, String>,
        direction: ReadDirection,
        limit: Limit,
        continueAfter: FactId? = null,
    ) = StreamFactsByTagsRequest(
        storeName = testStore,
        tags = tags.entries.associate { TagKey(it.key) to TagValue(it.value) },
        continueAfter = continueAfter,
        direction = direction,
        limit = limit,
    )

    private suspend fun streamToList(request: StreamFactsByTagsRequest): List<Fact> {
        val stream = (store.streamFactsByTags(request) as StreamFactsByTagsResult.FactStream).facts
        return withTimeout(10.seconds) { stream.toList() }
    }

    private fun typeRequest(
        type: String,
        direction: ReadDirection,
        limit: Limit,
        continueAfter: FactId? = null,
    ) = StreamFactsByTypeRequest(testStore, FactType(type), continueAfter, direction, limit)

    private suspend fun streamToList(request: StreamFactsByTypeRequest): List<Fact> {
        val stream = (store.streamFactsByType(request) as StreamFactsByTypeResult.FactStream).facts
        return withTimeout(10.seconds) { stream.toList() }
    }

    private fun subjectRequest(
        subject: String,
        direction: ReadDirection,
        limit: Limit,
        continueAfter: FactId? = null,
    ) = StreamFactsBySubjectRequest(testStore, Subject(subject), continueAfter, direction, limit)

    private suspend fun streamToList(request: StreamFactsRequest): List<Fact> {
        val stream = (store.streamFacts(request) as StreamFactsResult.FactStream).facts
        return withTimeout(10.seconds) { stream.toList() }
    }

    private suspend fun streamToList(request: StreamFactsBySubjectRequest): List<Fact> {
        val stream = (store.streamFactsBySubject(request) as StreamFactsBySubjectResult.FactStream).facts
        return withTimeout(10.seconds) { stream.toList() }
    }

    /**
     * Appends 2,000 facts, alternating between Alice and Bob, in appends of the maximum size.
     *
     * Large enough that a backend reading in batches needs several of them, so the
     * tests cover batch boundaries without knowing a backend's batch size.
     */
    private suspend fun appendLargeHistory(): List<Fact> {
        val inputs = (0 until 2_000).map { i ->
            val subject = if (i % 2 == 0) ALICE_SUBJECT_VALUE else BOB_SUBJECT_VALUE
            val type = if (i % 2 == 0) EVEN_FACT_TYPE else ODD_FACT_TYPE
            input(subject, type, """{ "index": $i }""".toFactPayload())
        }
        return inputs.chunked(AppendRequest.MAX_FACTS).flatMap { appendStored(it) }
    }









    @Test
    fun testConditionalAppendWithTagQuery(): Unit = runBlocking {

        val aliceTags = mapOf(TagKey("user") to TagValue("ALICE"))
        val tagQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = setOf(FactType("USER_CREATED")),
                    tags = mapOf(TagKey("user") to TagValue("ALICE")),
                )
            )
        )

        // append fact1: no matching USER_CREATED/user=ALICE exists yet → appended
        val appended1 = store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = aliceTags)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.TagQueryBased(failIfEventsMatch = tagQuery, after = null)
            )
        )
        assertThat(appended1).isInstanceOf(AppendResult.Appended::class.java)
        val fact1Id = (appended1 as AppendResult.Appended).factIds.single()

        // append fact2: only fact1 matches and we exclude everything up to it → appended
        val appended2 = store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload, tags = aliceTags)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.TagQueryBased(failIfEventsMatch = tagQuery, after = fact1Id)
            )
        )
        assertThat(appended2).isInstanceOf(AppendResult.Appended::class.java)

        // appending again without an `after` cursor sees fact1 and is rejected
        store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload, tags = aliceTags)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.TagQueryBased(failIfEventsMatch = tagQuery, after = null)
            )
        ).also {
            assertThat(it).isInstanceOf(AppendResult.AppendConditionViolated::class.java)
        }

        // append another user fact with another tag
        val bobTags = mapOf(TagKey("user") to TagValue("BOB"))
        val tagQuery2 = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = setOf(FactType("USER_CREATED")),
                    tags = mapOf(TagKey("user") to TagValue("BOB")),
                )
            )
        )

        val appended3 = store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = bobTags)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.TagQueryBased(failIfEventsMatch = tagQuery2, after = null)
            )
        )
        assertThat(appended3).isInstanceOf(AppendResult.Appended::class.java)
        val fact3Id = (appended3 as AppendResult.Appended).factIds.single()

        store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(BOB_SUBJECT_VALUE, "USER_LOCKED", bobPayload, tags = bobTags)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.TagQueryBased(failIfEventsMatch = tagQuery2, after = fact3Id)
            )
        ).also {
            assertThat(it).isInstanceOf(AppendResult.Appended::class.java)
        }

    }

    // ===== Tag queries require every tag of a query item =====
    //
    // A query item matches a fact only if the fact carries all of the item's tags. The
    // facts below deliberately carry the queried tags separately and interleaved, so a
    // backend cannot pass by looking at each tag's index on its own.

    private val courseTag = TagKey("course") to TagValue("c1")
    private val studentTag = TagKey("student") to TagValue("s1")
    private val semesterTag = TagKey("semester") to TagValue("2026")

    private suspend fun appendTagged(type: String, vararg tags: Pair<TagKey, TagValue>): Fact =
        appendStored(input(ALICE_SUBJECT_VALUE, type, alicePayload, tags = mapOf(*tags)))

    private suspend fun appendWithTagQueryCondition(query: TagQuery, after: FactId? = null): AppendResult =
        store.append(
            AppendRequest(
                storeName = testStore,
                facts = listOf(input(ALICE_SUBJECT_VALUE, "UNRELATED", alicePayload)),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.TagQueryBased(failIfEventsMatch = query, after = after),
            )
        )

    @Test
    fun testTagQueryRequiresAllTagsForTagOnlyCondition(): Unit = runBlocking {
        val query = TagQuery(listOf(TagOnlyQueryItem(mapOf(courseTag, studentTag))))

        appendTagged("COURSE_DEFINED", courseTag)
        appendTagged("STUDENT_REGISTERED", studentTag)
        appendTagged("COURSE_UPDATED", courseTag)

        assertThat(appendWithTagQueryCondition(query))
            .describedAs("no fact carries both tags yet")
            .isInstanceOf(AppendResult.Appended::class.java)

        appendTagged("STUDENT_SUBSCRIBED", courseTag, studentTag)

        assertThat(appendWithTagQueryCondition(query))
            .describedAs("a fact carrying both tags now exists")
            .isInstanceOf(AppendResult.AppendConditionViolated::class.java)
    }

    @Test
    fun testTagQueryRequiresAllTagsForTagTypeCondition(): Unit = runBlocking {
        val query = TagQuery(listOf(TagTypeItem(setOf(FactType("STUDENT_SUBSCRIBED")), mapOf(courseTag, studentTag))))

        appendTagged("STUDENT_SUBSCRIBED", courseTag)
        appendTagged("STUDENT_SUBSCRIBED", studentTag)
        appendTagged("STUDENT_SUBSCRIBED", TagKey("course") to TagValue("c2"), studentTag)
        appendTagged("STUDENT_UNSUBSCRIBED", courseTag, studentTag)

        assertThat(appendWithTagQueryCondition(query))
            .describedAs("no fact of the queried type carries both tags yet")
            .isInstanceOf(AppendResult.Appended::class.java)

        appendTagged("STUDENT_SUBSCRIBED", courseTag, studentTag)

        assertThat(appendWithTagQueryCondition(query))
            .describedAs("a fact of the queried type carrying both tags now exists")
            .isInstanceOf(AppendResult.AppendConditionViolated::class.java)
    }

    @Test
    fun testTagQueryRequiresAllTagsAfterPositionCondition(): Unit = runBlocking {
        val query = TagQuery(listOf(TagOnlyQueryItem(mapOf(courseTag, studentTag, semesterTag))))

        val cursor = appendTagged("STUDENT_SUBSCRIBED", courseTag, studentTag, semesterTag)
        appendTagged("STUDENT_SUBSCRIBED", courseTag, studentTag)
        appendTagged("STUDENT_SUBSCRIBED", studentTag, semesterTag)
        appendTagged("STUDENT_SUBSCRIBED", courseTag, semesterTag)

        assertThat(appendWithTagQueryCondition(query, after = cursor.id))
            .describedAs("the only fact carrying all three tags is not after the cursor")
            .isInstanceOf(AppendResult.Appended::class.java)

        appendTagged("STUDENT_SUBSCRIBED", courseTag, studentTag, semesterTag)

        assertThat(appendWithTagQueryCondition(query, after = cursor.id))
            .describedAs("a fact carrying all three tags now exists after the cursor")
            .isInstanceOf(AppendResult.AppendConditionViolated::class.java)
    }

    @Test
    fun testTagQueryRequiresAllTagsAcrossLongInterleaving(): Unit = runBlocking {
        val query = TagQuery(listOf(TagOnlyQueryItem(mapOf(courseTag, studentTag))))

        // Hundreds of facts alternate between the two tags without ever carrying both, so a
        // backend that intersects the tag indexes step by step has to take many steps before
        // it can rule out a match.
        store.append(
            testStore,
            (1..500).map { i ->
                input(ALICE_SUBJECT_VALUE, "INTERLEAVED", alicePayload, tags = mapOf(if (i % 2 == 0) courseTag else studentTag))
            }
        )

        assertThat(appendWithTagQueryCondition(query))
            .describedAs("no fact carries both tags")
            .isInstanceOf(AppendResult.Appended::class.java)

        appendTagged("STUDENT_SUBSCRIBED", courseTag, studentTag)

        assertThat(appendWithTagQueryCondition(query))
            .describedAs("a fact carrying both tags now exists after the interleaved facts")
            .isInstanceOf(AppendResult.AppendConditionViolated::class.java)
    }


    @Test
    fun testIsolationOfFactStoreInstances(): Unit = runBlocking {

        // two instances of fact stores should be treated separately
        // facts belong to one and only one fact store and cannot be "shared"
        // two fact store instances should be treated as two logical database instances
        // even if they share underlying infrastructure, like the same FoundationDB cluster

        val storeName1 = StoreName("store-1")
        val storeName2 = StoreName("store-2")

        store.create(CreateStoreRequest(storeName1))
        store.create(CreateStoreRequest(storeName2))

        val fact1 = appendStored(input(BOB_SUBJECT_VALUE, "USER_LOCKED", bobPayload), storeName1)
        val fact2 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload), storeName2)

        assertThat(store.existsById(ExistsByIdRequest(storeName1, fact1.id))).isEqualTo(ExistsByIdResult.Exists)
        assertThat(store.existsById(ExistsByIdRequest(storeName1, fact2.id))).isEqualTo(ExistsByIdResult.DoesNotExist)

        assertThat(store.existsById(ExistsByIdRequest(storeName2, fact1.id))).isEqualTo(ExistsByIdResult.DoesNotExist)
        assertThat(store.existsById(ExistsByIdRequest(storeName2, fact2.id))).isEqualTo(ExistsByIdResult.Exists)
    }

    @Test
    fun testAppendWithoutFactStore(): Unit = runBlocking {
        val result = store.append(nonExistingStore, userInput("TEST", "Test User", "user", "eu"))
        assertThat(result).isInstanceOf(AppendResult.StoreNotFound::class.java)
    }

    @Test
    fun testNewAppend(): Unit = runBlocking {

        val idempotencyKey = IdempotencyKey(UUID.randomUUID())
        val appendRequest = AppendRequest(
            storeName = testStore,
            facts = listOf(input("USER:DOMI", "USER_LOCKED", """{ "username": "DOMI" }""".toFactPayload())),
            idempotencyKey = idempotencyKey,
            condition = AppendCondition.ExpectedLastFact(
                subject = Subject("USER:DOMI"),
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
    fun testRemoveStore(): Unit = runBlocking {
        val storeName = StoreName("store-to-delete")
        store.create(CreateStoreRequest(storeName))

        val factInput = input("TEST_SUBJECT", "TEST_FACT_TYPE", """DATA""".toFactPayload())

        assertThat(store.append(storeName, factInput))
            .isInstanceOf(AppendResult.Appended::class.java)

        assertThat(store.remove(RemoveStoreRequest(storeName)))
            .isInstanceOf(RemoveStoreResult.StoreRemoved::class.java)

        // a second time should result in StoreNotFound
        assertThat(store.remove(RemoveStoreRequest(storeName)))
            .isInstanceOf(RemoveStoreResult.StoreNotFound::class.java)

        assertThat(store.append(storeName, factInput))
            .isInstanceOf(AppendResult.StoreNotFound::class.java)
    }

}
