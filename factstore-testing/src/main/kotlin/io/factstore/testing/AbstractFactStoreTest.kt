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
import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.*
import java.time.Instant
import java.util.UUID
import kotlin.system.measureTimeMillis
import kotlin.time.Duration.Companion.milliseconds

private const val ALICE_SUBJECT_VALUE = "USER:ALICE"
private const val BOB_SUBJECT_VALUE = "USER:BOB"
private const val CHARLIE_SUBJECT_VALUE = "USER:CHARLIE"

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
        metadata: Map<String, String> = emptyMap(),
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

        store.stream(StreamFactsRequest(testStore)).let { (it as StreamResult.FactStream).stream }.transform { batch -> batch.forEach { emit(it) } }.take(1).collect {
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
    fun testFindInTimeRange(): Unit = runBlocking {
        // appendedAt is server-assigned; small delays guarantee distinct, increasing timestamps.
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        delay(10.milliseconds)
        val fact2 = appendStored(
            input(ALICE_SUBJECT_VALUE, "USER_UPDATED", """{ "username": "Alice", "status": "active" }""".toFactPayload())
        )
        delay(10.milliseconds)
        val fact3 = appendStored(input(BOB_SUBJECT_VALUE, "USER_DELETED", bobPayload))

        // Half-open [fact1, fact3): covers fact1 + fact2, excludes fact3 (end is exclusive).
        val result = store.findInTimeRange(
            FindInTimeRangeRequest(
                storeName = testStore,
                timeRange = TimeRange(
                    start = fact1.appendedAt,
                    end = fact3.appendedAt
                )
            )
        )

        assertThat(result).isInstanceOf(FindInTimeRangeResult.Found::class.java)
        val foundFacts = (result as FindInTimeRangeResult.Found).facts
        assertThat(foundFacts).containsExactlyInAnyOrder(fact1, fact2)
        assertThat(foundFacts).doesNotContain(fact3)
    }

    @Test
    fun testFindInTimeRangeBoundariesAreHalfOpen(): Unit = runBlocking {
        // Use the fact's own server-assigned timestamp as the boundary, so the
        // assertion is exact regardless of the backend's timestamp precision.
        val fact = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        val t = fact.appendedAt

        // end is exclusive: a fact exactly at `end` must NOT be returned.
        val endExclusive = store.findInTimeRange(
            FindInTimeRangeRequest(
                storeName = testStore,
                timeRange = TimeRange(start = t.minusSeconds(10), end = t)
            )
        )
        assertThat(endExclusive).isInstanceOf(FindInTimeRangeResult.Found::class.java)
        assertThat((endExclusive as FindInTimeRangeResult.Found).facts).isEmpty()

        // start is inclusive: a fact exactly at `start` must be returned.
        val startInclusive = store.findInTimeRange(
            FindInTimeRangeRequest(
                storeName = testStore,
                timeRange = TimeRange(start = t, end = t.plusSeconds(10))
            )
        )
        assertThat(startInclusive).isInstanceOf(FindInTimeRangeResult.Found::class.java)
        assertThat((startInclusive as FindInTimeRangeResult.Found).facts)
            .containsExactly(fact)
    }

    @Test
    fun testFindInTimeRangeWithOpenBounds(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        delay(10.milliseconds)
        val fact2 = appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))
        delay(10.milliseconds)
        val fact3 = appendStored(input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload))

        // from(t2): unbounded above, inclusive lower → fact2 and fact3
        val fromResult = store.findInTimeRange(
            FindInTimeRangeRequest(testStore, TimeRange.from(fact2.appendedAt))
        )
        assertThat((fromResult as FindInTimeRangeResult.Found).facts).containsExactly(fact2, fact3)

        // until(t2): unbounded below, exclusive upper → fact1 only
        val untilResult = store.findInTimeRange(
            FindInTimeRangeRequest(testStore, TimeRange.until(fact2.appendedAt))
        )
        assertThat((untilResult as FindInTimeRangeResult.Found).facts).containsExactly(fact1)

        // unbounded: every fact
        val allResult = store.findInTimeRange(
            FindInTimeRangeRequest(testStore, TimeRange.unbounded)
        )
        assertThat((allResult as FindInTimeRangeResult.Found).facts).containsExactly(fact1, fact2, fact3)
    }

    @Test
    fun testFindInTimeRangeForNonExistingStore(): Unit = runBlocking {
        val result = store.findInTimeRange(
            FindInTimeRangeRequest(
                storeName = nonExistingStore,
                timeRange = TimeRange(
                    start = Instant.now().minusSeconds(120),
                    end = Instant.now().plusSeconds(10)
                )
            )
        )

        assertThat(result).isInstanceOf(FindInTimeRangeResult.StoreNotFound::class.java)
    }

    @Test
    fun testFindInTimeRangeWithLimit(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        delay(10.milliseconds)
        val fact2 = appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))
        delay(10.milliseconds)
        val fact3 = appendStored(input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload))

        val result = store.findInTimeRange(
            FindInTimeRangeRequest(
                storeName = testStore,
                timeRange = TimeRange.until(fact3.appendedAt.plusSeconds(10)),
                limit = Limit.of(2),
            )
        )

        assertThat(result).isInstanceOf(FindInTimeRangeResult.Found::class.java)
        // Forward + limit 2 → oldest two facts
        assertThat((result as FindInTimeRangeResult.Found).facts).containsExactly(fact1, fact2)
    }

    @Test
    fun testFindInTimeRangeWithReadDirectionBackward(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        delay(10.milliseconds)
        val fact2 = appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))
        delay(10.milliseconds)
        val fact3 = appendStored(input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload))

        val result = store.findInTimeRange(
            FindInTimeRangeRequest(
                storeName = testStore,
                timeRange = TimeRange.until(fact3.appendedAt.plusSeconds(10)),
                direction = ReadDirection.Backward,
            )
        )

        assertThat(result).isInstanceOf(FindInTimeRangeResult.Found::class.java)
        // Backward, no limit → newest first
        assertThat((result as FindInTimeRangeResult.Found).facts).containsExactly(fact3, fact2, fact1)
    }

    @Test
    fun testFindInTimeRangeWithLimitAndBackwardDirection(): Unit = runBlocking {
        val fact1 = appendStored(input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload))
        delay(10.milliseconds)
        val fact2 = appendStored(input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload))
        delay(10.milliseconds)
        val fact3 = appendStored(input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload))

        val result = store.findInTimeRange(
            FindInTimeRangeRequest(
                storeName = testStore,
                timeRange = TimeRange.until(fact3.appendedAt.plusSeconds(10)),
                limit = Limit.of(2),
                direction = ReadDirection.Backward,
            )
        )

        assertThat(result).isInstanceOf(FindInTimeRangeResult.Found::class.java)
        // Backward + limit 2 → the 2 most recent facts, newest first
        assertThat((result as FindInTimeRangeResult.Found).facts).containsExactly(fact3, fact2)
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
        val subjectFacts = store.findBySubject(FindBySubjectRequest(testStore, Subject(ALICE_SUBJECT_VALUE)))
        assertThat(subjectFacts).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((subjectFacts as FindBySubjectResult.Found).facts).hasSize(2)
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
    fun testSubjectQueries(): Unit = runBlocking {

        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        val aliceResult = store.findBySubject(FindBySubjectRequest(testStore, Subject(ALICE_SUBJECT_VALUE)))
        assertThat(aliceResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((aliceResult as FindBySubjectResult.Found).facts)
            .containsExactly(fact1, fact3)

        val bobResult = store.findBySubject(FindBySubjectRequest(testStore, Subject(BOB_SUBJECT_VALUE)))
        assertThat(bobResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((bobResult as FindBySubjectResult.Found).facts)
            .containsExactly(fact2)

        val peterResult = store.findBySubject(FindBySubjectRequest(testStore, Subject("USER:PETER")))
        assertThat(peterResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((peterResult as FindBySubjectResult.Found).facts).isEmpty()

        val unknownResult = store.findBySubject(FindBySubjectRequest(testStore, Subject("UNKNOWN:UNKNOWN")))
        assertThat(unknownResult).isInstanceOf(FindBySubjectResult.Found::class.java)
        assertThat((unknownResult as FindBySubjectResult.Found).facts).isEmpty()
    }

    @Test
    fun findBySubjectForNonExistingFactstore(): Unit = runBlocking {
        val result = store.findBySubject(FindBySubjectRequest(nonExistingStore, Subject("SOME_TYPE:SOME_ID")))
        assertThat(result).isInstanceOf(FindBySubjectResult.StoreNotFound::class.java)
    }

    @Test
    fun testFindBySubjectWithLimit(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_UPDATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        val result = store.findBySubject(
            FindBySubjectRequest(
                storeName = testStore,
                subject = Subject(ALICE_SUBJECT_VALUE),
                limit = Limit.of(2),
            )
        )

        assertThat(result).isInstanceOf(FindBySubjectResult.Found::class.java)
        // Forward + limit 2 → first two facts appended
        assertThat((result as FindBySubjectResult.Found).facts).containsExactly(fact1, fact2)
    }

    @Test
    fun testFindBySubjectWithReadDirectionBackward(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_UPDATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        val result = store.findBySubject(
            FindBySubjectRequest(
                storeName = testStore,
                subject = Subject(ALICE_SUBJECT_VALUE),
                direction = ReadDirection.Backward,
            )
        )

        assertThat(result).isInstanceOf(FindBySubjectResult.Found::class.java)
        // Backward, no limit → newest first
        assertThat((result as FindBySubjectResult.Found).facts).containsExactly(fact3, fact2, fact1)
    }

    @Test
    fun testFindBySubjectWithLimitAndBackwardDirection(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_UPDATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_LOCKED", alicePayload),
            )
        )

        val result = store.findBySubject(
            FindBySubjectRequest(
                storeName = testStore,
                subject = Subject(ALICE_SUBJECT_VALUE),
                limit = Limit.of(2),
                direction = ReadDirection.Backward,
            )
        )

        assertThat(result).isInstanceOf(FindBySubjectResult.Found::class.java)
        // Backward + limit 2 → the 2 most recently appended facts, newest first
        assertThat((result as FindBySubjectResult.Found).facts).containsExactly(fact3, fact2)
    }


    @Test
    fun testWithMetadata(): Unit = runBlocking {

        val (fact1, fact2) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, metadata = mapOf("test" to "123", "loc" to "world")),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload),
            )
        )

        assertThat(store.findById(FindByIdRequest(testStore, fact1.id))).isInstanceOf(FindByIdResult.Found::class.java)
        assertThat((store.findById(FindByIdRequest(testStore, fact1.id)) as FindByIdResult.Found).fact).isEqualTo(fact1)
        assertThat(store.findById(FindByIdRequest(testStore, fact2.id))).isInstanceOf(FindByIdResult.Found::class.java)
        assertThat((store.findById(FindByIdRequest(testStore, fact2.id)) as FindByIdResult.Found).fact).isEqualTo(fact2)
    }

    @Test
    fun appendEventsWithTagsAndFindThem(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("role") to TagValue("user"), TagKey("region") to TagValue("us"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("us"))),
            )
        )

        // --- Query 1: Find all role=admin (AND semantics → fact1 + fact3, both have role=admin)
        val adminResult = store.findByTags(FindByTagsRequest(testStore, listOf(TagKey("role") to TagValue("admin"))))
        assertThat(adminResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((adminResult as FindByTagsResult.Found).facts).containsExactly(fact1, fact3)

        // --- Query 2: Find all region=us (AND semantics → fact2 + fact3, both have region=us)
        val usResult = store.findByTags(FindByTagsRequest(testStore, listOf(TagKey("region") to TagValue("us"))))
        assertThat(usResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((usResult as FindByTagsResult.Found).facts).containsExactly(fact2, fact3)

        // --- Query 3: Find role=admin AND region=eu (AND semantics → fact1 only, the only fact with both tags)
        val adminAndEuResult = store.findByTags(
            FindByTagsRequest(
                storeName = testStore,
                tags = listOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))
            )
        )
        assertThat(adminAndEuResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((adminAndEuResult as FindByTagsResult.Found).facts).containsExactly(fact1)

        // --- Query 4: Non-existent tag → empty
        val noFactsResult = store.findByTags(FindByTagsRequest(testStore, listOf(TagKey("region") to TagValue("asia"))))
        assertThat(noFactsResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((noFactsResult as FindByTagsResult.Found).facts).isEmpty()

        // --- Query 5: Find role=admin AND region=us (AND semantics → fact3 only, the only fact with both tags)
        val adminAndUsResult = store.findByTags(
            FindByTagsRequest(
                storeName = testStore,
                tags = listOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("us"))
            )
        )
        assertThat(adminAndUsResult).isInstanceOf(FindByTagsResult.Found::class.java)
        assertThat((adminAndUsResult as FindByTagsResult.Found).facts).containsExactly(fact3)

        // --- Query 6: Direct lookup still works as before
        val fact1Loaded = store.findById(FindByIdRequest(testStore, fact1.id))
        assertThat(fact1Loaded).isInstanceOf(FindByIdResult.Found::class.java)
        println(fact1Loaded)
    }

    @Test
    fun testFindByTagsWithNonExistingFactstore(): Unit = runBlocking {
        val result = store.findByTags(
            FindByTagsRequest(
                storeName = nonExistingStore,
                tags = listOf(TagKey("region") to TagValue("asia"))
            )
        )

        assertThat(result).isInstanceOf(FindByTagsResult.StoreNotFound::class.java)
    }

    @Test
    fun testFindByTagsWithLimit(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
            )
        )

        val result = store.findByTags(
            FindByTagsRequest(
                storeName = testStore,
                tags = listOf(TagKey("role") to TagValue("admin")),
                limit = Limit.of(2),
            )
        )

        assertThat(result).isInstanceOf(FindByTagsResult.Found::class.java)
        // Forward + limit 2 → first two matching facts
        assertThat((result as FindByTagsResult.Found).facts).containsExactly(fact1, fact2)
    }

    @Test
    fun testFindByTagsWithReadDirectionBackward(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
            )
        )

        val result = store.findByTags(
            FindByTagsRequest(
                storeName = testStore,
                tags = listOf(TagKey("role") to TagValue("admin")),
                direction = ReadDirection.Backward,
            )
        )

        assertThat(result).isInstanceOf(FindByTagsResult.Found::class.java)
        // Backward, no limit → newest first
        assertThat((result as FindByTagsResult.Found).facts).containsExactly(fact3, fact2, fact1)
    }

    @Test
    fun testFindByTagsWithLimitAndBackwardDirection(): Unit = runBlocking {
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("role") to TagValue("admin"))),
            )
        )

        val result = store.findByTags(
            FindByTagsRequest(
                storeName = testStore,
                tags = listOf(TagKey("role") to TagValue("admin")),
                limit = Limit.of(2),
                direction = ReadDirection.Backward,
            )
        )

        assertThat(result).isInstanceOf(FindByTagsResult.Found::class.java)
        // Backward + limit 2 → the 2 most recently appended matching facts, newest first
        assertThat((result as FindByTagsResult.Found).facts).containsExactly(fact3, fact2)
    }

    @Test
    fun testFindByTagsWithLimitAndBackwardDirectionMultipleTags(): Unit = runBlocking {
        // Verifies that limit + direction work correctly on the multi-tag intersection path
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu"))),
            )
        )

        val result = store.findByTags(
            FindByTagsRequest(
                storeName = testStore,
                tags = listOf(TagKey("role") to TagValue("admin"), TagKey("region") to TagValue("eu")),
                limit = Limit.of(2),
                direction = ReadDirection.Backward,
            )
        )

        assertThat(result).isInstanceOf(FindByTagsResult.Found::class.java)
        // Backward + limit 2 on multi-tag intersection → the 2 most recently appended matching facts
        assertThat((result as FindByTagsResult.Found).facts).containsExactly(fact3, fact2)
    }

    @Test
    fun testLimitLargerThanResultSetReturnsAll(): Unit = runBlocking {
        val (fact1, fact2) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload),
                input(ALICE_SUBJECT_VALUE, "USER_UPDATED", alicePayload),
            )
        )

        val result = store.findBySubject(
            FindBySubjectRequest(
                storeName = testStore,
                subject = Subject(ALICE_SUBJECT_VALUE),
                limit = Limit.of(100),
            )
        )

        assertThat(result).isInstanceOf(FindBySubjectResult.Found::class.java)
        // Limit larger than actual result set → all facts returned
        assertThat((result as FindBySubjectResult.Found).facts).containsExactly(fact1, fact2)
    }

    @OptIn(FlowPreview::class)
    @Test
    fun testFactStreaming(): Unit = runBlocking {

        val collectedFacts = mutableListOf<Fact>()
        val firstThreeReceived = CompletableDeferred<Unit>()

        // Start streaming from beginning
        val streamJob = launch {
            store.stream(StreamFactsRequest(testStore))
                .let { (it as StreamResult.FactStream).stream }
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

        val streamedEvents = store.stream(
            StreamFactsRequest(testStore, startPosition = StartPosition.After(fact1.id))
        ).let { (it as StreamResult.FactStream).stream }
            .transform { batch -> batch.forEach { emit(it) } }
            .take(2)
            .toList()

        assertThat(streamedEvents).containsExactly(fact2, fact3)

        // ---- Test non-existing fact ----

        val nonExistingFactId = FactId.generate()

        val streamResult = store.stream(
            StreamFactsRequest(testStore, startPosition = StartPosition.After(nonExistingFactId))
        )

        assertThat(streamResult).isInstanceOf(StreamResult.FactIdNotFound::class.java)
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
            store.stream(
                StreamFactsRequest(testStore, startPosition = StartPosition.End)
            )
                .let { (it as StreamResult.FactStream).stream }
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
    fun testStreamingNonExistingFactstore(): Unit = runBlocking {
        val streamResult = store.stream(StreamFactsRequest(nonExistingStore))
        assertThat(streamResult).isInstanceOf(StreamResult.StoreNotFound::class.java)
    }

    @Test
    fun testFindByTagQuery(): Unit = runBlocking {

        // define facts to append

        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))),
                input(BOB_SUBJECT_VALUE, "USER_CREATED", bobPayload, tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))),
            )
        )


        // Test 1: Query with a single tag (username = "bob")
        val bobQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("username") to TagValue("bob"))
                )
            )
        )

        val bobFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, bobQuery))
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

        val multipleTagsFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, multipleTagsQuery))
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

        val noMatchFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, noMatchQuery))
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

        val multipleTypesFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, multipleTypesQuery))
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

        val complexFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, complexQuery))
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

        val noMatchingTagFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, noMatchingTagQuery))
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

        val noMatchingTypeFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, noMatchingTypeQuery))
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

        val tagsNoFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, tagsNoFactsQuery))
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

        val differentTagsFacts = store.findByTagQuery(FindByTagQueryRequest(testStore, differentTagsQuery))
        assertThat(differentTagsFacts).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((differentTagsFacts as FindByTagQueryResult.Found).facts).containsExactly(fact3)

    }

    @Test
    fun testMultipleFactTypesOneQueryItem(): Unit = runBlocking {
        // Create facts with different types
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))),
                input(BOB_SUBJECT_VALUE, "USER_UPDATED", bobPayload, tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))),
            )
        )

        // Query for facts with types "USER_CREATED" or "USER_UPDATED" and with tags "username" = "alice"
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf("USER_CREATED".toFactType(), "USER_UPDATED".toFactType()),
                    tags = listOf(TagKey("username") to TagValue("alice"))
                )
            )
        )

        val result = store.findByTagQuery(FindByTagQueryRequest(testStore, query))

        // Expecting fact1 only (username = alice, type = "USER_CREATED")
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts).containsExactly(fact1)
    }

    @Test
    fun testMultipleQueryItems(): Unit = runBlocking {
        // Create facts with different types and tags
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))),
                input(BOB_SUBJECT_VALUE, "USER_UPDATED", bobPayload, tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))),
            )
        )

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

        val result = store.findByTagQuery(FindByTagQueryRequest(testStore, query))

        // Expecting fact2 (username = bob, region = us, type = USER_UPDATED)
        // Expecting fact3 (region = us, type = USER_CREATED, but no username filter for 'bob')
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts).containsExactlyInAnyOrder(fact2, fact3)

    }

    @Test
    fun testMixedTypesAndTagsQueryItems(): Unit = runBlocking {
        // Create facts with different types and tags
        val (fact1, fact2, fact3) = appendStored(
            listOf(
                input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu"))),
                input(BOB_SUBJECT_VALUE, "USER_UPDATED", bobPayload, tags = mapOf(TagKey("username") to TagValue("bob"), TagKey("region") to TagValue("us"))),
                input(CHARLIE_SUBJECT_VALUE, "USER_CREATED", charliePayload, tags = mapOf(TagKey("username") to TagValue("charlie"), TagKey("region") to TagValue("us"))),
            )
        )

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

        val result = store.findByTagQuery(FindByTagQueryRequest(testStore, query))

        // Expecting fact2 (username = bob, region = us, type = USER_UPDATED)
        // Expecting fact1 (username = alice, region = eu, type = USER_CREATED)
        assertThat(result).isInstanceOf(FindByTagQueryResult.Found::class.java)
        assertThat((result as FindByTagQueryResult.Found).facts).containsExactlyInAnyOrder(fact2, fact1)
    }

    @Test
    fun testNoMatchingFacts(): Unit = runBlocking {
        // Create facts with different types and tags
        appendStored(
            input(ALICE_SUBJECT_VALUE, "USER_CREATED", alicePayload, tags = mapOf(TagKey("username") to TagValue("alice"), TagKey("region") to TagValue("eu")))
        )

        // Query for facts with a non-matching type and tag
        val query = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_UPDATED")),
                    tags = listOf(TagKey("username") to TagValue("bob"))
                )
            )
        )

        val result = store.findByTagQuery(FindByTagQueryRequest(testStore, query))

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

            input(
                subject = "USER:user-$index",
                type = "USER_CREATED",
                payload = """{ "username": "user$index" }""".toFactPayload(),
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
            input(
                subject = "USER:user-${FactId.generate()}",
                type = "USER_CREATED",
                payload = """{ "username": "user" }""".toFactPayload(),
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

        val result = store.findByTagQuery(FindByTagQueryRequest(testStore, query))


        measureTimeMillis {
            store.findByTagQuery(FindByTagQueryRequest(testStore, query))
        }.also { println(it) }

        measureTimeMillis {
            store.findByTagQuery(
                FindByTagQueryRequest(
                    storeName = testStore,
                    query = TagQuery(
                        listOf(
                            TagTypeItem(
                                types = listOf(FactType("USER_CREATED")),
                                tags = listOf(TagKey("role") to TagValue("custom"))
                            )
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
                FindByTagQueryRequest(
                    storeName = nonExistingStore,
                    query = TagQuery(
                        listOf(
                            TagTypeItem(
                                types = listOf(FactType("USER_CREATED")),
                                tags = listOf(TagKey("role") to TagValue("custom"))
                            )
                        )
                    )
                )
            )
        )
            .isInstanceOf(FindByTagQueryResult.StoreNotFound::class.java)
    }


    @Test
    fun testConditionalAppendWithTagQuery(): Unit = runBlocking {

        val aliceTags = mapOf(TagKey("user") to TagValue("ALICE"))
        val tagQuery = TagQuery(
            queryItems = listOf(
                TagTypeItem(
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("user") to TagValue("ALICE")),
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
                    types = listOf(FactType("USER_CREATED")),
                    tags = listOf(TagKey("user") to TagValue("BOB")),
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
        store.handle(CreateStoreRequest(storeName))

        val factInput = input("TEST_SUBJECT", "TEST_FACT_TYPE", """DATA""".toFactPayload())

        assertThat(store.append(storeName, factInput))
            .isInstanceOf(AppendResult.Appended::class.java)

        assertThat(store.handle(RemoveStoreRequest(storeName)))
            .isInstanceOf(RemoveStoreResult.StoreRemoved::class.java)

        // a second time should result in StoreNotFound
        assertThat(store.handle(RemoveStoreRequest(storeName)))
            .isInstanceOf(RemoveStoreResult.StoreNotFound::class.java)

        assertThat(store.append(storeName, factInput))
            .isInstanceOf(AppendResult.StoreNotFound::class.java)
    }

}
