package io.factstore.server.grpc

import com.google.protobuf.ByteString
import io.factstore.grpc.v1.*
import io.quarkus.grpc.GrpcClient
import io.quarkus.test.junit.QuarkusTest
import io.smallrye.mutiny.coroutines.awaitSuspending
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.jdk9.asFlow
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.*
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import java.util.UUID

@QuarkusTest
@TestMethodOrder(OrderAnnotation::class)
@TestInstance(PER_CLASS)
class GrpcFactServiceTest {

    @GrpcClient
    lateinit var factService: FactService

    @GrpcClient
    lateinit var storeService: StoreService

    // Assigned by the server during the Order(1) append and reused by later ordered tests.
    lateinit var seedFactId: String
    val seedIdempotencyKey: String = UUID.randomUUID().toString()

    companion object {
        const val STORE = "grpc-fact-store"
        const val SUBJECT = "order-99"
    }

    @BeforeAll
    fun setUp(): Unit = runBlocking {
        storeService.createStore(createStoreRequest { name = STORE }).awaitSuspending()
    }

    // ─── AppendFacts ─────────────────────────────────────────────────────────

    @Test
    @Order(1)
    @DisplayName("AppendFacts - should return FactsAppended when facts are new")
    fun appendFacts(): Unit = runBlocking {
        val ikey = seedIdempotencyKey
        val response = factService.appendFacts(appendFactsRequest {
            storeName = STORE
            idempotencyKey = ikey
            facts += factInput {
                type = "order.created"
                subject = SUBJECT
                payload = factPayload { data = ByteString.copyFromUtf8("{}") }
                tags["region"] = "eu"
            }
        }).awaitSuspending()

        assertThat(response.hasAppended()).isTrue()

        seedFactId = response.appended.factIdsList.single()
    }

    @Test
    @Order(2)
    @DisplayName("AppendFacts - should return AlreadyApplied when idempotency key was already used")
    fun appendFactsAlreadyApplied(): Unit = runBlocking {
        val ikey = seedIdempotencyKey
        val response = factService.appendFacts(appendFactsRequest {
            storeName = STORE
            idempotencyKey = ikey
            facts += factInput {
                type = "order.created"
                subject = SUBJECT
                payload = factPayload { data = ByteString.copyFromUtf8("{}") }
            }
        }).awaitSuspending()

        assertThat(response.hasAlreadyApplied()).isTrue()
    }

    @Test
    @Order(3)
    @DisplayName("AppendFacts - should return ConditionViolated when condition is not satisfied")
    fun appendFactsConditionViolated(): Unit = runBlocking {
        val wrongId = UUID.randomUUID().toString()
        val response = factService.appendFacts(appendFactsRequest {
            storeName = STORE
            facts += factInput {
                type = "order.updated"
                subject = SUBJECT
                payload = factPayload { data = ByteString.copyFromUtf8("{}") }
            }
            condition = appendCondition {
                expectedLastFact = expectedLastFact {
                    subject = SUBJECT
                    expectedLastFactId = wrongId
                }
            }
        }).awaitSuspending()

        assertThat(response.hasConditionViolated()).isTrue()
    }

    @Test
    @Order(5)
    @DisplayName("AppendFacts - should return StoreNotFound when store does not exist")
    fun appendFactsStoreNotFound(): Unit = runBlocking {
        val response = factService.appendFacts(appendFactsRequest {
            storeName = "ghost-store"
            facts += factInput {
                type = "order.created"
                subject = SUBJECT
                payload = factPayload { data = ByteString.copyFromUtf8("{}") }
            }
        }).awaitSuspending()

        assertThat(response.hasStoreNotFound()).isTrue()
        assertThat(response.storeNotFound.storeName).isEqualTo("ghost-store")
    }

    // ─── GetFact ─────────────────────────────────────────────────────────────

    @Test
    @Order(6)
    @DisplayName("GetFact - should return FactFound with the correct fact")
    fun getFact(): Unit = runBlocking {
        val response = factService.getFact(getFactRequest {
            storeName = STORE
            factId = seedFactId
        }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.fact.id).isEqualTo(seedFactId)
        assertThat(response.found.fact.subject).isEqualTo(SUBJECT)
        assertThat(response.found.fact.type).isEqualTo("order.created")
    }

    @Test
    @Order(7)
    @DisplayName("GetFact - should return FactNotFound when fact does not exist")
    fun getFactNotFound(): Unit = runBlocking {
        val response = factService.getFact(getFactRequest {
            storeName = STORE
            factId = UUID.randomUUID().toString()
        }).awaitSuspending()

        assertThat(response.hasNotFound()).isTrue()
    }

    @Test
    @Order(8)
    @DisplayName("GetFact - should return StoreNotFound when store does not exist")
    fun getFactStoreNotFound(): Unit = runBlocking {
        val response = factService.getFact(getFactRequest {
            storeName = "ghost-store"
            factId = seedFactId
        }).awaitSuspending()

        assertThat(response.hasStoreNotFound()).isTrue()
        assertThat(response.storeNotFound.storeName).isEqualTo("ghost-store")
    }

    // ─── FactExists ──────────────────────────────────────────────────────────

    @Test
    @Order(9)
    @DisplayName("FactExists - should return FactPresent when fact exists")
    fun factExists(): Unit = runBlocking {
        val response = factService.factExists(factExistsRequest {
            storeName = STORE
            factId = seedFactId
        }).awaitSuspending()

        assertThat(response.hasPresent()).isTrue()
    }

    @Test
    @Order(10)
    @DisplayName("FactExists - should return FactNotFound when fact does not exist")
    fun factDoesNotExist(): Unit = runBlocking {
        val response = factService.factExists(factExistsRequest {
            storeName = STORE
            factId = UUID.randomUUID().toString()
        }).awaitSuspending()

        assertThat(response.hasAbsent()).isTrue()
    }

    @Test
    @Order(11)
    @DisplayName("FactExists - should return StoreNotFound when store does not exist")
    fun factExistsStoreNotFound(): Unit = runBlocking {
        val response = factService.factExists(factExistsRequest {
            storeName = "ghost-store"
            factId = seedFactId
        }).awaitSuspending()

        assertThat(response.hasStoreNotFound()).isTrue()
        assertThat(response.storeNotFound.storeName).isEqualTo("ghost-store")
    }

    // ─── FindFactsBySubject ───────────────────────────────────────────────────

    @Test
    @Order(12)
    @DisplayName("FindFactsBySubject - should return FactsFound with matching facts")
    fun findFactsBySubject(): Unit = runBlocking {
        val response = factService.findFactsBySubject(findFactsBySubjectRequest {
            storeName = STORE
            subject = SUBJECT
        }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.factsList).hasSize(1)
        assertThat(response.found.factsList.first().subject).isEqualTo(SUBJECT)
    }

    @Test
    @Order(13)
    @DisplayName("FindFactsBySubject - should return StoreNotFound when store does not exist")
    fun findFactsBySubjectStoreNotFound(): Unit = runBlocking {
        val response = factService.findFactsBySubject(findFactsBySubjectRequest {
            storeName = "ghost-store"
            subject = SUBJECT
        }).awaitSuspending()

        assertThat(response.hasStoreNotFound()).isTrue()
    }

    // ─── FindFactsByTags ──────────────────────────────────────────────────────

    @Test
    @Order(14)
    @DisplayName("FindFactsByTags - should return FactsFound with facts matching the tags")
    fun findFactsByTags(): Unit = runBlocking {
        val response = factService.findFactsByTags(findFactsByTagsRequest {
            storeName = STORE
            tags["region"] = "eu"
        }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.factsList).isNotEmpty()
        assertThat(response.found.factsList.first().tagsMap).containsEntry("region", "eu")
    }

    @Test
    @Order(15)
    @DisplayName("FindFactsByTags - should return StoreNotFound when store does not exist")
    fun findFactsByTagsStoreNotFound(): Unit = runBlocking {
        val response = factService.findFactsByTags(findFactsByTagsRequest {
            storeName = "ghost-store"
            tags["region"] = "eu"
        }).awaitSuspending()

        assertThat(response.hasStoreNotFound()).isTrue()
    }

    // ─── QueryFacts ───────────────────────────────────────────────────────────

    @Test
    @Order(16)
    @DisplayName("QueryFacts - should return FactsFound using a tag-only query")
    fun queryFacts(): Unit = runBlocking {
        val response = factService.queryFacts(queryFactsRequest {
            storeName = STORE
            query = tagQuery {
                items += tagQueryItem {
                    tagOnly = tagOnlyItem { tags["region"] = "eu" }
                }
            }
        }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.factsList).isNotEmpty()
    }

    @Test
    @Order(17)
    @DisplayName("QueryFacts - should return StoreNotFound when store does not exist")
    fun queryFactsStoreNotFound(): Unit = runBlocking {
        val response = factService.queryFacts(queryFactsRequest {
            storeName = "ghost-store"
            query = tagQuery {
                items += tagQueryItem {
                    tagOnly = tagOnlyItem { tags["region"] = "eu" }
                }
            }
        }).awaitSuspending()

        assertThat(response.hasStoreNotFound()).isTrue()
    }

    @Test
    @Order(18)
    @DisplayName("QueryFacts - should return FactsFound using a tag-type query")
    fun queryFactsWithTagTypeItem(): Unit = runBlocking {
        val response = factService.queryFacts(queryFactsRequest {
            storeName = STORE
            query = tagQuery {
                items += tagQueryItem {
                    tagType = tagTypeItem {
                        types += "order.created"
                        tags["region"] = "eu"
                    }
                }
            }
        }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.factsList).isNotEmpty()
        assertThat(response.found.factsList.first().type).isEqualTo("order.created")
    }

    @Test
    @Order(19)
    @DisplayName("QueryFacts - should return empty FactsFound when type does not match")
    fun queryFactsWithTagTypeItemNoMatch(): Unit = runBlocking {
        val response = factService.queryFacts(queryFactsRequest {
            storeName = STORE
            query = tagQuery {
                items += tagQueryItem {
                    tagType = tagTypeItem {
                        types += "order.cancelled"
                        tags["region"] = "eu"
                    }
                }
            }
        }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.factsList).isEmpty()
    }

    // ─── FindFactsInTimeRange ─────────────────────────────────────────────────

    @Test
    @Order(20)
    @DisplayName("FindFactsInTimeRange - should return FactsFound for an unbounded time range")
    fun findFactsInTimeRange(): Unit = runBlocking {
        val response = factService.findFactsInTimeRange(findFactsInTimeRangeRequest {
            storeName = STORE
            // no from/to — unbounded range matches all facts in the store
        }).awaitSuspending()

        assertThat(response.hasFound()).isTrue()
        assertThat(response.found.factsList).isNotEmpty()
    }

    @Test
    @Order(21)
    @DisplayName("FindFactsInTimeRange - should return StoreNotFound when store does not exist")
    fun findFactsInTimeRangeStoreNotFound(): Unit = runBlocking {
        val response = factService.findFactsInTimeRange(findFactsInTimeRangeRequest {
            storeName = "ghost-store"
        }).awaitSuspending()

        assertThat(response.hasStoreNotFound()).isTrue()
    }

    // ─── SubscribeFacts ─────────────────────────────────────────────────────────

    @Test
    @Order(22)
    @DisplayName("SubscribeFacts - should emit existing facts from the beginning of the store")
    fun subscribeFacts(): Unit = runBlocking {
        val responses = factService.subscribeFacts(subscribeFactsRequest {
            storeName = STORE
        }).asFlow().take(1).toList()

        assertThat(responses).hasSize(1)
        assertThat(responses.first().hasBatch()).isTrue()
        assertThat(responses.first().batch.factsList.first().id).isEqualTo(seedFactId)
    }

    @Test
    @Order(23)
    @DisplayName("SubscribeFacts - should emit a store_not_found message when the store does not exist")
    fun subscribeFactsStoreNotFound(): Unit = runBlocking {
        val responses = factService.subscribeFacts(subscribeFactsRequest {
            storeName = "ghost-store"
        }).asFlow().toList()

        assertThat(responses).hasSize(1)
        assertThat(responses.first().hasStoreNotFound()).isTrue()
        assertThat(responses.first().storeNotFound.storeName).isEqualTo("ghost-store")
    }

    @Test
    @Order(24)
    @DisplayName("SubscribeFacts - should emit an after_fact_not_found message for an unknown cursor")
    fun subscribeFactsCursorNotFound(): Unit = runBlocking {
        val responses = factService.subscribeFacts(subscribeFactsRequest {
            storeName = STORE
            afterFactId = UUID.randomUUID().toString()
        }).asFlow().toList()

        assertThat(responses).hasSize(1)
        assertThat(responses.first().hasAfterFactNotFound()).isTrue()
    }

    // ─── ReplayFacts ────────────────────────────────────────────────────────────

    @Test
    @Order(25)
    @DisplayName("ReplayFacts - should emit existing facts up to the head and then complete")
    fun replayFacts(): Unit = runBlocking {
        // A bounded replay terminates on its own, so collecting the whole flow returns.
        val responses = factService.replayFacts(replayFactsRequest {
            storeName = STORE
        }).asFlow().toList()

        assertThat(responses).isNotEmpty()
        assertThat(responses.first().hasBatch()).isTrue()
        assertThat(responses.first().batch.factsList.first().id).isEqualTo(seedFactId)
    }

    @Test
    @Order(26)
    @DisplayName("ReplayFacts - should emit a store_not_found message when the store does not exist")
    fun replayFactsStoreNotFound(): Unit = runBlocking {
        val responses = factService.replayFacts(replayFactsRequest {
            storeName = "ghost-store"
        }).asFlow().toList()

        assertThat(responses).hasSize(1)
        assertThat(responses.first().hasStoreNotFound()).isTrue()
        assertThat(responses.first().storeNotFound.storeName).isEqualTo("ghost-store")
    }
}
