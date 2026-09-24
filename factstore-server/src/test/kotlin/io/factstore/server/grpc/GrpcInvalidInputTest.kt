package io.factstore.server.grpc

import com.google.protobuf.ByteString
import io.factstore.grpc.v1.*
import io.grpc.Channel
import io.grpc.Status
import io.grpc.StatusException
import io.quarkus.grpc.GrpcClient
import io.quarkus.test.junit.QuarkusTest
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource

/**
 * Every RPC reports input it cannot parse as `INVALID_ARGUMENT` with a description, before it
 * looks up the store — so the store named here does not have to exist.
 */
@QuarkusTest
@TestInstance(PER_CLASS)
class GrpcInvalidInputTest {

    @GrpcClient
    lateinit var channel: Channel

    private val facts by lazy { FactServiceGrpcKt.FactServiceCoroutineStub(channel) }
    private val stores by lazy { StoreServiceGrpcKt.StoreServiceCoroutineStub(channel) }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidRequests")
    @DisplayName("Every RPC - Should fail with INVALID_ARGUMENT when the input cannot be parsed")
    fun invalidInputIsRejected(description: String, call: suspend () -> Unit) {
        val error = try {
            runBlocking { call() }
            null
        } catch (e: StatusException) {
            e
        }

        assertThat(error).describedAs("%s should fail", description).isNotNull()
        assertThat(error!!.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
        assertThat(error.status.description).isNotBlank()
    }

    fun invalidRequests(): List<Arguments> = listOf(
        case("create a store with an invalid name") { stores.createStore(createStoreRequest { name = "1 bad" }) },
        case("get a store with an invalid name") { stores.getStore(getStoreRequest { name = "1 bad" }) },
        case("delete a store with an invalid name") { stores.deleteStore(deleteStoreRequest { name = "1 bad" }) },
        case("check a store with an invalid name") { stores.storeExists(storeExistsRequest { name = "1 bad" }) },

        case("append a fact with an invalid subject") {
            facts.appendFacts(appendFactsRequest { storeName = STORE; this.facts += fact { subject = "order 1" } })
        },
        case("append a fact with six tags") {
            facts.appendFacts(appendFactsRequest {
                storeName = STORE
                this.facts += fact { (1..6).forEach { tags["k$it"] = "v" } }
            })
        },
        case("append with an idempotency key that is not a UUID") {
            facts.appendFacts(appendFactsRequest { storeName = STORE; idempotencyKey = "nope"; this.facts += fact { } })
        },

        case("get a fact by an id that is not a UUID") {
            facts.getFact(getFactRequest { storeName = STORE; factId = "nope" })
        },
        case("check a fact by an id that is not a UUID") {
            facts.factExists(factExistsRequest { storeName = STORE; factId = "nope" })
        },
        case("stream the facts of an invalid subject") {
            facts.streamFactsBySubject(streamFactsBySubjectRequest {
                storeName = STORE; subject = "order 1"; direction = FORWARD
            }).first()
        },
        case("stream the facts of a subject without a direction") {
            facts.streamFactsBySubject(streamFactsBySubjectRequest { storeName = STORE; subject = "s" }).first()
        },
        case("stream facts by a query without filters") {
            facts.streamFactsByQuery(streamFactsByQueryRequest {
                storeName = STORE; direction = FORWARD; query = factQuery { }
            }).first()
        },
        case("stream facts by a filter without predicates") {
            facts.streamFactsByQuery(streamFactsByQueryRequest {
                storeName = STORE; direction = FORWARD; query = factQuery { filters += factFilter { } }
            }).first()
        },
        case("stream facts by a query with an invalid subject") {
            facts.streamFactsByQuery(streamFactsByQueryRequest {
                storeName = STORE; direction = FORWARD
                query = factQuery { filters += factFilter { subjects += "order 1" } }
            }).first()
        },
        case("stream facts without tags") {
            facts.streamFactsByTags(streamFactsByTagsRequest { storeName = STORE; direction = FORWARD }).first()
        },
        case("stream facts by six tags") {
            facts.streamFactsByTags(streamFactsByTagsRequest {
                storeName = STORE; direction = FORWARD; (1..6).forEach { tags["k$it"] = "v" }
            }).first()
        },
        case("stream the facts of an invalid type") {
            facts.streamFactsByType(streamFactsByTypeRequest {
                storeName = STORE; type = "order created"; direction = FORWARD
            }).first()
        },
        case("stream the facts of a type without a direction") {
            facts.streamFactsByType(streamFactsByTypeRequest { storeName = STORE; type = "T" }).first()
        },
        case("stream facts continued after an id that is not a UUID") {
            facts.streamFacts(streamFactsRequest {
                storeName = STORE; direction = FORWARD; continueAfterFactId = "nope"
            }).first()
        },
        case("stream facts without a direction") {
            facts.streamFacts(streamFactsRequest { storeName = STORE }).first()
        },
        case("stream facts in an unknown direction") {
            facts.streamFacts(streamFactsRequest { storeName = STORE; directionValue = 42 }).first()
        },
        case("stream facts with limit 0") {
            facts.streamFacts(streamFactsRequest { storeName = STORE; direction = FORWARD; limit = 0 }).first()
        },
        case("find facts with limit 0") {
            facts.findFactsInTimeRange(findFactsInTimeRangeRequest { storeName = STORE; direction = FORWARD; limit = 0 })
        },
        case("find facts in a time range without a direction") {
            facts.findFactsInTimeRange(findFactsInTimeRangeRequest { storeName = STORE })
        },

        case("subscribe to a store with an invalid name") {
            facts.subscribeFacts(subscribeFactsRequest { storeName = "1 bad" }).first()
        },
        case("replay after an id that is not a UUID") {
            facts.replayFacts(replayFactsRequest { storeName = STORE; afterFactId = "nope" }).first()
        },
    )

    private fun case(description: String, call: suspend () -> Unit): Arguments = arguments(description, call)

    private fun fact(block: FactInputKt.Dsl.() -> Unit): FactStoreProto.FactInput = factInput {
        type = "T"
        subject = "s"
        payload = factPayload { data = ByteString.copyFromUtf8("{}") }
        block()
    }

    companion object {
        const val STORE = "unknown-store"
        val FORWARD = FactStoreProto.ReadDirection.READ_DIRECTION_FORWARD
    }
}
