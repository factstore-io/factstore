package io.factstore.client.operations

import io.factstore.client.exceptions.AppendConditionViolatedException
import io.factstore.client.exceptions.FactNotFoundException
import io.factstore.client.exceptions.StoreNotFoundException
import io.factstore.client.internal.grpcCall
import io.factstore.client.internal.toDomain
import io.factstore.client.internal.toFactStoreException
import io.factstore.client.internal.toInstant
import io.factstore.client.internal.toProto
import io.factstore.client.internal.toProtoTimestamp
import io.factstore.client.model.AppendCondition
import io.factstore.client.model.AppendFactsBuilder
import io.factstore.client.model.AppendOutcome
import io.factstore.client.model.Fact
import io.factstore.client.model.FactInput
import io.factstore.client.model.ReadDirection
import io.factstore.client.model.ReplayStartPosition
import io.factstore.client.model.SubscribeStartPosition
import io.factstore.client.model.TagQuery
import io.factstore.grpc.v1.FactServiceGrpcKt.FactServiceCoroutineStub
import io.factstore.grpc.v1.appendFactsRequest
import io.factstore.grpc.v1.factExistsRequest
import io.factstore.grpc.v1.findFactsByTagsRequest
import io.factstore.grpc.v1.findFactsInTimeRangeRequest
import io.factstore.grpc.v1.fromBeginning
import io.factstore.grpc.v1.fromEnd
import io.factstore.grpc.v1.getFactRequest
import io.factstore.grpc.v1.queryFactsRequest
import io.factstore.grpc.v1.replayFactsRequest
import io.factstore.grpc.v1.streamFactsBySubjectRequest
import io.factstore.grpc.v1.streamFactsRequest
import io.factstore.grpc.v1.subscribeFactsRequest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.transform
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit

class FactOperations internal constructor(
    private val stub: FactServiceCoroutineStub,
    private val callTimeout: Duration,
) {
    private fun timedStub() = stub.withDeadlineAfter(callTimeout.toMillis(), TimeUnit.MILLISECONDS)

    suspend fun append(
        storeName: String,
        facts: List<FactInput>,
        idempotencyKey: String? = null,
        condition: AppendCondition? = null,
    ): AppendOutcome = grpcCall {
        val response = timedStub().appendFacts(appendFactsRequest {
            this.storeName = storeName
            this.facts += facts.map { it.toProto() }
            idempotencyKey?.let { this.idempotencyKey = it }
            condition?.let { this.condition = it.toProto() }
        })
        when {
            response.hasAppended() -> AppendOutcome.Appended(
                factIds = response.appended.factIdsList.toList(),
                appendedAt = response.appended.appendedAt.toInstant(),
            )
            response.hasAlreadyApplied() -> AppendOutcome.AlreadyApplied
            response.hasConditionViolated() -> throw AppendConditionViolatedException()
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun append(
        storeName: String,
        idempotencyKey: String? = null,
        condition: AppendCondition? = null,
        block: AppendFactsBuilder.() -> Unit,
    ): AppendOutcome = append(storeName, AppendFactsBuilder().apply(block).build(), idempotencyKey, condition)

    suspend fun get(storeName: String, factId: String): Fact = grpcCall {
        val response = timedStub().getFact(getFactRequest {
            this.storeName = storeName
            this.factId = factId
        })
        when {
            response.hasFound() -> response.found.fact.toDomain()
            response.hasNotFound() -> throw FactNotFoundException(factId)
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun exists(storeName: String, factId: String): Boolean = grpcCall {
        val response = timedStub().factExists(factExistsRequest {
            this.storeName = storeName
            this.factId = factId
        })
        when {
            response.hasPresent() -> true
            response.hasAbsent() -> false
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    /**
     * Streams all facts of a store, up to the newest fact at the time of the call, then completes.
     *
     * Throws [StoreNotFoundException] when collected if the store does not exist.
     *
     * @param limit the maximum number of facts to emit, or `null` for all of them
     */
    fun streamFacts(
        storeName: String,
        direction: ReadDirection,
        limit: Int? = null,
    ): Flow<Fact> = stub.streamFacts(streamFactsRequest {
        this.storeName = storeName
        this.direction = direction.toProto()
        limit?.let { this.limit = it }
    }).toFactFlow { response ->
        when {
            response.hasBatch() -> response.batch.factsList.forEach { emit(it.toDomain()) }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected stream message: $response")
        }
    }

    /**
     * Streams the facts of a subject, up to the newest fact at the time of the call, then completes.
     *
     * Throws [StoreNotFoundException] when collected if the store does not exist.
     *
     * @param limit the maximum number of facts to emit, or `null` for all of them
     */
    fun streamFactsBySubject(
        storeName: String,
        subject: String,
        direction: ReadDirection,
        limit: Int? = null,
    ): Flow<Fact> = stub.streamFactsBySubject(streamFactsBySubjectRequest {
        this.storeName = storeName
        this.subject = subject
        this.direction = direction.toProto()
        limit?.let { this.limit = it }
    }).toFactFlow { response ->
        when {
            response.hasBatch() -> response.batch.factsList.forEach { emit(it.toDomain()) }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected stream message: $response")
        }
    }

    suspend fun findByTags(
        storeName: String,
        tags: Map<String, String>,
        limit: Int? = null,
        direction: ReadDirection = ReadDirection.FORWARD,
    ): List<Fact> = grpcCall {
        val response = timedStub().findFactsByTags(findFactsByTagsRequest {
            this.storeName = storeName
            this.tags.putAll(tags)
            limit?.let { this.limit = it }
            this.direction = direction.toProto()
        })
        when {
            response.hasFound() -> response.found.factsList.map { it.toDomain() }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun query(storeName: String, tagQuery: TagQuery): List<Fact> = grpcCall {
        val response = timedStub().queryFacts(queryFactsRequest {
            this.storeName = storeName
            query = tagQuery.toProto()
        })
        when {
            response.hasFound() -> response.found.factsList.map { it.toDomain() }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun findInTimeRange(
        storeName: String,
        from: Instant? = null,
        to: Instant? = null,
        limit: Int? = null,
        direction: ReadDirection = ReadDirection.FORWARD,
    ): List<Fact> = grpcCall {
        val response = timedStub().findFactsInTimeRange(findFactsInTimeRangeRequest {
            this.storeName = storeName
            from?.let { this.from = it.toProtoTimestamp() }
            to?.let { this.to = it.toProtoTimestamp() }
            limit?.let { this.limit = it }
            this.direction = direction.toProto()
        })
        when {
            response.hasFound() -> response.found.factsList.map { it.toDomain() }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    /** Live subscription: drains history then follows the tail indefinitely. */
    fun subscribe(
        storeName: String,
        startPosition: SubscribeStartPosition = SubscribeStartPosition.Beginning,
    ): Flow<Fact> = stub.subscribeFacts(subscribeFactsRequest {
        this.storeName = storeName
        when (startPosition) {
            SubscribeStartPosition.Beginning -> fromBeginning = fromBeginning {}
            SubscribeStartPosition.End -> fromEnd = fromEnd {}
            is SubscribeStartPosition.AfterFact -> afterFactId = startPosition.factId
        }
    }).toFactFlow { response ->
        when {
            response.hasBatch() -> response.batch.factsList.forEach { emit(it.toDomain()) }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            response.hasAfterFactNotFound() ->
                throw FactNotFoundException((startPosition as? SubscribeStartPosition.AfterFact)?.factId ?: "")
            else -> error("Unexpected stream message: $response")
        }
    }

    /** Bounded replay: drains history up to the pinned head, then completes. */
    fun replay(
        storeName: String,
        start: ReplayStartPosition = ReplayStartPosition.Beginning,
    ): Flow<Fact> = stub.replayFacts(replayFactsRequest {
        this.storeName = storeName
        when (start) {
            ReplayStartPosition.Beginning -> fromBeginning = fromBeginning {}
            is ReplayStartPosition.AfterFact -> afterFactId = start.factId
        }
    }).toFactFlow { response ->
        when {
            response.hasBatch() -> response.batch.factsList.forEach { emit(it.toDomain()) }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            response.hasAfterFactNotFound() ->
                throw FactNotFoundException((start as? ReplayStartPosition.AfterFact)?.factId ?: "")
            else -> error("Unexpected stream message: $response")
        }
    }

    /**
     * Turns the messages of a streaming RPC into facts, mapping gRPC status failures to
     * [io.factstore.client.exceptions.FactStoreException]s.
     *
     * Streams are called on the plain stub rather than [timedStub]: a deadline would cut off
     * reading a long history.
     */
    private fun <R> Flow<R>.toFactFlow(decode: suspend FlowCollector<Fact>.(R) -> Unit): Flow<Fact> =
        transform { decode(it) }.catch { throw it.toFactStoreException() }
}
