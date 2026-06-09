package io.factstore.client.operations

import io.factstore.client.exceptions.AppendConditionViolatedException
import io.factstore.client.exceptions.DuplicateFactIdsException
import io.factstore.client.exceptions.FactNotFoundException
import io.factstore.client.exceptions.StoreNotFoundException
import io.factstore.client.internal.toDomain
import io.factstore.client.internal.toProto
import io.factstore.client.internal.toProtoTimestamp
import io.factstore.client.model.AppendCondition
import io.factstore.client.model.AppendFactsBuilder
import io.factstore.client.model.Fact
import io.factstore.client.model.FactInput
import io.factstore.client.model.ReadDirection
import io.factstore.client.model.StreamStartPosition
import io.factstore.client.model.TagQuery
import io.factstore.grpc.v1.FactServiceGrpcKt.FactServiceCoroutineStub
import io.factstore.grpc.v1.appendFactsRequest
import io.factstore.grpc.v1.factExistsRequest
import io.factstore.grpc.v1.findFactsBySubjectRequest
import io.factstore.grpc.v1.findFactsByTagsRequest
import io.factstore.grpc.v1.findFactsInTimeRangeRequest
import io.factstore.grpc.v1.fromBeginning
import io.factstore.grpc.v1.fromEnd
import io.factstore.grpc.v1.getFactRequest
import io.factstore.grpc.v1.queryFactsRequest
import io.factstore.grpc.v1.streamFactsRequest
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
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
    ) {
        val response = timedStub().appendFacts(appendFactsRequest {
            this.storeName = storeName
            this.facts += facts.map { it.toProto() }
            idempotencyKey?.let { this.idempotencyKey = it }
            condition?.let { this.condition = it.toProto() }
        })
        when {
            response.hasAppended() || response.hasAlreadyApplied() -> Unit
            response.hasConditionViolated() -> throw AppendConditionViolatedException()
            response.hasDuplicateFactIds() -> throw DuplicateFactIdsException(response.duplicateFactIds.factIdsList)
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun append(
        storeName: String,
        idempotencyKey: String? = null,
        condition: AppendCondition? = null,
        block: AppendFactsBuilder.() -> Unit,
    ) = append(storeName, AppendFactsBuilder().apply(block).build(), idempotencyKey, condition)

    suspend fun get(storeName: String, factId: String): Fact {
        val response = timedStub().getFact(getFactRequest {
            this.storeName = storeName
            this.factId = factId
        })
        return when {
            response.hasFound() -> response.found.fact.toDomain()
            response.hasNotFound() -> throw FactNotFoundException(factId)
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun exists(storeName: String, factId: String): Boolean {
        val response = timedStub().factExists(factExistsRequest {
            this.storeName = storeName
            this.factId = factId
        })
        return when {
            response.hasPresent() -> true
            response.hasAbsent() -> false
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun findBySubject(
        storeName: String,
        subject: String,
        limit: Int? = null,
        direction: ReadDirection = ReadDirection.FORWARD,
    ): List<Fact> {
        val response = timedStub().findFactsBySubject(findFactsBySubjectRequest {
            this.storeName = storeName
            this.subject = subject
            limit?.let { this.limit = it }
            this.direction = direction.toProto()
        })
        return when {
            response.hasFound() -> response.found.factsList.map { it.toDomain() }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun findByTags(
        storeName: String,
        tags: Map<String, String>,
        limit: Int? = null,
        direction: ReadDirection = ReadDirection.FORWARD,
    ): List<Fact> {
        val response = timedStub().findFactsByTags(findFactsByTagsRequest {
            this.storeName = storeName
            this.tags.putAll(tags)
            limit?.let { this.limit = it }
            this.direction = direction.toProto()
        })
        return when {
            response.hasFound() -> response.found.factsList.map { it.toDomain() }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    suspend fun query(storeName: String, tagQuery: TagQuery): List<Fact> {
        val response = timedStub().queryFacts(queryFactsRequest {
            this.storeName = storeName
            query = tagQuery.toProto()
        })
        return when {
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
    ): List<Fact> {
        val response = timedStub().findFactsInTimeRange(findFactsInTimeRangeRequest {
            this.storeName = storeName
            from?.let { this.from = it.toProtoTimestamp() }
            to?.let { this.to = it.toProtoTimestamp() }
            limit?.let { this.limit = it }
            this.direction = direction.toProto()
        })
        return when {
            response.hasFound() -> response.found.factsList.map { it.toDomain() }
            response.hasStoreNotFound() -> throw StoreNotFoundException(storeName)
            else -> error("Unexpected response: $response")
        }
    }

    fun stream(
        storeName: String,
        startPosition: StreamStartPosition = StreamStartPosition.Beginning,
    ): Flow<Fact> = stub.streamFacts(streamFactsRequest {
        this.storeName = storeName
        when (startPosition) {
            StreamStartPosition.Beginning -> fromBeginning = fromBeginning {}
            StreamStartPosition.End -> fromEnd = fromEnd {}
            is StreamStartPosition.AfterFact -> afterFactId = startPosition.factId
        }
    }).map { it.toDomain() }
}
