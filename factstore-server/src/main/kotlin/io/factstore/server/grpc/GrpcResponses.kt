package io.factstore.server.grpc

import com.google.protobuf.ByteString
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.Timestamp
import io.factstore.core.*
import io.factstore.grpc.v1.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import java.time.Instant

/*
 * Renders the specification's results as gRPC responses.
 */

internal fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder()
    .setSeconds(epochSecond)
    .setNanos(nano)
    .build()

internal fun List<Fact>.toProtoFactBatch(): FactStoreProto.FactBatch = factBatch {
    facts += this@toProtoFactBatch.map { it.toProto() }
}

/**
 * The maximum encoded size of a [FactStoreProto.FactBatch]: 1 MiB.
 *
 * Far below gRPC's default message limit of 4 MiB, and large enough for any single fact,
 * which the specification's limits keep below 80 kB.
 */
internal const val MAX_FACT_BATCH_BYTES = 1_048_576

/**
 * Groups the facts into batches whose encoded size stays within [maxBytes].
 *
 * A batch is sent once the next fact would not fit, and the last one when the facts are
 * exhausted, so this suits bounded streams only: a live stream would hold back facts
 * until a batch is full.
 */
internal fun Flow<Fact>.toProtoFactBatches(maxBytes: Int = MAX_FACT_BATCH_BYTES): Flow<FactStoreProto.FactBatch> =
    flow {
        val batch = mutableListOf<FactStoreProto.Fact>()
        var batchBytes = 0
        collect { fact ->
            val proto = fact.toProto()
            // The size the fact takes up as an element of FactBatch.facts, tag and length included.
            val size = CodedOutputStream.computeMessageSize(FactStoreProto.FactBatch.FACTS_FIELD_NUMBER, proto)
            if (batch.isNotEmpty() && batchBytes + size > maxBytes) {
                emit(factBatch { facts += batch })
                batch.clear()
                batchBytes = 0
            }
            batch += proto
            batchBytes += size
        }
        if (batch.isNotEmpty()) emit(factBatch { facts += batch })
    }

internal fun Fact.toProto(): FactStoreProto.Fact = fact {
    id = this@toProto.id.uuid.toString()
    type = this@toProto.type.value
    subject = this@toProto.subject.value
    appendedAt = this@toProto.appendedAt.toTimestamp()
    payload = this@toProto.payload.toProto()
    metadata.putAll(this@toProto.metadata.entries.associate { (k, v) -> k.value to v.value })
    tags.putAll(this@toProto.tags.entries.associate { (k, v) -> k.value to v.value })
}

internal fun FactPayload.toProto(): FactStoreProto.FactPayload = factPayload {
    data = ByteString.copyFrom(this@toProto.data)
}

internal fun StoreMetadata.toProto(): FactStoreProto.StoreInfo = storeInfo {
    id = this@toProto.id.uuid.toString()
    name = this@toProto.name.value
    createdAt = this@toProto.createdAt.toTimestamp()
}

typealias GrpcAppendFactsResponse = FactStoreProto.AppendFactsResponse

internal fun AppendResult.toGrpcResponse(): GrpcAppendFactsResponse =
    appendFactsResponse {
        when (this@toGrpcResponse) {
            is AppendResult.Appended -> appended = factsAppended {
                factIds += this@toGrpcResponse.factIds.map { it.uuid.toString() }
                appendedAt = this@toGrpcResponse.appendedAt.toTimestamp()
            }
            is AppendResult.AlreadyApplied -> alreadyApplied = alreadyApplied { }
            is AppendResult.AppendConditionViolated -> conditionViolated = conditionViolated { }
            is AppendResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcGetFactResponse = FactStoreProto.GetFactResponse

internal fun FindByIdResult.toGrpcResponse(): GrpcGetFactResponse =
    getFactResponse {
        when (this@toGrpcResponse) {
            is FindByIdResult.Found -> found = factFound { fact = this@toGrpcResponse.fact.toProto() }
            is FindByIdResult.NotFound -> notFound = factNotFound { }
            is FindByIdResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcFactExistsResponse = FactStoreProto.FactExistsResponse

internal fun ExistsByIdResult.toGrpcResponse(): GrpcFactExistsResponse =
    factExistsResponse {
        when (this@toGrpcResponse) {
            ExistsByIdResult.Exists -> present = factPresent { }
            ExistsByIdResult.DoesNotExist -> absent = factAbsent { }
            is ExistsByIdResult.StoreNotFound -> storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcFindFactsByTagsResponse = FactStoreProto.FindFactsByTagsResponse

internal fun FindByTagsResult.toGrpcResponse(): GrpcFindFactsByTagsResponse =
    findFactsByTagsResponse {
        when (this@toGrpcResponse) {
            is FindByTagsResult.Found ->
                found = factsFound { facts += this@toGrpcResponse.facts.map { it.toProto() } }

            is FindByTagsResult.StoreNotFound ->
                storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcQueryFactsResponse = FactStoreProto.QueryFactsResponse

internal fun FindByTagQueryResult.toGrpcResponse(): GrpcQueryFactsResponse =
    queryFactsResponse {
        when (this@toGrpcResponse) {
            is FindByTagQueryResult.Found ->
                found = factsFound { facts += this@toGrpcResponse.facts.map { it.toProto() } }

            is FindByTagQueryResult.StoreNotFound ->
                storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcFindInTimeRangeResponse = FactStoreProto.FindFactsInTimeRangeResponse

internal fun FindInTimeRangeResult.toGrpcResponse(): GrpcFindInTimeRangeResponse =
    findFactsInTimeRangeResponse {
        when (this@toGrpcResponse) {
            is FindInTimeRangeResult.Found ->
                found = factsFound { facts += this@toGrpcResponse.facts.map { it.toProto() } }

            is FindInTimeRangeResult.StoreNotFound ->
                storeNotFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcCreateStoreResponse = FactStoreProto.CreateStoreResponse

internal fun CreateStoreResult.toGrpcResponse(): GrpcCreateStoreResponse =
    createStoreResponse {
        when (this@toGrpcResponse) {
            is CreateStoreResult.Created ->
                created = storeCreated { id = this@toGrpcResponse.id.uuid.toString() }
            is CreateStoreResult.NameAlreadyExists ->
                nameAlreadyExists = storeNameAlreadyExists { }
        }
    }

internal fun List<StoreMetadata>.toGrpcResponse(): FactStoreProto.ListStoresResponse =
    listStoresResponse {
        stores += this@toGrpcResponse.map { it.toProto() }
    }

typealias GrpcDeleteStoreResponse = FactStoreProto.DeleteStoreResponse

internal fun RemoveStoreResult.toGrpcResponse(): GrpcDeleteStoreResponse =
    deleteStoreResponse {
        when (this@toGrpcResponse) {
            is RemoveStoreResult.StoreRemoved -> deleted = storeDeleted { }
            is RemoveStoreResult.StoreNotFound -> notFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value }
        }
    }

typealias GrpcFindStoreByNameResult = FactStoreProto.GetStoreResponse

internal fun FindStoreByNameResult.toGrpcResponse(): GrpcFindStoreByNameResult =
    when (this) {
        is FindStoreByNameResult.Found -> getStoreResponse { found = storeFound { store = storeMetadata.toProto() } }
        is FindStoreByNameResult.NotFound -> getStoreResponse { notFound = storeNotFound { storeName = this@toGrpcResponse.storeName.value } }
    }

typealias GrpcStoreExistsResponse = FactStoreProto.StoreExistsResponse

internal fun ExistsStoreByNameResult.toGrpcResponse(): GrpcStoreExistsResponse =
    when (this) {
        ExistsStoreByNameResult.StoreExists -> storeExistsResponse { present = storePresent { } }
        ExistsStoreByNameResult.StoreAbsent -> storeExistsResponse { absent = storeAbsent { } }
    }
