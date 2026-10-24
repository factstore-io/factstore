package io.factstore.server.grpc

import com.google.protobuf.ByteString
import com.google.protobuf.CodedOutputStream
import com.google.protobuf.Timestamp
import io.factstore.core.*
import io.factstore.grpc.v1.*
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.produceIn
import java.time.Instant

/*
 * Renders the specification's results as gRPC responses.
 */

internal fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder()
    .setSeconds(epochSecond)
    .setNanos(nano)
    .build()

/**
 * The maximum encoded size of a [FactStoreProto.FactBatch]: 1 MiB.
 *
 * Far below gRPC's default message limit of 4 MiB, and large enough for any single fact,
 * which the specification's limits keep below 80 kB.
 */
internal const val MAX_FACT_BATCH_BYTES = 1_048_576

/**
 * The number of facts a stream keeps ready to be batched.
 *
 * Reading ahead is what gives [toProtoFactBatches] something to group: while facts are read
 * faster than they are sent they queue up here, and when they trickle in the queue is empty,
 * so a fact is sent on its own.
 *
 * The queue fills only while a client reads more slowly than the store, and then holds about
 * 10 MB at the largest fact the specification allows, a few dozen kB for typical facts.
 */
internal const val FACT_BATCH_BUFFER = 128

/**
 * Groups the facts of a stream into batches whose encoded size stays within [maxBytes].
 *
 * A batch is never waited for: it holds the facts that were ready when it was built. A stream
 * read faster than it is sent, such as a subscription draining history, sends full batches,
 * while one whose facts trickle in, such as a selective query or a subscription following the
 * tail, sends each fact as it arrives. A fact that no longer fits [maxBytes] opens the next
 * batch rather than being dropped or delayed.
 */
internal fun Flow<Fact>.toProtoFactBatches(
    maxBytes: Int = MAX_FACT_BATCH_BYTES,
): Flow<FactStoreProto.FactBatch> = flow {
    coroutineScope {
        val ready = buffer(FACT_BATCH_BUFFER).produceIn(this)
        // The fact that did not fit the batch just sent; it opens the next one.
        var carried: FactStoreProto.Fact? = null
        while (true) {
            val first = carried ?: ready.receiveOrNull()?.toProto() ?: break
            carried = null
            val batch = mutableListOf(first)
            var batchBytes = first.sizeInBatch()
            while (true) {
                // Takes what is already there, and never waits for more.
                val next = ready.tryReceive().getOrNull()?.toProto() ?: break
                val size = next.sizeInBatch()
                if (batchBytes + size > maxBytes) {
                    carried = next
                    break
                }
                batch += next
                batchBytes += size
            }
            emit(factBatch { facts += batch })
        }
    }
}

/** Receives the next fact, or `null` once the channel is closed; a failure upstream is rethrown. */
private suspend fun ReceiveChannel<Fact>.receiveOrNull(): Fact? {
    val received = receiveCatching()
    received.exceptionOrNull()?.let { throw it }
    return received.getOrNull()
}

/** The size the fact takes up as an element of `FactBatch.facts`, tag and length included. */
private fun FactStoreProto.Fact.sizeInBatch(): Int =
    CodedOutputStream.computeMessageSize(FactStoreProto.FactBatch.FACTS_FIELD_NUMBER, this)

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
            is AppendResult.AfterFactNotFound -> afterFactNotFound = afterFactNotFound { factId = this@toGrpcResponse.factId.uuid.toString() }
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
