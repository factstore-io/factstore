package io.factstore.server.http

import io.factstore.core.*
import jakarta.ws.rs.core.Response
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.transform

/*
 * Renders the specification's results as HTTP responses.
 */

internal fun AppendResult.toResponse(): Response = when (this) {
    is AppendResult.Appended -> Response.ok(AppendedHttp(factIds.map { it.uuid }, appendedAt)).build()
    is AppendResult.AlreadyApplied -> Response.ok().build()
    is AppendResult.AppendConditionViolated -> appendConditionViolatedError()
    is AppendResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun FindByIdResult.toResponse(): Response = when (this) {
    is FindByIdResult.Found -> Response.ok(fact.toFactHttp()).build()
    is FindByIdResult.NotFound -> factNotFoundError(id)
    is FindByIdResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun FindBySubjectResult.toResponse(): Response = when (this) {
    is FindBySubjectResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
    is FindBySubjectResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun FindByTagsResult.toResponse(): Response = when (this) {
    is FindByTagsResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
    is FindByTagsResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun FindByTagQueryResult.toResponse(): Response = when (this) {
    is FindByTagQueryResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
    is FindByTagQueryResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun FindInTimeRangeResult.toResponse(): Response = when (this) {
    is FindInTimeRangeResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
    is FindInTimeRangeResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun CreateStoreResult.toResponse(): Response = when (this) {
    is CreateStoreResult.Created -> Response
        .status(Response.Status.CREATED)
        .entity(mapOf("id" to id.uuid))
        .build()

    is CreateStoreResult.NameAlreadyExists -> storeAlreadyExistsError(storeName)
}

internal fun FindStoreByNameResult.toResponse(): Response = when (this) {
    is FindStoreByNameResult.Found -> Response.ok(storeMetadata.toHttp()).build()
    is FindStoreByNameResult.NotFound -> storeNotFoundError(storeName)
}

internal fun ExistsStoreByNameResult.toResponse(): Response = when (this) {
    ExistsStoreByNameResult.StoreExists -> Response.ok().build()
    ExistsStoreByNameResult.StoreAbsent -> Response.status(Response.Status.NOT_FOUND).build()
}

internal fun RemoveStoreResult.toResponse(): Response = when (this) {
    is RemoveStoreResult.StoreRemoved -> Response.ok().build()
    is RemoveStoreResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun List<StoreMetadata>.toResponse(): Response = Response.ok(map { it.toHttp() }).build()

// ─── Streams ──────────────────────────────────────────────────────────────────

internal fun SubscribeResult.toResponse(): Flow<FactHttp> = when (this) {
    is SubscribeResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is SubscribeResult.FactIdNotFound -> throw StreamApiException.FactNotFoundException(id)
    is SubscribeResult.FactStream -> stream.transform { batch -> batch.forEach { emit(it.toFactHttp()) } }
}

internal fun ReplayResult.toResponse(): Flow<FactHttp> = when (this) {
    is ReplayResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is ReplayResult.FactIdNotFound -> throw StreamApiException.FactNotFoundException(id)
    is ReplayResult.FactStream -> stream.transform { batch -> batch.forEach { emit(it.toFactHttp()) } }
}

/** A stream cannot return a response, so its outcomes are thrown and rendered by [ErrorMappers]. */
sealed class StreamApiException : RuntimeException() {
    data class FactNotFoundException(val factId: FactId) : StreamApiException()
    data class StoreNotFoundException(val storeName: StoreName) : StreamApiException()
}

// ─── Facts and stores in their HTTP shape ─────────────────────────────────────

internal fun Fact.toFactHttp() = FactHttp(
    id = id.uuid,
    type = type.value,
    subject = subject.value,
    appendedAt = appendedAt,
    payload = payload.toFactPayloadHttp(),
    metadata = metadata.entries.associate { Pair(it.key.value, it.value.value) },
    tags = tags.entries.associate { Pair(it.key.value, it.value.value) },
)

internal fun FactPayload.toFactPayloadHttp() = FactPayloadHttp(data = data)

internal fun StoreMetadata.toHttp() = StoreMetadataHttp(id = id.uuid, name = name.value, createdAt = createdAt)
