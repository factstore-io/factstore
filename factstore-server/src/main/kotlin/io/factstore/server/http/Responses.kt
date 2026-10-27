package io.factstore.server.http

import io.factstore.core.*
import jakarta.ws.rs.core.Response

/*
 * Renders the specification's results as HTTP responses.
 */

/**
 * The header that marks the response of a replayed append.
 */
internal const val IDEMPOTENT_REPLAYED_HEADER = "Idempotent-Replayed"

internal fun AppendResult.toResponse(): Response = when (this) {
    is AppendResult.Appended -> Response.ok(AppendedHttp(facts.map { it.toHttp() })).build()
    is AppendResult.AlreadyApplied -> Response.ok(AppendedHttp(facts.map { it.toHttp() }))
        .header(IDEMPOTENT_REPLAYED_HEADER, true)
        .build()
    is AppendResult.IdempotencyKeyReused -> idempotencyKeyReusedError(idempotencyKey)
    is AppendResult.AppendConditionViolated -> appendConditionViolatedError()
    is AppendResult.StoreNotFound -> storeNotFoundError(storeName)
    is AppendResult.AfterFactNotFound -> afterFactNotFoundError(factId)
}

internal fun FindFactByIdResult.toResponse(): Response = when (this) {
    is FindFactByIdResult.Found -> Response.ok(fact.toHttp()).build()
    is FindFactByIdResult.NotFound -> factNotFoundError(factId)
    is FindFactByIdResult.StoreNotFound -> storeNotFoundError(storeName)
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
    ExistsStoreByNameResult.Exists -> Response.ok().build()
    ExistsStoreByNameResult.DoesNotExist -> Response.status(Response.Status.NOT_FOUND).build()
}

internal fun RemoveStoreResult.toResponse(): Response = when (this) {
    is RemoveStoreResult.Removed -> Response.ok().build()
    is RemoveStoreResult.NotFound -> storeNotFoundError(storeName)
}

internal fun List<StoreMetadata>.toResponse(): Response = Response.ok(map { it.toHttp() }).build()

// ─── Facts and stores in their HTTP shape ─────────────────────────────────────

internal fun Fact.toHttp() = FactHttp(
    id = id.uuid,
    type = type.value,
    subject = subject.value,
    appendedAt = appendedAt,
    payload = payload.toHttp(),
    metadata = metadata.entries.associate { Pair(it.key.value, it.value.value) },
    tags = tags.entries.associate { Pair(it.key.value, it.value.value) },
)

internal fun FactPayload.toHttp() = FactPayloadHttp(data = data)

internal fun StoreMetadata.toHttp() = StoreMetadataHttp(id = id.uuid, name = name.value, createdAt = createdAt)
