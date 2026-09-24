package io.factstore.server.http

import io.factstore.core.*
import jakarta.ws.rs.core.Response
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
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

// ─── NDJSON fact streams ──────────────────────────────────────────────────────

internal fun StreamFactsResult.toResponse(): Flow<FactStreamLineHttp> = when (this) {
    is StreamFactsResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is StreamFactsResult.ContinuationNotFound -> throw StreamApiException.ContinuationNotFoundException(factId)
    is StreamFactsResult.FactStream -> facts.toFactStreamLines()
}

internal fun StreamFactsBySubjectResult.toResponse(): Flow<FactStreamLineHttp> = when (this) {
    is StreamFactsBySubjectResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is StreamFactsBySubjectResult.ContinuationNotFound -> throw StreamApiException.ContinuationNotFoundException(factId)
    is StreamFactsBySubjectResult.FactStream -> facts.toFactStreamLines()
}

internal fun StreamFactsByTypeResult.toResponse(): Flow<FactStreamLineHttp> = when (this) {
    is StreamFactsByTypeResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is StreamFactsByTypeResult.ContinuationNotFound -> throw StreamApiException.ContinuationNotFoundException(factId)
    is StreamFactsByTypeResult.FactStream -> facts.toFactStreamLines()
}

internal fun StreamFactsByQueryResult.toResponse(): Flow<FactStreamLineHttp> = when (this) {
    is StreamFactsByQueryResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is StreamFactsByQueryResult.ContinuationNotFound -> throw StreamApiException.ContinuationNotFoundException(factId)
    is StreamFactsByQueryResult.FactStream -> facts.toFactStreamLines()
}

internal fun StreamFactsByTagsResult.toResponse(): Flow<FactStreamLineHttp> = when (this) {
    is StreamFactsByTagsResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is StreamFactsByTagsResult.ContinuationNotFound -> throw StreamApiException.ContinuationNotFoundException(factId)
    is StreamFactsByTagsResult.FactStream -> facts.toFactStreamLines()
}

/**
 * Renders facts as the lines of an NDJSON fact stream: a `fact` line per fact, then an `end`
 * line, or an `error` line in place of the `end` line if reading the facts fails.
 *
 * The response ends normally after the `error` line: by then the status 200 has been sent, so
 * the terminal line is the only place the client can learn about the failure.
 */
internal fun Flow<Fact>.toFactStreamLines(): Flow<FactStreamLineHttp> = flow {
    var count = 0L
    emitAll(
        this@toFactStreamLines
            .map<Fact, FactStreamLineHttp> { fact ->
                count++
                FactStreamLineHttp.FactLine(fact.toFactHttp())
            }
            .onCompletion { cause ->
                if (cause == null) emit(FactStreamLineHttp.EndLine(FactStreamEndHttp(count)))
            }
            .catch { e -> emit(FactStreamLineHttp.ErrorLine(unexpectedError(e))) }
    )
}

// ─── Server-sent event streams ────────────────────────────────────────────────

internal fun SubscribeResult.toResponse(): Flow<FactHttp> = when (this) {
    is SubscribeResult.StoreNotFound -> throw StreamApiException.StoreNotFoundException(storeName)
    is SubscribeResult.FactIdNotFound -> throw StreamApiException.FactNotFoundException(id)
    is SubscribeResult.FactStream -> stream.transform { batch -> batch.forEach { emit(it.toFactHttp()) } }
}

/** A stream cannot return a response, so its outcomes are thrown and rendered by [ErrorMappers]. */
sealed class StreamApiException : RuntimeException() {
    data class FactNotFoundException(val factId: FactId) : StreamApiException()
    data class StoreNotFoundException(val storeName: StoreName) : StreamApiException()
    data class ContinuationNotFoundException(val factId: FactId) : StreamApiException()
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
