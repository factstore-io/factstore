package io.factstore.server.http

import io.factstore.core.*
import io.factstore.server.http.Reason.Conflict
import io.factstore.server.http.validation.ValidStoreName
import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.BAD_REQUEST
import java.time.Instant
import java.util.*

@Path("/v1/stores/{storeName}")
class QueryResource(
    private val store: FactStore
) {

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts/{factId}")
    suspend fun findById(
        @PathParam("storeName") @ValidStoreName storeName: String,
        @PathParam("factId") factId: UUID,
    ): Response =
        store
            .findById(StoreName(storeName), factId.toFactId())
            .toResponse()

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/facts/query")
    suspend fun findByQuery(
        @PathParam("storeName") @ValidStoreName storeName: String,
        @Valid factQueryHttp: FactQueryHttp
    ): Response =
        store
            .findByTagQuery(StoreName(storeName), factQueryHttp.toTagQuery())
            .toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/subjects/{subject}/facts")
    suspend fun findBySubject(
        @PathParam("storeName") @ValidStoreName storeName: String,
        @PathParam("subject") subject: String,
        @QueryParam("limit") limit: Int? = null,
        @DefaultValue("forward") @QueryParam("direction") direction: ReadDirection,
    ): Response =
        store
            .findBySubject(
                storeName = StoreName(storeName),
                subject = Subject(subject),
                limit = limit.toLimit(),
                direction = direction,
            )
            .toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts")
    suspend fun findFacts(
        @PathParam("storeName") @ValidStoreName storeName: String,
        @QueryParam("from") from: Instant? = null,
        @QueryParam("to") to: Instant? = null,
        @QueryParam("tag") tags: List<String> = emptyList(),
        @DefaultValue("0") @QueryParam("limit") limit: Int? = null,
        @DefaultValue("forward") @QueryParam("direction") direction: ReadDirection,
    ): Response {
        return when {
            tags.isNotEmpty() && (from != null || to != null) -> apiErrorResponse(
                status = BAD_REQUEST,
                reason = Conflict,
                message = "Combining tag filters with time range is not yet supported.",
            )
            tags.isNotEmpty() -> store
                .findByTags(
                    storeName = StoreName(storeName),
                    tags = tags.map { it.toTagPair() },
                    limit = limit.toLimit(),
                    direction = direction,
                )
                .toResponse()
            else -> store
                .findInTimeRange(
                    storeName = StoreName(storeName),
                    timeRange = TimeRange(
                        start = from ?: Instant.MIN,
                        end = to ?: Instant.now()
                    ),
                    limit = limit.toLimit(),
                    direction = direction,
                )
                .toResponse()
        }
    }

    private fun String.toTagPair(): Pair<TagKey, TagValue> {
        val parts = split("=", limit = 2)
        require(parts.size == 2) { "Tag must be in key=value format, got: '$this'" }
        return TagKey(parts[0]) to TagValue(parts[1])
    }

    private fun Int?.toLimit(): Limit = if (this != null && this > 0) Limit.of(this) else Limit.None

}

private fun FindByIdResult.toResponse(): Response = when (this) {
    is FindByIdResult.Found -> Response.ok(fact.toFactHttp()).build()
    is FindByIdResult.NotFound -> factNotFoundError(id)
    is FindByIdResult.StoreNotFound -> storeNotFoundError(storeName)
}

private fun FindInTimeRangeResult.toResponse(): Response {
    return when (this) {
        is FindInTimeRangeResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
        is FindInTimeRangeResult.StoreNotFound -> storeNotFoundError(storeName)
    }
}

private fun FindBySubjectResult.toResponse(): Response {
    return when (this) {
        is FindBySubjectResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
        is FindBySubjectResult.StoreNotFound -> storeNotFoundError(storeName)
    }
}

private fun FindByTagQueryResult.toResponse(): Response {
    return when (this) {
        is FindByTagQueryResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
        is FindByTagQueryResult.StoreNotFound -> storeNotFoundError(storeName)
    }
}

private fun FindByTagsResult.toResponse(): Response = when (this) {
    is FindByTagsResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
    is FindByTagsResult.StoreNotFound -> storeNotFoundError(storeName)
}
