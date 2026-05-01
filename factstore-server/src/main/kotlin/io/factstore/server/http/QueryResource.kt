package io.factstore.server.http

import io.factstore.core.FactStore
import io.factstore.core.FindByIdResult
import io.factstore.core.FindBySubjectResult
import io.factstore.core.FindByTagQueryResult
import io.factstore.core.FindInTimeRangeResult
import io.factstore.core.StoreName
import io.factstore.core.SubjectRef
import io.factstore.core.TimeRange
import io.factstore.core.toFactId
import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.NOT_FOUND
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
        @PathParam("storeName") storeName: String,
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
        @PathParam("storeName") storeName: String,
        @Valid factQueryHttp: FactQueryHttp
    ): Response =
        store
            .findByTagQuery(StoreName(storeName), factQueryHttp.toTagQuery())
            .toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/subjects/{subjectType}/{subjectId}/facts")
    suspend fun findBySubject(
        @PathParam("storeName") storeName: String,
        @PathParam("subjectType") subjectType: String,
        @PathParam("subjectId") subjectId: String,
    ): Response =
        store
            .findBySubject(StoreName(storeName), SubjectRef(subjectType, subjectId))
            .toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts")
    suspend fun findFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("from") from: Instant? = null,
        @QueryParam("to") to: Instant? = null,
    ): Response =
        store
            .findInTimeRange(
                storeName = StoreName(storeName),
                TimeRange(
                    start = from ?: Instant.MIN,
                    end = to ?: Instant.now()
                )
            )
            .toResponse()

}

private fun FindByIdResult.toResponse(): Response {
    return when (this) {
        is FindByIdResult.Found -> Response.ok(fact.toFactHttp()).build()
        is FindByIdResult.NotFound -> Response.status(NOT_FOUND).build()
        is FindByIdResult.StoreNotFound -> Response.status(NOT_FOUND).build()
    }
}

private fun FindInTimeRangeResult.toResponse(): Response {
    return when (this) {
        is FindInTimeRangeResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
        is FindInTimeRangeResult.StoreNotFound -> Response.status(NOT_FOUND).build()
    }
}

private fun FindBySubjectResult.toResponse(): Response {
    return when (this) {
        is FindBySubjectResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
        is FindBySubjectResult.StoreNotFound -> Response.status(NOT_FOUND).build()
    }
}

private fun FindByTagQueryResult.toResponse(): Response {
    return when (this) {
        is FindByTagQueryResult.Found -> Response.ok(facts.map { it.toFactHttp() }).build()
        is FindByTagQueryResult.StoreNotFound -> Response.status(NOT_FOUND).build()
    }
}
