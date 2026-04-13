package io.factstore.server.http

import io.factstore.core.Fact
import io.factstore.core.FactStore
import io.factstore.core.FindByIdResult
import io.factstore.core.SubjectRef
import io.factstore.core.toFactId
import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.NOT_FOUND
import java.time.Instant
import java.util.*

@Path("/v1/stores/{factStoreName}")
class QueryResource(
    private val store: FactStore
) {

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts/{factId}")
    suspend fun findById(
        @PathParam("factStoreName") factStoreName: String,
        @PathParam("factId") factId: UUID,
    ): Response =
        store
            .resolveStoreOrThrow(factStoreName)
            .let { store.findById(it.id, factId.toFactId()) }
            .toResponse()

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/facts/query")
    suspend fun findByQuery(
        @PathParam("factStoreName") factStoreName: String,
        @Valid factQueryHttp: FactQueryHttp
    ): Response =
        store
            .resolveStoreOrThrow(factStoreName)
            .let { metadata ->
                store.findByTagQuery(metadata.id, factQueryHttp.toTagQuery())
                    .toResponse()
            }

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/subjects/{subjectType}/{subjectId}/facts")
    suspend fun findBySubject(
        @PathParam("factStoreName") factStoreName: String,
        @PathParam("subjectType") subjectType: String,
        @PathParam("subjectId") subjectId: String,
    ): Response =
        store
            .resolveStoreOrThrow(factStoreName)
            .let { metadata ->
                store.findBySubject(metadata.id, SubjectRef(subjectType, subjectId))
            }
            .toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts")
    suspend fun findFacts(
        @PathParam("factStoreName") factStoreName: String,
        @QueryParam("from") from: Instant? = null,
        @QueryParam("to") to: Instant? = null,
    ): Response =
        store
            .resolveStoreOrThrow(factStoreName)
            .let { metadata ->
                store.findInTimeRange(
                    factStoreId = metadata.id,
                    start = from ?: Instant.MIN,
                    end = to ?: Instant.now()
                )
            }
            .toResponse()

}

private fun FindByIdResult.toResponse(): Response {
    return when (this) {
        is FindByIdResult.Found -> Response.ok(fact.toFactHttp()).build()
        is FindByIdResult.NotFound -> Response.status(NOT_FOUND).build()
        is FindByIdResult.FactstoreNotFound -> Response.status(NOT_FOUND).build()
    }
}

private fun List<Fact>.toResponse(): Response =
    Response.ok(map { it.toFactHttp() }).build()
