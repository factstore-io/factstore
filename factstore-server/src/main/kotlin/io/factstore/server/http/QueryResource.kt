package io.factstore.server.http

import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.NOT_FOUND
import io.factstore.core.Fact
import io.factstore.core.FactStore
import io.factstore.core.FactStoreFinder
import io.factstore.core.SubjectRef
import io.factstore.core.toFactId
import java.time.Instant
import java.util.*

@Path("/v1/stores/{factStoreName}")
class QueryResource(
    private val finder: FactStoreFinder,
    private val factStore: FactStore
) {

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts/{factId}")
    suspend fun findById(
        @PathParam("factStoreName") factStoreName: String,
        @PathParam("factId") factId: UUID,
    ): Response =
        finder
            .resolveStoreOrThrow(factStoreName)
            .let { factStore.findById(it.id, factId.toFactId()) }
            .toResponse()

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/facts/query")
    suspend fun findByQuery(
        @PathParam("factStoreName") factStoreName: String,
        @Valid factQueryHttp: FactQueryHttp
    ): Response =
        finder
            .resolveStoreOrThrow(factStoreName)
            .let { metadata ->
                factStore.findByTagQuery(metadata.id, factQueryHttp.toTagQuery())
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
        finder
            .resolveStoreOrThrow(factStoreName)
            .let { metadata ->
                factStore.findBySubject(metadata.id, SubjectRef(subjectType, subjectId))
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
        finder
            .resolveStoreOrThrow(factStoreName)
            .let { metadata ->
                factStore.findInTimeRange(
                    factStoreId = metadata.id,
                    start = from ?: Instant.MIN,
                    end = to ?: Instant.now()
                )
            }
            .toResponse()

}

private fun Fact?.toResponse(): Response {
    return this?.let {
        Response.ok(toFactHttp()).build()
    } ?: Response.status(NOT_FOUND).build()
}

private fun List<Fact>.toResponse(): Response =
    Response.ok(map { it.toFactHttp() }).build()
