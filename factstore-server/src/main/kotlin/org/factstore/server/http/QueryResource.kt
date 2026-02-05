package org.factstore.server.http

import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.NOT_FOUND
import org.factstore.core.Fact
import org.factstore.core.SubjectRef
import org.factstore.core.toFactId
import org.factstore.server.FactStoreProvider
import java.time.Instant
import java.util.*

@Path("/v1/stores/{factStoreName}")
class QueryResource(
    private val factStoreProvider: FactStoreProvider
) {

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts/{factId}")
    suspend fun findById(
        @PathParam("factStoreName") factStoreName: String,
        @PathParam("factId") factId: UUID,
    ): Response =
        factStoreProvider
            .findByName(factStoreName)
            .findById(factId.toFactId())
            .toResponse()

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/facts/query")
    suspend fun findByQuery(
        @PathParam("factStoreName") factStoreName: String,
        @Valid factQueryHttp: FactQueryHttp
    ): Response =
        factStoreProvider
            .findByName(factStoreName)
            .findByTagQuery(factQueryHttp.toTagQuery())
            .toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/subjects/{subjectType}/{subjectId}/facts")
    suspend fun findBySubject(
        @PathParam("factStoreName") factStoreName: String,
        @PathParam("subjectType") subjectType: String,
        @PathParam("subjectId") subjectId: String,
    ): Response =
        factStoreProvider
            .findByName(factStoreName)
            .findBySubject(SubjectRef(subjectType, subjectId))
            .toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts")
    suspend fun findFacts(
        @PathParam("factStoreName") factStoreName: String,
        @QueryParam("from") from: Instant,
        @QueryParam("to") to: Instant?,
    ): Response =
        factStoreProvider
            .findByName(factStoreName)
            .findInTimeRange(from, to ?: Instant.now())
            .toResponse()

}

private fun Fact?.toResponse(): Response {
    return this?.let {
        Response.ok(toFactHttp()).build()
    } ?: Response.status(NOT_FOUND).build()
}

private fun List<Fact>.toResponse(): Response =
    Response.ok(map { it.toFactHttp() }).build()
