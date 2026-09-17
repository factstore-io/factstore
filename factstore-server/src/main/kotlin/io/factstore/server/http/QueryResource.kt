package io.factstore.server.http

import io.factstore.core.*
import io.factstore.server.publishTo
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType
import org.eclipse.microprofile.openapi.annotations.media.Schema
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter

@Path("/v1/stores/{storeName}")
class QueryResource(
    private val store: FactStore
) {

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts/{factId}")
    suspend fun findById(
        @PathParam("storeName") storeName: String,
        @PathParam("factId") @Parameter(schema = Schema(type = SchemaType.STRING, format = "uuid")) factId: String,
    ): Response =
        findByIdRequest(storeName, factId).publishTo(store).toResponse()

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/facts/query")
    suspend fun findByQuery(
        @PathParam("storeName") storeName: String,
        factQueryHttp: FactQueryHttp,
    ): Response =
        factQueryHttp.toDomainRequest(storeName).publishTo(store).toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/subjects/{subject}/facts")
    suspend fun findBySubject(
        @PathParam("storeName") storeName: String,
        @PathParam("subject") subject: String,
        @QueryParam("limit") @Parameter(schema = Schema(type = SchemaType.INTEGER, minimum = "1")) limit: String?,
        @QueryParam("direction") @Parameter(schema = Schema(enumeration = ["forward", "backward"], defaultValue = "forward")) direction: String?,
    ): Response =
        findBySubjectRequest(storeName, subject, limit, direction).publishTo(store).toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts")
    suspend fun findFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("from") @Parameter(schema = Schema(type = SchemaType.STRING, format = "date-time")) from: String?,
        @QueryParam("to") @Parameter(schema = Schema(type = SchemaType.STRING, format = "date-time")) to: String?,
        @QueryParam("tag") @Parameter(description = "A tag the facts must carry, as key=value. Repeatable.") tags: List<String> = emptyList(),
        @QueryParam("limit") @Parameter(schema = Schema(type = SchemaType.INTEGER, minimum = "1")) limit: String?,
        @QueryParam("direction") @Parameter(schema = Schema(enumeration = ["forward", "backward"], defaultValue = "forward")) direction: String?,
    ): Response =
        if (tags.isEmpty()) {
            findInTimeRangeRequest(storeName, from, to, limit, direction).publishTo(store).toResponse()
        } else {
            findByTagsRequest(storeName, tags, from, to, limit, direction).publishTo(store).toResponse()
        }

}
