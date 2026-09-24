package io.factstore.server.http

import io.factstore.core.*
import io.factstore.server.publishTo
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import kotlinx.coroutines.flow.Flow
import org.eclipse.microprofile.openapi.annotations.Operation
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType
import org.eclipse.microprofile.openapi.annotations.media.Schema
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter
import org.jboss.resteasy.reactive.common.util.RestMediaType.APPLICATION_NDJSON
import org.jboss.resteasy.reactive.RestStreamElementType

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

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/facts:query")
    @Operation(
        summary = "Stream the facts matching a query",
        description = "A fact matches when it matches any of the query's filters, and a fact matching " +
            "several of them is streamed once. " + FACT_STREAM_DESCRIPTION,
    )
    suspend fun streamFactsByQuery(
        @PathParam("storeName") storeName: String,
        request: StreamFactsByQueryHttpRequest,
    ): Flow<FactStreamLineHttp> =
        request.toDomainRequest(storeName).publishTo(store).toResponse()

    @GET
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/subjects/{subject}/facts")
    @Operation(
        summary = "Stream the facts of a subject",
        description = FACT_STREAM_DESCRIPTION,
    )
    suspend fun streamFactsBySubject(
        @PathParam("storeName") storeName: String,
        @PathParam("subject") subject: String,
        @QueryParam("continueAfter") @Parameter(description = "Continue after this fact, exclusive and in reading order.", schema = Schema(type = SchemaType.STRING, format = "uuid")) continueAfter: String?,
        @QueryParam("direction") @Parameter(schema = Schema(enumeration = ["forward", "backward"], defaultValue = "forward")) direction: String?,
        @QueryParam("limit") @Parameter(schema = Schema(type = SchemaType.INTEGER, minimum = "1")) limit: String?,
    ): Flow<FactStreamLineHttp> =
        streamFactsBySubjectRequest(storeName, subject, continueAfter, direction, limit).publishTo(store).toResponse()

    @GET
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/types/{type}/facts")
    @Operation(
        summary = "Stream the facts of one type, matched exactly",
        description = FACT_STREAM_DESCRIPTION,
    )
    suspend fun streamFactsByType(
        @PathParam("storeName") storeName: String,
        @PathParam("type") type: String,
        @QueryParam("continueAfter") @Parameter(description = "Continue after this fact, exclusive and in reading order.", schema = Schema(type = SchemaType.STRING, format = "uuid")) continueAfter: String?,
        @QueryParam("direction") @Parameter(schema = Schema(enumeration = ["forward", "backward"], defaultValue = "forward")) direction: String?,
        @QueryParam("limit") @Parameter(schema = Schema(type = SchemaType.INTEGER, minimum = "1")) limit: String?,
    ): Flow<FactStreamLineHttp> =
        streamFactsByTypeRequest(storeName, type, continueAfter, direction, limit).publishTo(store).toResponse()

    @GET
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/facts")
    @Operation(
        summary = "Stream the facts of a store, optionally filtered by tags or a time range",
        description = FACT_STREAM_DESCRIPTION,
    )
    suspend fun streamFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("continueAfter") @Parameter(description = "Continue after this fact, exclusive and in reading order.", schema = Schema(type = SchemaType.STRING, format = "uuid")) continueAfter: String?,
        @QueryParam("direction") @Parameter(schema = Schema(enumeration = ["forward", "backward"], defaultValue = "forward")) direction: String?,
        @QueryParam("limit") @Parameter(schema = Schema(type = SchemaType.INTEGER, minimum = "1")) limit: String?,
        @QueryParam("from") @Parameter(schema = Schema(type = SchemaType.STRING, format = "date-time")) from: String?,
        @QueryParam("to") @Parameter(schema = Schema(type = SchemaType.STRING, format = "date-time")) to: String?,
        @QueryParam("tag") @Parameter(description = "A tag the facts must carry, as key=value. Repeatable.") tags: List<String> = emptyList(),
    ): Flow<FactStreamLineHttp> =
        when {
            tags.isNotEmpty() ->
                streamFactsByTagsRequest(storeName, tags, from, to, continueAfter, direction, limit)
                    .publishTo(store).toResponse()

            from != null || to != null ->
                findInTimeRangeRequest(storeName, from, to, limit, direction).publishTo(store).toResponse()

            else ->
                streamFactsRequest(storeName, continueAfter, direction, limit).publishTo(store).toResponse()
        }

    private companion object {
        const val FACT_STREAM_DESCRIPTION =
            "Responds with NDJSON: one JSON object per line, with exactly one property. A `fact` line " +
                "carries a fact; the stream then closes with an `end` line, holding the number of facts sent, " +
                "or with an `error` line if it failed part way. A stream that stops without an `end` or " +
                "`error` line is incomplete. A missing store or invalid input is answered before the stream " +
                "starts, with an ApiError."
    }

}
