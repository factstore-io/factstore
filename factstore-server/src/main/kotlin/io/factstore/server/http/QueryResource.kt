package io.factstore.server.http

import io.factstore.core.*
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import io.smallrye.mutiny.Multi
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
        store.findById(findByIdRequest(storeName, factId)).toResponse()

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/facts:query")
    @Operation(
        summary = "Stream the facts matching a query",
        description = "A fact matches when it matches any of the query's items, and a fact matching " +
            "several of them is streamed once. " + FACT_STREAM_DESCRIPTION,
    )
    fun queryFacts(
        @PathParam("storeName") storeName: String,
        request: QueryFactsHttpRequest,
    ): Multi<FactStreamLineHttp> =
        streamOfLines { store.streamFacts(request.toDomainRequest(storeName)).toFactStreamLines() }

    @GET
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/facts")
    @Operation(
        summary = "Stream the facts of a store, all of them or those of one subject, type and tag selection",
        description = FACT_STREAM_DESCRIPTION,
    )
    fun streamFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("subject") @Parameter(description = "A subject to match. Repeatable: a fact matches any of them.") subjects: List<String> = emptyList(),
        @QueryParam("type") @Parameter(description = "A type to match, exactly. Repeatable: a fact matches any of them.") types: List<String> = emptyList(),
        @QueryParam("tag") @Parameter(description = "A tag the facts must carry, as key=value. Repeatable: a fact must carry all of them.") tags: List<String> = emptyList(),
        @QueryParam("continueAfter") @Parameter(description = "Continue after this fact, exclusive and in reading order. An empty value counts as not given, and reads from the start.", schema = Schema(type = SchemaType.STRING, format = "uuid")) continueAfter: String? = null,
        @QueryParam("direction") @Parameter(schema = Schema(enumeration = ["forward", "backward"], defaultValue = "forward")) direction: String? = null,
        @QueryParam("limit") @Parameter(schema = Schema(type = SchemaType.INTEGER, minimum = "1")) limit: String? = null,
    ): Multi<FactStreamLineHttp> = streamOfLines {
        // Without criteria the whole store is read. Any criteria form one query item, and the
        // store chooses the cheapest read for it.
        store.streamFacts(streamFactsRequest(storeName, subjects, types, tags, continueAfter, direction, limit)).toFactStreamLines()
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
