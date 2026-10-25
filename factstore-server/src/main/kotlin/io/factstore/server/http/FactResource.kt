package io.factstore.server.http

import io.factstore.core.*
import io.smallrye.mutiny.Multi
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.openapi.annotations.Operation
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType
import org.eclipse.microprofile.openapi.annotations.media.Schema
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter
import org.jboss.resteasy.reactive.RestStreamElementType
import org.jboss.resteasy.reactive.common.util.RestMediaType.APPLICATION_NDJSON

/** The facts of a store: appending them, reading one, streaming them, and subscribing to them. */
@Path("/v1/stores/{storeName}")
class FactResource(
    private val factStore: FactStore,
) {

    @POST
    @Path("/facts")
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun appendFacts(
        @PathParam("storeName") storeName: String,
        httpRequest: AppendHttpRequest,
    ): Response =
        factStore.append(httpRequest.toDomainRequest(storeName)).toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/facts/{factId}")
    suspend fun findById(
        @PathParam("storeName") storeName: String,
        @PathParam("factId") @Parameter(schema = Schema(type = SchemaType.STRING, format = "uuid")) factId: String,
    ): Response =
        factStore.findById(findByIdRequest(storeName, factId)).toResponse()

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
        factStore.streamFacts(streamFactsRequest(storeName, subjects, types, tags, continueAfter, direction, limit)).toFactStreamLines()
    }

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
        streamOfLines { factStore.streamFacts(request.toDomainRequest(storeName)).toFactStreamLines() }

    @GET
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/facts:subscribe")
    @Operation(
        summary = "Subscribe to the facts of a store",
        description = SUBSCRIPTION_DESCRIPTION,
    )
    fun subscribeFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("from") @Parameter(description = "Where to start: the store's beginning, or its end to receive only what is appended next.", schema = Schema(enumeration = ["beginning", "end"], defaultValue = "beginning")) from: String? = null,
        @QueryParam("continueAfter") @Parameter(description = "Continue after this fact, exclusive. Cannot be combined with 'from'. An empty value counts as not given.", schema = Schema(type = SchemaType.STRING, format = "uuid")) continueAfter: String? = null,
    ): Multi<FactStreamLineHttp> =
        streamOfLines { factStore.subscribe(subscribeRequest(storeName, from, continueAfter)).toSubscriptionLines() }

    private companion object {
        const val FACT_STREAM_DESCRIPTION =
            "Responds with NDJSON: one JSON object per line, with exactly one property. A `fact` line " +
                "carries a fact; the stream then closes with an `end` line, holding the number of facts sent, " +
                "or with an `error` line if it failed part way. A stream that stops without an `end` or " +
                "`error` line is incomplete. A missing store or invalid input is answered before the stream " +
                "starts, with an ApiError."

        const val SUBSCRIPTION_DESCRIPTION =
            "Drains the facts the store already holds and then keeps emitting the facts appended " +
                "next, for as long as the reader reads: the response never ends on its own. Every " +
                "fact of the store is emitted; reading a part of one is what the fact streams are " +
                "for. " +
                "Responds with NDJSON, as the fact streams do, but with no `end` line: a `fact` line " +
                "per fact, a `heartbeat` line while nothing is appended, and an `error` line if the " +
                "subscription fails. A missing store or invalid input is answered before the stream " +
                "starts, with an ApiError. To resume a subscription that was cut off, pass the last " +
                "fact you processed as `continueAfter`."
    }

}
