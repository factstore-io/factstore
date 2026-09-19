package io.factstore.server.http

import io.factstore.core.*
import io.factstore.server.publishTo
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import kotlinx.coroutines.flow.Flow
import org.eclipse.microprofile.openapi.annotations.Operation
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType
import org.eclipse.microprofile.openapi.annotations.media.Schema
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter
import org.jboss.resteasy.reactive.RestStreamElementType
import org.jboss.resteasy.reactive.common.util.RestMediaType.APPLICATION_NDJSON

@Path("/v1/stores/{storeName}")
class StreamResource(
    private val factStore: FactStore,
) {

    @GET
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    @Path("/facts:subscribe")
    @Operation(
        summary = "Subscribe to the facts of a store",
        description = SUBSCRIPTION_DESCRIPTION,
    )
    suspend fun subscribeFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("from") @Parameter(description = "Where to start: the store's beginning, or its end to receive only what is appended next.", schema = Schema(enumeration = ["beginning", "end"], defaultValue = "beginning")) from: String? = null,
        @QueryParam("continueAfter") @Parameter(description = "Continue after this fact, exclusive. Cannot be combined with 'from'.", schema = Schema(type = SchemaType.STRING, format = "uuid")) continueAfter: String? = null,
    ): Flow<FactStreamLineHttp> =
        subscribeRequest(storeName, from, continueAfter).publishTo(factStore).toResponse()

    private companion object {
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
