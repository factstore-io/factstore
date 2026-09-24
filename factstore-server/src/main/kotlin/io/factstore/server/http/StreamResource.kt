package io.factstore.server.http

import io.factstore.core.*
import io.factstore.server.publishTo
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.MediaType.SERVER_SENT_EVENTS
import kotlinx.coroutines.flow.Flow
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType
import org.eclipse.microprofile.openapi.annotations.media.Schema
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter
import org.jboss.resteasy.reactive.RestStreamElementType

@Path("/v1/stores/{storeName}/facts")
class StreamResource(
    private val factStore: FactStore,
) {

    /** Live subscription (never completes on its own). */
    @GET
    @Path("subscribe")
    @RestStreamElementType(APPLICATION_JSON)
    @Produces(SERVER_SENT_EVENTS)
    @Suppress("kotlin:S6309")
    suspend fun subscribeFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("after") @Parameter(schema = Schema(type = SchemaType.STRING, format = "uuid")) after: String?,
        @QueryParam("from") @Parameter(schema = Schema(enumeration = ["beginning", "end"])) from: String?,
    ): Flow<FactHttp> =
        subscribeRequest(storeName, after, from).publishTo(factStore).toResponse()

}
