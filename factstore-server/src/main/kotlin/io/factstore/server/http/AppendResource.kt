package io.factstore.server.http

import io.factstore.core.FactStore
import io.factstore.server.publishTo
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response

@Path("/v1/stores/{storeName}/facts")
class AppendResource(
    private val factStore: FactStore
) {

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun appendFacts(
        @PathParam("storeName") storeName: String,
        httpRequest: AppendHttpRequest,
    ): Response =
        httpRequest.toDomainRequest(storeName).publishTo(factStore).toResponse()

}
