package io.factstore.server.http

import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response
import io.factstore.server.FactStoreProvider

@Path("/v1/stores/{factStoreName}/facts/append")
class AppendResource(
    private val factStoreProvider: FactStoreProvider
) {

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun appendFacts(
        @PathParam("factStoreName") factStoreName: String,
        @Valid httpRequest: AppendHttpRequest
    ): Response =
        factStoreProvider
            .findByName(factStoreName)
            .append(httpRequest.toAppendRequest())
            .toResponse()

}
