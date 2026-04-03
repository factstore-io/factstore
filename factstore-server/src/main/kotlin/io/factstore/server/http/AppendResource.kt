package io.factstore.server.http

import io.factstore.core.FactStore
import io.factstore.core.FactStoreFinder
import io.factstore.foundationdb.FdbFactStoreFinder
import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response

@Path("/v1/stores/{factStoreName}/facts")
class AppendResource(
    private val factStoreFinder: FactStoreFinder,
    private val factStore: FactStore
) {

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun appendFacts(
        @PathParam("factStoreName") factStoreName: String,
        @Valid httpRequest: AppendHttpRequest
    ): Response {
        val factstoreId = factStoreFinder.resolveStoreOrThrow(factStoreName).id
        val appendRequest = httpRequest.toAppendRequest(factstoreId)
        return factStore.append(appendRequest).toResponse()
    }

}
