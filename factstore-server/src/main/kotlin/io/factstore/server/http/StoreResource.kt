package io.factstore.server.http

import io.factstore.core.FactStore
import io.factstore.server.publishTo
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response

@Path("/v1/stores")
class StoreResource(
    private val store: FactStore,
) {

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun createStore(
        request: CreateStoreHttpRequest,
    ): Response =
        request.toDomainRequest().publishTo(store).toResponse()

    @GET
    @Path("/{name}")
    @Produces(APPLICATION_JSON)
    suspend fun findStore(
        @PathParam("name") name: String,
    ): Response =
        findStoreByNameRequest(name).publishTo(store).toResponse()

    @HEAD
    @Path("/{name}")
    suspend fun existsByName(
        @PathParam("name") name: String,
    ): Response =
        existsStoreByNameRequest(name).publishTo(store).toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    suspend fun listFactStores(): Response =
        store.listAll().toResponse()

    @DELETE
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/{name}")
    suspend fun removeStore(
        @PathParam("name") name: String,
    ): Response =
        removeStoreRequest(name).publishTo(store).toResponse()

}
