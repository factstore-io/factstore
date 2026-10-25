package io.factstore.server.http

import io.factstore.core.FactStore
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
        store.createStore(request.toDomainRequest()).toResponse()

    @GET
    @Path("/{name}")
    @Produces(APPLICATION_JSON)
    suspend fun findStore(
        @PathParam("name") name: String,
    ): Response =
        store.findStoreByName(findStoreByNameRequest(name)).toResponse()

    @HEAD
    @Path("/{name}")
    suspend fun existsStoreByName(
        @PathParam("name") name: String,
    ): Response =
        store.existsStoreByName(existsStoreByNameRequest(name)).toResponse()

    @GET
    @Produces(APPLICATION_JSON)
    suspend fun listFactStores(): Response =
        store.listStores().toResponse()

    @DELETE
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/{name}")
    suspend fun removeStore(
        @PathParam("name") name: String,
    ): Response =
        store.removeStore(removeStoreRequest(name)).toResponse()

}
