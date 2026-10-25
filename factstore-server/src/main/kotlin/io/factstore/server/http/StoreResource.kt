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
        store.create(request.toDomainRequest()).toResponse()

    @GET
    @Path("/{name}")
    @Produces(APPLICATION_JSON)
    suspend fun findStore(
        @PathParam("name") name: String,
    ): Response =
        store.findByName(findStoreByNameRequest(name)).toResponse()

    @HEAD
    @Path("/{name}")
    suspend fun existsByName(
        @PathParam("name") name: String,
    ): Response =
        store.existsByName(existsStoreByNameRequest(name)).toResponse()

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
        store.remove(removeStoreRequest(name)).toResponse()

}
