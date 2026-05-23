package io.factstore.server.http

import io.factstore.core.CreateStoreRequest
import io.factstore.core.CreateStoreResult
import io.factstore.core.FactStore
import io.factstore.core.RemoveStoreRequest
import io.factstore.core.RemoveStoreResult
import io.factstore.core.StoreMetadata
import io.factstore.core.StoreName
import io.factstore.server.http.validation.ValidStoreName
import jakarta.validation.Valid
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
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
        @Valid request: CreateStoreHttpRequest
    ): Response = store
        .handle(CreateStoreRequest(StoreName(request.name)))
        .toResponse()

    private fun CreateStoreResult.toResponse(): Response = when (this) {
        is CreateStoreResult.Created -> Response
            .status(Response.Status.CREATED)
            .entity(mapOf("id" to id.uuid))
            .build()

        is CreateStoreResult.NameAlreadyExists -> storeAlreadyExistsError(storeName)
    }

    @GET
    @Path("/{name}")
    @Produces(APPLICATION_JSON)
    suspend fun findStore(
        @PathParam("name") @ValidStoreName name: String
    ): Response = StoreName(name).let { storeName ->
        store.findByName(storeName)?.toResponse() ?: storeNotFoundError(storeName)
    }

    @HEAD
    @Path("/{name}")
    suspend fun existsByName(
        @PathParam("name") @ValidStoreName name: String
    ): Response {
        store.existsByName(StoreName(name)).let { exists ->
            return if (exists) {
                Response.ok().build()
            } else {
                Response.status(Response.Status.NOT_FOUND).build()
            }
        }
    }

    @GET
    @Produces(APPLICATION_JSON)
    suspend fun listFactStores(): Response {
        return store.listAll().toResponse()
    }

    private fun StoreMetadata.toHttp() = StoreMetadataHttp(id = id.uuid, name = name.value, createdAt = createdAt)

    private fun StoreMetadata.toResponse(): Response = Response.ok(toHttp()).build()

    private fun List<StoreMetadata>.toResponse(): Response = Response.ok(map { it.toHttp() }).build()

    @DELETE
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/{name}")
    suspend fun removeStore(
        @PathParam("name") @ValidStoreName name: String
    ): Response =
        store
            .handle(RemoveStoreRequest(StoreName(name)))
            .toResponse()

    private fun RemoveStoreResult.toResponse(): Response = when (this) {
        is RemoveStoreResult.StoreRemoved -> Response.ok().build()
        is RemoveStoreResult.StoreNotFound -> storeNotFoundError(storeName)
    }

}
