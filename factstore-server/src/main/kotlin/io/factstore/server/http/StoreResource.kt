package io.factstore.server.http

import io.factstore.core.CreateStoreRequest
import io.factstore.core.CreateStoreResult
import io.factstore.core.FactStore
import io.factstore.core.StoreMetadata
import io.factstore.core.StoreName
import jakarta.validation.Valid
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

    @HEAD
    @Path("/{name}")
    suspend fun existsByName(
        @PathParam("name") name: String
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

    private fun List<StoreMetadata>.toResponse(): Response {
        val stores = this.map { store ->
            StoreMetadataHttp(
                id = store.id.uuid,
                name = store.name.value,
                createdAt = store.createdAt
            )
        }
        return Response.ok(stores).build()
    }
}
