package io.factstore.server.http

import io.factstore.core.CreateFactStoreRequest
import io.factstore.core.CreateFactStoreResult
import io.factstore.core.FactStore
import io.factstore.core.FactStoreName
import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response

@Path("/v1/stores")
class FactStoreResource(
    private val store: FactStore,
) {

    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun createFactStore(
        @Valid request: CreateFactStoreHttpRequest
    ): Response = store
        .handle(CreateFactStoreRequest(FactStoreName(request.name)))
        .toResponse()

    private fun CreateFactStoreResult.toResponse(): Response {
        return when (this) {
            is CreateFactStoreResult.Created -> Response
                .status(Response.Status.CREATED)
                .entity(id.uuid)
                .build()
            is CreateFactStoreResult.NameAlreadyExists -> Response
                .status(Response.Status.CONFLICT)
                .entity("A fact store with the name '${factStoreName}' already exists.")
                .build()
        }
    }

    @HEAD
    @Path("/{name}")
    suspend fun factStoreExists(
        @PathParam("name") name: String
    ): Response {
        store.existsByName(FactStoreName(name)).let { exists ->
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
        store.listAll().let { factStores ->
            val response = factStores.map { factStore ->
                FactStoreMetadataHttp(
                    id = factStore.id.uuid,
                    name = factStore.name.value,
                    createdAt = factStore.createdAt
                )
            }
            return Response.ok(response).build()
        }
    }
}
