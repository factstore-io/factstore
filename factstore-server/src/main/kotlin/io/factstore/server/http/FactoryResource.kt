package io.factstore.server.http

import io.factstore.core.FactStoreFactory
import jakarta.validation.Valid
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.Response

/**
 * REST API endpoints for managing FactStore lifecycle.
 *
 * This resource provides HTTP access to FactStoreFactory operations:
 * - Create new fact stores
 * - Retrieve fact store metadata
 * - Check fact store existence
 * - Delete fact stores
 * - List all fact stores
 *
 * All endpoints follow REST conventions and return appropriate HTTP status codes.
 */
@Path("/v1/stores")
class FactoryResource(
    private val factory: FactStoreFactory
) {

    /**
     * Create a new fact store.
     *
     * POST /v1/stores
     * Content-Type: application/json
     *
     * Request body:
     * {
     *   "name": "my-store"
     * }
     *
     * Response: 201 Created
     * {
     *   "name": "my-store",
     *   "id": "550e8400-e29b-41d4-a716-446655440000",
     *   "createdAt": 1645000000000
     * }
     *
     * Error responses:
     * - 400 Bad Request: Invalid fact store name
     * - 409 Conflict: Fact store with this name already exists
     */
    @POST
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    suspend fun createFactStore(
        @Valid request: CreateFactStoreHttpRequest
    ): Response {
        val metadata = factory.create(request.name)
        return Response.status(Response.Status.CREATED)
            .entity(metadata.toFactStoreMetadataHttp())
            .build()
    }

    /**
     * Get metadata for a specific fact store.
     *
     * GET /v1/stores/{factStoreName}
     *
     * Response: 200 OK
     * {
     *   "name": "my-store",
     *   "id": "550e8400-e29b-41d4-a716-446655440000",
     *   "createdAt": 1645000000000
     * }
     *
     * Error responses:
     * - 400 Bad Request: Invalid fact store name
     * - 404 Not Found: Fact store does not exist
     */
    @GET
    @Path("/{factStoreName}")
    @Produces(APPLICATION_JSON)
    suspend fun getFactStoreMetadata(
        @PathParam("factStoreName") factStoreName: String
    ): Response {
        val metadata = factory.getMetadata(factStoreName)
        return Response.ok(metadata.toFactStoreMetadataHttp()).build()
    }

    /**
     * Check if a fact store exists.
     *
     * HEAD /v1/stores/{factStoreName}
     *
     * Response: 200 OK (exists) or 404 Not Found (does not exist)
     */
    @HEAD
    @Path("/{factStoreName}")
    suspend fun factStoreExists(
        @PathParam("factStoreName") factStoreName: String
    ): Response {
        val exists = factory.exists(factStoreName)
        return if (exists) {
            Response.ok().build()
        } else {
            Response.status(Response.Status.NOT_FOUND).build()
        }
    }

    /**
     * Delete a fact store and all its facts.
     *
     * DELETE /v1/stores/{factStoreName}
     *
     * Response: 204 No Content
     *
     * Error responses:
     * - 400 Bad Request: Invalid fact store name
     * - 404 Not Found: Fact store does not exist
     */
    @DELETE
    @Path("/{factStoreName}")
    suspend fun deleteFactStore(
        @PathParam("factStoreName") factStoreName: String
    ): Response {
        factory.delete(factStoreName)
        return Response.noContent().build()
    }

    /**
     * List all fact stores.
     *
     * GET /v1/stores
     *
     * Response: 200 OK
     * [
     *   {
     *     "name": "store1",
     *     "id": "550e8400-e29b-41d4-a716-446655440000",
     *     "createdAt": 1645000000000
     *   },
     *   {
     *     "name": "store2",
     *     "id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
     *     "createdAt": 1645000001000
     *   }
     * ]
     */
    @GET
    @Produces(APPLICATION_JSON)
    suspend fun listFactStores(): Response {
        val stores = factory.listAll()
        return Response.ok(stores.toFactStoreMetadataHttpList()).build()
    }
}
