package io.factstore.cli.client

import io.smallrye.mutiny.Multi
import jakarta.ws.rs.Consumes
import jakarta.ws.rs.DELETE
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.MediaType.SERVER_SENT_EVENTS
import jakarta.ws.rs.core.Response
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider
import java.time.Instant
import java.util.UUID

@Path("/v1/")
@RegisterProvider(FactStoreClientExceptionMapper::class)
interface FactStoreClient {

    @POST
    @Path("/stores")
    fun createStore(request: CreateStoreRequest): Response

    @GET
    @Path("/stores")
    fun listStores(): List<StoreMetadata>

    @GET
    @Produces(SERVER_SENT_EVENTS)
    @Path("/stores/{store}/facts/stream")
    fun streamFacts(
        @PathParam("store") storeName: String,
        @QueryParam("from") from: String?,
        @QueryParam("after") after: UUID?,
    ): Multi<FactHttp>

    @POST
    @Path("/stores/{store}/facts")
    @Consumes(APPLICATION_JSON)
    fun appendFact(
        @PathParam("store") store: String,
        request: AppendHttpRequest
    )

    @DELETE
    @Consumes(APPLICATION_JSON)
    @Produces(APPLICATION_JSON)
    @Path("/stores/{store}")
    fun removeStore(
        @PathParam("store") storeName: String
    ): Response

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/stores/{store}/facts/{factId}")
    fun findFact(
        @PathParam("store") store: String,
        @PathParam("factId") factId: UUID,
    ): FactHttp

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/stores/{store}/subjects/{subject}/facts")
    fun findFactsBySubject(
        @PathParam("store") storeName: String,
        @PathParam("subject") subject: String,
        @QueryParam("limit") limit: Int? = null,
        @QueryParam("direction") direction: String? = null,
    ): List<FactHttp>

    @GET
    @Produces(APPLICATION_JSON)
    @Path("/stores/{store}/facts")
    fun findFacts(
        @PathParam("store") storeName: String,
        @QueryParam("from") from: Instant? = null,
        @QueryParam("to") to: Instant? = null,
        @QueryParam("tag") tags: List<String> = emptyList(),
        @QueryParam("limit") limit: Int? = null,
        @QueryParam("direction") direction: String? = null,
    ): List<FactHttp>

}

data class CreateStoreRequest(val name: String)

data class StoreMetadata(
    val id: String,
    val name: String,
    val createdAt: Instant
)

data class FactHttp(
    val id: UUID? = null,
    val type: String,
    val subject: String,
    val appendedAt: Instant? = null,
    val payload: FactPayloadHttp,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap()
)

data class FactPayloadHttp(
    val data: ByteArray,
)

data class AppendHttpRequest(
    val facts: List<FactHttp>,
    val idempotencyKey: UUID? = UUID.randomUUID(),
)
