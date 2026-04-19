package io.factstore.server.http

import io.factstore.core.*
import jakarta.ws.rs.*
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.MediaType.SERVER_SENT_EVENTS
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.jboss.resteasy.reactive.RestStreamElementType
import java.util.*

@Path("/v1/stores/{factStoreName}/facts/stream")
class StreamResource(
    private val finder: FactStoreFinder,
    private val factStore: FactStore,
) {

    @GET
    @RestStreamElementType(APPLICATION_JSON)
    @Produces(SERVER_SENT_EVENTS)
    suspend fun streamFacts(
        @PathParam("factStoreName") factStoreName: String,
        @QueryParam("after") after: UUID? = null,
        @QueryParam("from") from: String? = null,
    ): Flow<FactHttp> =
        finder
            .resolveStoreOrThrow(factStoreName)
            .let { metadata ->
                val streamResult = factStore.stream(
                    factStoreId = metadata.id,
                    streamingOptions = buildStreamingOptions(after, from?.lowercase(Locale.ENGLISH))
                )
                streamResult.toResponse()
            }


    private fun buildStreamingOptions(after: UUID?, from: String?): StreamingOptions {
        val startPosition = when (from) {
            "beginning" -> StartPosition.Beginning
            "end" -> StartPosition.End
            else -> after?.let { StartPosition.After(it.toFactId()) } ?: StartPosition.Beginning
        }
        return StreamingOptions(startPosition)
    }

}

private fun StreamResult.toResponse(): Flow<FactHttp> = when (this) {
    is StreamResult.FactStoreNotFound -> throw NotFoundException("Fact store not found")
    is StreamResult.InvalidStartPosition -> throw InvalidFactIdException(this.id)
    is StreamResult.FactStream -> this.stream.map { it.toFactHttp() }
}

class InvalidFactIdException(val factId: FactId) : RuntimeException()

@Provider
class InvalidFactIdExceptionMapper : ExceptionMapper<InvalidFactIdException> {

    override fun toResponse(exception: InvalidFactIdException): Response? {
        return Response.status(422).entity("${exception.factId.uuid} does not exist" ).build()
    }

}
