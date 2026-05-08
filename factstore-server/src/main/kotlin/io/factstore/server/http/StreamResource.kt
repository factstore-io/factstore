package io.factstore.server.http

import io.factstore.core.*
import io.factstore.server.http.StreamApiException.FactNotFoundException
import io.factstore.server.http.StreamApiException.StoreNotFoundException
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

@Path("/v1/stores/{storeName}/facts/stream")
class StreamResource(
    private val factStore: FactStore,
) {

    @GET
    @RestStreamElementType(APPLICATION_JSON)
    @Produces(SERVER_SENT_EVENTS)
    @Suppress("kotlin:S6309")
    suspend fun streamFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("after") after: UUID? = null,
        @QueryParam("from") from: String? = null,
    ): Flow<FactHttp> =
        factStore.stream(
            storeName = StoreName(storeName),
            streamingOptions = buildStreamingOptions(after, from?.lowercase(Locale.ENGLISH))
        )
            .toResponse()


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
    is StreamResult.StoreNotFound -> throw StoreNotFoundException(storeName)
    is StreamResult.FactIdNotFound -> throw FactNotFoundException(this.id)
    is StreamResult.FactStream -> this.stream.map { it.toFactHttp() }
}

sealed class StreamApiException : RuntimeException() {
    data class FactNotFoundException(val factId: FactId) : StreamApiException()
    data class StoreNotFoundException(val storeName: StoreName) : StreamApiException()
}

@Provider
class StreamApiExceptionExceptionMapper : ExceptionMapper<StreamApiException> {

    override fun toResponse(exception: StreamApiException): Response = when (exception) {
        is StoreNotFoundException -> storeNotFoundError(exception.storeName)
        is FactNotFoundException -> factNotFoundError(exception.factId)
    }

}
