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
import kotlinx.coroutines.flow.transform
import org.jboss.resteasy.reactive.RestStreamElementType
import java.util.*

@Path("/v1/stores/{storeName}/facts")
class StreamResource(
    private val factStore: FactStore,
) {

    /** Live subscription (never completes on its own). */
    @GET
    @Path("subscribe")
    @RestStreamElementType(APPLICATION_JSON)
    @Produces(SERVER_SENT_EVENTS)
    @Suppress("kotlin:S6309")
    suspend fun subscribeFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("after") after: UUID? = null,
        @QueryParam("from") from: String? = null,
    ): Flow<FactHttp> =
        factStore.subscribe(
            SubscribeRequest(
                storeName = StoreName(storeName),
                startPosition = buildStartPosition(after, from?.lowercase(Locale.ENGLISH)),
            )
        ).toResponse()

    /** Bounded replay (completes at the head pinned at call time). */
    @GET
    @Path("replay")
    @RestStreamElementType(APPLICATION_JSON)
    @Produces(SERVER_SENT_EVENTS)
    @Suppress("kotlin:S6309")
    suspend fun replayFacts(
        @PathParam("storeName") storeName: String,
        @QueryParam("after") after: UUID? = null,
    ): Flow<FactHttp> =
        factStore.replay(
            ReplayRequest(
                storeName = StoreName(storeName),
                start = after?.let { ReplayStart.After(it.toFactId()) } ?: ReplayStart.Beginning,
            )
        ).toResponse()

    private fun buildStartPosition(after: UUID?, from: String?): StartPosition = when (from) {
        "beginning" -> StartPosition.Beginning
        "end" -> StartPosition.End
        else -> after?.let { StartPosition.After(it.toFactId()) } ?: StartPosition.Beginning
    }

}

private fun SubscribeResult.toResponse(): Flow<FactHttp> = when (this) {
    is SubscribeResult.StoreNotFound -> throw StoreNotFoundException(storeName)
    is SubscribeResult.FactIdNotFound -> throw FactNotFoundException(this.id)
    is SubscribeResult.FactStream -> this.stream.transform { batch -> batch.forEach { emit(it.toFactHttp()) } }
}

private fun ReplayResult.toResponse(): Flow<FactHttp> = when (this) {
    is ReplayResult.StoreNotFound -> throw StoreNotFoundException(storeName)
    is ReplayResult.FactIdNotFound -> throw FactNotFoundException(this.id)
    is ReplayResult.FactStream -> this.stream.transform { batch -> batch.forEach { emit(it.toFactHttp()) } }
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
