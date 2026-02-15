package org.factstore.server.http

import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.PathParam
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.MediaType.SERVER_SENT_EVENTS
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.factstore.core.StreamingOptionSet
import org.factstore.core.toFactId
import org.factstore.server.FactStoreProvider
import org.jboss.resteasy.reactive.RestStreamElementType
import java.util.UUID

@Path("/v1/stores/{factStoreName}/facts/stream")
class StreamResource(
    private val factStoreProvider: FactStoreProvider
) {

    @GET
    @RestStreamElementType(APPLICATION_JSON)
    @Produces(SERVER_SENT_EVENTS)
    suspend fun streamFacts(
        @PathParam("factStoreName") factStoreName: String,
        @QueryParam("after") after: UUID?,
    ): Flow<FactHttp> =
        factStoreProvider
            .findByName(factStoreName)
            .streamAll(StreamingOptionSet(lastSeenId = after?.toFactId()))
            .map { it.toFactHttp() }

}
