package io.factstore.server.http

import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.toFactPayload
import io.factstore.core.toFactType
import io.factstore.core.toSubject
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.jboss.resteasy.reactive.RestStreamElementType
import org.jboss.resteasy.reactive.common.util.RestMediaType.APPLICATION_NDJSON
import java.time.Instant

/** Fails on purpose, so tests can observe how unexpected failures are reported. */
@Path("/test/failures")
class FailingTestResource {

    @GET
    @Path("illegal-state")
    fun illegalState(): String = throw IllegalStateException(SECRET)

    /** An [IllegalArgumentException] that does not come from parsing client input. */
    @GET
    @Path("illegal-argument")
    fun illegalArgument(): String = throw IllegalArgumentException(SECRET)

    /** An NDJSON fact stream that fails after its first fact. */
    @GET
    @Path("fact-stream")
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    fun failingFactStream(): Flow<FactStreamLineHttp> =
        flow {
            emit(Fact(FactId.generate(), "T".toFactType(), "{}".toFactPayload(), "s".toSubject(), Instant.now()))
            throw IllegalStateException(SECRET)
        }.toFactStreamLines()

    companion object {
        const val SECRET = "internal detail that must not leak"
    }
}
