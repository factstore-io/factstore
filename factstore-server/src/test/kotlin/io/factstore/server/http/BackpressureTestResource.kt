package io.factstore.server.http

import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.FactPayload
import io.factstore.core.toFactType
import io.factstore.core.toSubject
import io.smallrye.mutiny.Multi
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.QueryParam
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import jakarta.ws.rs.core.MediaType.TEXT_PLAIN
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.jboss.resteasy.reactive.RestStreamElementType
import org.jboss.resteasy.reactive.common.util.RestMediaType.APPLICATION_NDJSON
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/** A long fact stream that counts how many facts it has produced, so tests can see how far it ran ahead. */
@Path("/test/backpressure")
class BackpressureTestResource {

    /** Returned like the fact streams of the API. */
    @GET
    @Path("facts")
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    fun facts(@QueryParam("count") count: Int): Multi<FactStreamLineHttp> = counted(count).toFactStreamLines().toMulti()

    @GET
    @Path("produced")
    @Produces(TEXT_PLAIN)
    fun produced(): Int = PRODUCED.get()

    private fun counted(count: Int): Flow<Fact> {
        PRODUCED.set(0)
        val payload = FactPayload(ByteArray(1_000) { 'x'.code.toByte() })
        return flow {
            repeat(count) {
                PRODUCED.incrementAndGet()
                emit(Fact(FactId.generate(), "T".toFactType(), payload, "s".toSubject(), Instant.now()))
            }
        }
    }

    companion object {
        val PRODUCED = AtomicInteger()
    }
}
