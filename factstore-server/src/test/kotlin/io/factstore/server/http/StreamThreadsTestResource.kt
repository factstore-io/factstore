package io.factstore.server.http

import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.toFactPayload
import io.factstore.core.toFactType
import io.factstore.core.toSubject
import io.smallrye.mutiny.Multi
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withContext
import org.jboss.resteasy.reactive.RestStreamElementType
import org.jboss.resteasy.reactive.common.util.RestMediaType.APPLICATION_NDJSON
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.MILLISECONDS

/**
 * A fact stream that suspends the ways the backends do, and records the thread it continues on
 * after each suspension.
 */
@Path("/test/stream-threads")
class StreamThreadsTestResource {

    @GET
    @Produces(APPLICATION_NDJSON)
    @RestStreamElementType(APPLICATION_JSON)
    fun facts(): Multi<FactStreamLineHttp> {
        THREADS.clear()
        record("resource")
        return streamOfLines { suspending().toFactStreamLines() }
    }

    private fun suspending(): Flow<Fact> = flow {
        record("start")
        emit(fact())
        // A timer, as a subscription's heartbeat waits on.
        delay(5)
        record("after a delay")
        emit(fact())
        // A future completed on a thread of its own, as a FoundationDB read is.
        CompletableFuture.supplyAsync({ }, CompletableFuture.delayedExecutor(5, MILLISECONDS, FOREIGN)).await()
        record("after a future completed elsewhere")
        emit(fact())
        // Work moved to another dispatcher, as deserialization is.
        withContext(Dispatchers.Default) { }
        record("after work on another dispatcher")
        emit(fact())
    }

    private fun fact() = Fact(FactId.generate(), "T".toFactType(), "{}".toFactPayload(), "s".toSubject(), Instant.now())

    companion object {
        /** The step and the thread it ran on, in order. */
        val THREADS = CopyOnWriteArrayList<Pair<String, String>>()

        private val FOREIGN = Executors.newSingleThreadExecutor { Thread(it, "foreign-callback").apply { isDaemon = true } }

        // Coroutine debugging appends the coroutine's name to the thread's.
        private fun record(step: String) {
            THREADS += step to Thread.currentThread().name.substringBefore(" @")
        }
    }
}
