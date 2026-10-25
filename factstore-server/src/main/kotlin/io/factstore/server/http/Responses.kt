package io.factstore.server.http

import io.factstore.core.*
import io.smallrye.mutiny.Multi
import io.vertx.core.Vertx
import io.vertx.kotlin.coroutines.dispatcher
import jakarta.ws.rs.core.Response
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.jdk9.asPublisher
import kotlinx.coroutines.selects.onTimeout
import kotlinx.coroutines.selects.select
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/*
 * Renders the specification's results as HTTP responses.
 */

internal fun AppendResult.toResponse(): Response = when (this) {
    is AppendResult.Appended -> Response.ok(AppendedHttp(factIds.map { it.uuid }, appendedAt)).build()
    is AppendResult.AlreadyApplied -> Response.ok().build()
    is AppendResult.AppendConditionViolated -> appendConditionViolatedError()
    is AppendResult.StoreNotFound -> storeNotFoundError(storeName)
    is AppendResult.AfterFactNotFound -> afterFactNotFoundError(factId)
}

internal fun FindByIdResult.toResponse(): Response = when (this) {
    is FindByIdResult.Found -> Response.ok(fact.toHttp()).build()
    is FindByIdResult.NotFound -> factNotFoundError(id)
    is FindByIdResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun CreateStoreResult.toResponse(): Response = when (this) {
    is CreateStoreResult.Created -> Response
        .status(Response.Status.CREATED)
        .entity(mapOf("id" to id.uuid))
        .build()

    is CreateStoreResult.NameAlreadyExists -> storeAlreadyExistsError(storeName)
}

internal fun FindStoreByNameResult.toResponse(): Response = when (this) {
    is FindStoreByNameResult.Found -> Response.ok(storeMetadata.toHttp()).build()
    is FindStoreByNameResult.NotFound -> storeNotFoundError(storeName)
}

internal fun ExistsStoreByNameResult.toResponse(): Response = when (this) {
    ExistsStoreByNameResult.StoreExists -> Response.ok().build()
    ExistsStoreByNameResult.StoreAbsent -> Response.status(Response.Status.NOT_FOUND).build()
}

internal fun RemoveStoreResult.toResponse(): Response = when (this) {
    is RemoveStoreResult.StoreRemoved -> Response.ok().build()
    is RemoveStoreResult.StoreNotFound -> storeNotFoundError(storeName)
}

internal fun List<StoreMetadata>.toResponse(): Response = Response.ok(map { it.toHttp() }).build()

// ─── NDJSON fact streams ──────────────────────────────────────────────────────

/**
 * Converts the flow into a [Multi] that collects it only as far as its subscriber has requested.
 *
 * Unlike Mutiny's own `asMulti()`, which collects the whole flow into an unbounded queue whatever
 * was requested, this keeps backpressure: returned to Quarkus, a response is read only as fast as
 * the client reads it, so a slow client holds back the read rather than piling it up in memory.
 *
 * The flow is collected on [dispatcher]. Without one, it would continue on whichever thread
 * resumed it after each suspension, such as a timer thread or a database client's thread.
 */
internal fun <T : Any> Flow<T>.toMulti(dispatcher: CoroutineDispatcher): Multi<T> =
    Multi.createFrom().publisher(asPublisher(dispatcher))

/**
 * A stream of lines whose request is made once the response is read.
 *
 * Resources return it from a plain function: Quarkus streams a [Multi] only when the method's
 * declared return type is one, which a suspending function's is not. A failure before the first
 * line, such as a missing store or invalid input, still becomes an error response.
 *
 * The stream runs on the request's Vert.x context, as suspending resource code does, so it must
 * be created on that context.
 */
internal fun streamOfLines(lines: suspend () -> Flow<FactStreamLineHttp>): Multi<FactStreamLineHttp> {
    val context = checkNotNull(Vertx.currentContext()) { "A stream of lines must be created on a Vert.x context." }
    return flow { emitAll(lines()) }.toMulti(context.dispatcher())
}

internal fun StreamFactsResult.toFactStreamLines(): Flow<FactStreamLineHttp> = when (this) {
    is StreamFactsResult.StoreNotFound -> throw StreamApiException.StoreNotFound(storeName)
    is StreamFactsResult.ContinuationNotFound -> throw StreamApiException.ContinuationNotFound(factId)
    is StreamFactsResult.FactStream -> facts.toFactStreamLines()
}

/**
 * Renders facts as the lines of an NDJSON fact stream: a `fact` line per fact, then an `end`
 * line, or an `error` line in place of the `end` line if reading the facts fails.
 *
 * The response ends normally after the `error` line: by then the status 200 has been sent, so
 * the terminal line is the only place the client can learn about the failure.
 */
internal fun Flow<Fact>.toFactStreamLines(): Flow<FactStreamLineHttp> = flow {
    var count = 0L
    emitAll(
        this@toFactStreamLines
            .map<Fact, FactStreamLineHttp> { fact ->
                count++
                FactStreamLineHttp.FactLine(fact.toHttp())
            }
            .onCompletion { cause ->
                if (cause == null) emit(FactStreamLineHttp.EndLine(FactStreamEndHttp(count)))
            }
            .catch { e -> emit(FactStreamLineHttp.ErrorLine(unexpectedError(e))) }
    )
}

// ─── Subscriptions ────────────────────────────────────────────────────────────

internal fun SubscribeResult.toSubscriptionLines(): Flow<FactStreamLineHttp> = when (this) {
    is SubscribeResult.StoreNotFound -> throw StreamApiException.StoreNotFound(storeName)
    is SubscribeResult.ContinuationNotFound -> throw StreamApiException.ContinuationNotFound(factId)
    is SubscribeResult.FactStream -> facts.toSubscriptionLines()
}

/**
 * How long an idle subscription waits before it writes a `heartbeat` line.
 *
 * Short enough to stay under the idle timeout of a proxy that does not know the stream is alive,
 * and long enough that a quiet store costs almost nothing.
 */
private val HEARTBEAT_INTERVAL = 20.seconds

/**
 * Renders a subscription as NDJSON lines: a `fact` line per fact, a `heartbeat` line whenever
 * no fact arrives within [heartbeatEvery], and an `error` line if the subscription fails.
 *
 * There is no `end` line: a subscription is only ever ended by its reader or by a failure.
 */
@OptIn(ExperimentalCoroutinesApi::class)
internal fun Flow<Fact>.toSubscriptionLines(
    heartbeatEvery: Duration = HEARTBEAT_INTERVAL,
): Flow<FactStreamLineHttp> = flow {
    coroutineScope {
        // Read in a coroutine of its own, so writing a heartbeat does not depend on a fact
        // arriving. A failure travels as an element rather than as an exception: thrown, it
        // would cancel this scope before the facts that arrived before it are written.
        val signals = map<Fact, SubscriptionSignal> { SubscriptionSignal.Arrived(it) }
            .catch { emit(SubscriptionSignal.Failed(it)) }
            .produceIn(this)

        var subscribed = true
        while (subscribed) {
            select {
                signals.onReceiveCatching { received ->
                    when (val signal = received.getOrNull()) {
                        // The backend's flow ended although a subscription does not: nothing
                        // more will arrive, and nothing claims the subscription was complete.
                        null -> subscribed = false

                        is SubscriptionSignal.Arrived ->
                            emit(FactStreamLineHttp.FactLine(signal.fact.toHttp()))

                        is SubscriptionSignal.Failed -> {
                            emit(FactStreamLineHttp.ErrorLine(unexpectedError(signal.cause)))
                            subscribed = false
                        }
                    }
                }
                onTimeout(heartbeatEvery) {
                    emit(FactStreamLineHttp.HeartbeatLine(FactStreamHeartbeatHttp(Instant.now())))
                }
            }
        }
        signals.cancel()
    }
}.catch { e -> emit(FactStreamLineHttp.ErrorLine(unexpectedError(e))) }

/** What a subscription's reader hands to its writer: the next fact, or the failure that ended it. */
private sealed interface SubscriptionSignal {
    class Arrived(val fact: Fact) : SubscriptionSignal
    class Failed(val cause: Throwable) : SubscriptionSignal
}

/** A stream cannot return a response, so its outcomes are thrown and rendered by [ErrorMappers]. */
sealed class StreamApiException(message: String) : RuntimeException(message) {
    class StoreNotFound(val storeName: StoreName) : StreamApiException("Store '$storeName' not found.")
    class ContinuationNotFound(val factId: FactId) : StreamApiException("No fact '${factId.uuid}' to continue after.")
}

// ─── Facts and stores in their HTTP shape ─────────────────────────────────────

internal fun Fact.toHttp() = FactHttp(
    id = id.uuid,
    type = type.value,
    subject = subject.value,
    appendedAt = appendedAt,
    payload = payload.toHttp(),
    metadata = metadata.entries.associate { Pair(it.key.value, it.value.value) },
    tags = tags.entries.associate { Pair(it.key.value, it.value.value) },
)

internal fun FactPayload.toHttp() = FactPayloadHttp(data = data)

internal fun StoreMetadata.toHttp() = StoreMetadataHttp(id = id.uuid, name = name.value, createdAt = createdAt)
