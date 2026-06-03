package io.factstore.server.grpc

import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.future
import kotlinx.coroutines.jdk9.asPublisher
import kotlin.coroutines.CoroutineContext

/**
 * Bridges a suspending block to a Mutiny [Uni].
 *
 * Exists because Quarkus generates gRPC service stubs against Mutiny rather than
 * Kotlin coroutines. We use this at the gRPC adapter boundary to keep service bodies
 * written as ordinary suspend functions.
 *
 * @param context Coroutine context for this call.
 * @param block Suspending body to execute on subscription.
 * @return A cold [Uni] that emits the result of [block] or fails with its exception.
 */
internal fun <T> toUni(
    context: CoroutineContext,
    block: suspend () -> T
): Uni<T> =
    Uni.createFrom().completionStage {
        CoroutineScope(context).future { block() }
    }

/**
 * Bridges a suspending [Flow] producer to a Mutiny [Multi].
 *
 * Exists because Quarkus generates gRPC server-streaming stubs against Mutiny
 * rather than Kotlin coroutines. We use this at the gRPC adapter boundary to keep
 * streaming bodies written as ordinary [Flow]s.
 *
 * @param context Coroutine context for this call.
 * @param block Suspending producer of the [Flow] to stream. Invoked once per
 *   subscription, so any per-call setup belongs inside it.
 * @return A cold [Multi] that mirrors the [Flow] returned by [block].
 */
internal fun <T : Any> toMulti(
    context: CoroutineContext,
    block: suspend () -> Flow<T>,
): Multi<T> =
    Multi.createFrom().publisher(
        flow { emitAll(block()) }.asPublisher(context)
    )
