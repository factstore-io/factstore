package io.factstore.server.grpc

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.factstore.core.*
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.future
import kotlinx.coroutines.jdk9.asPublisher
import java.time.Instant
import java.util.*
import kotlin.coroutines.CoroutineContext
import io.factstore.core.ReadDirection as CoreReadDirection


/**
 * Bridges a suspending block to a Mutiny [Uni].
 *
 * Exists because Quarkus generates gRPC service stubs against Mutiny rather than
 * Kotlin coroutines. Use this at the gRPC adapter boundary to keep service bodies
 * written as ordinary suspend functions.
 *
 * ### Lifecycle
 *
 * Each subscription runs [block] in a fresh [CoroutineScope] built from [context].
 * That scope exists only for the call: when [block] completes, fails, or the [Uni]
 * subscription is canceled, the scope has no remaining children and becomes
 * eligible for GC. No coroutine state outlives the call.
 *
 * ### Cancellation
 *
 * Cancellation propagates end-to-end. Downstream cancellation of the [Uni] cancels
 * the underlying [java.util.concurrent.CompletableFuture], which the [future] builder translates into
 * coroutine cancellation; conversely, exceptions from [block] surface as a failed
 * [Uni].
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
 * rather than Kotlin coroutines. Use this at the gRPC adapter boundary to keep
 * streaming bodies written as ordinary [Flow]s.
 *
 * ### Lifecycle
 *
 * Each subscription invokes [block] and collects the returned [Flow] in a fresh
 * coroutine launched on [context]. That coroutine exists only for the duration
 * of the subscription. No coroutine state outlives the stream.
 *
 * ### Cancellation and back-pressure
 *
 * Demand and cancellation propagate via the reactive-streams contract:
 * downstream cancellation cancels the collector; collector completion or failure
 * terminates the [Multi]. Back-pressure is honoured by [asPublisher].
 *
 * @param context Coroutine context for this call. Supplies the dispatcher (typically
 *   `vertx.dispatcher()`) and may carry other context elements. Do not pass a shared
 *   parent Job — see [toUni] for rationale.
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

internal fun Instant.toTimestamp(): Timestamp = Timestamp.newBuilder()
    .setSeconds(epochSecond)
    .setNanos(nano)
    .build()

internal fun Timestamp.toInstant(): Instant = Instant.ofEpochSecond(seconds, nanos.toLong())

internal fun Int.toLimit(): Limit = if (this > 0) Limit.of(this) else Limit.None

internal fun FactStoreProto.ReadDirection.toCore(): CoreReadDirection = when (this) {
    FactStoreProto.ReadDirection.BACKWARD -> CoreReadDirection.Backward
    else -> CoreReadDirection.Forward
}

internal fun Fact.toProto(): FactStoreProto.Fact = fact {
    id = this@toProto.id.uuid.toString()
    type = this@toProto.type.value
    subject = this@toProto.subject.value
    appendedAt = this@toProto.appendedAt.toTimestamp()
    payload = this@toProto.payload.toProto()
    metadata.putAll(this@toProto.metadata)
    tags.putAll(this@toProto.tags.entries.associate { (k, v) -> k.value to v.value })
}

internal fun FactPayload.toProto(): FactStoreProto.FactPayload = factPayload {
    data = ByteString.copyFrom(this@toProto.data)
    this@toProto.format?.let { format = it.value }
    this@toProto.schema?.let { schemaRef = it.value }
}

internal fun StoreMetadata.toProto(): FactStoreProto.StoreInfo = storeInfo {
    id = this@toProto.id.uuid.toString()
    name = this@toProto.name.value
    createdAt = this@toProto.createdAt.toTimestamp()
}

internal fun FactStoreProto.FactInput.toDomain(): Fact = Fact(
    id = if (hasId()) UUID.fromString(id).toFactId() else FactId.generate(),
    type = type.toFactType(),
    subject = Subject(subject),
    appendedAt = if (hasAppendedAt()) appendedAt.toInstant() else Instant.now(),
    payload = payload.toDomain(),
    metadata = metadataMap,
    tags = tagsMap.entries.associate { (k, v) -> k.toTagKey() to v.toTagValue() }
)

internal fun FactStoreProto.FactPayload.toDomain(): FactPayload = FactPayload(
    data = data.toByteArray(),
    format = if (hasFormat()) PayloadFormat(format) else null,
    schema = if (hasSchemaRef()) PayloadSchemaRef(schemaRef) else null
)

internal fun FactStoreProto.AppendCondition.toDomain(): AppendCondition = when (kindCase) {
    FactStoreProto.AppendCondition.KindCase.EXPECTED_LAST_FACT -> AppendCondition.ExpectedLastFact(
        subject = Subject(expectedLastFact.subject),
        expectedLastFactId = if (expectedLastFact.hasExpectedLastFactId())
            UUID.fromString(expectedLastFact.expectedLastFactId).toFactId()
        else null
    )

    FactStoreProto.AppendCondition.KindCase.EXPECTED_MULTI_SUBJECT_LAST_FACT -> AppendCondition.ExpectedMultiSubjectLastFact(
        expectations = expectedMultiSubjectLastFact.expectationsList.associate { exp ->
            Subject(exp.subject) to
                    if (exp.hasExpectedLastFactId()) UUID.fromString(exp.expectedLastFactId).toFactId()
                    else null
        }
    )

    FactStoreProto.AppendCondition.KindCase.TAG_QUERY_BASED -> AppendCondition.TagQueryBased(
        failIfEventsMatch = tagQueryBased.failIfEventsMatch.toDomain(),
        after = if (tagQueryBased.hasAfterFactId()) UUID.fromString(tagQueryBased.afterFactId).toFactId()
        else null
    )

    else -> AppendCondition.None
}

internal fun FactStoreProto.TagQuery.toDomain(): TagQuery = TagQuery(
    queryItems = itemsList.map { it.toDomain() }
)

internal fun FactStoreProto.TagQueryItem.toDomain(): TagQueryItem = when (kindCase) {
    FactStoreProto.TagQueryItem.KindCase.TAG_ONLY -> TagOnlyQueryItem(
        tags = tagOnly.tagsMap.entries.map { (k, v) -> k.toTagKey() to v.toTagValue() }
    )

    FactStoreProto.TagQueryItem.KindCase.TAG_TYPE -> TagTypeItem(
        types = tagType.typesList.map { it.toFactType() },
        tags = tagType.tagsMap.entries.map { (k, v) -> k.toTagKey() to v.toTagValue() }
    )

    else -> throw IllegalArgumentException("TagQueryItem has no kind set")
}
