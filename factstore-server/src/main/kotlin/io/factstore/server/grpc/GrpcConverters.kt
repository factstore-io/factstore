package io.factstore.server.grpc

import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import io.factstore.core.*
import io.smallrye.mutiny.Multi
import io.smallrye.mutiny.Uni
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.future
import kotlinx.coroutines.jdk9.asPublisher
import java.time.Instant
import java.util.*
import io.factstore.core.ReadDirection as CoreReadDirection


/**
 * Converts a suspending function into a Mutiny [Uni].
 *
 * This bridge is necessary because Quarkus gRPC generates service stubs based on Mutiny
 * ([Uni] and [Multi]) rather than Kotlin coroutines. Until Quarkus provides native support
 * for grpc-kotlin coroutine stubs, service implementations must return Mutiny types while
 * still allowing business logic to be written in idiomatic Kotlin with coroutines.
 *
 * Internally uses [future] from `kotlinx-coroutines-jdk9` to produce a [java.util.concurrent.CompletableFuture],
 * which Mutiny natively understands via [Uni.createFrom().completionStage()]. Both successful
 * results and exceptions are propagated correctly.
 *
 * @param block The suspending function to execute.
 * @return A [Uni] that emits the result of [block], or fails if [block] throws.
 */
internal fun <T> CoroutineScope.toUni(block: suspend () -> T): Uni<T> =
    Uni.createFrom().completionStage(
        future { block() }
    )

/**
 * Converts a suspending function returning a [Flow] into a Mutiny [Multi].
 *
 * This bridge is necessary because Quarkus gRPC generates service stubs based on Mutiny
 * ([Uni] and [Multi]) rather than Kotlin coroutines. Until Quarkus provides native support
 * for grpc-kotlin coroutine stubs, server-streaming methods must return [Multi] while still
 * allowing streaming logic to be written using idiomatic Kotlin [Flow].
 *
 * Internally uses [asPublisher] from `kotlinx-coroutines-jdk9` to convert the [Flow] into
 * a [java.util.concurrent.Flow.Publisher], which Mutiny natively understands via
 * [Multi.createFrom().publisher()]. The calling [CoroutineScope]'s coroutineContext is
 * passed to [asPublisher] to ensure correct dispatcher and cancellation propagation.
 *
 * @param block The suspending function returning a [Flow] of items to stream.
 * @return A [Multi] that emits the items produced by the [Flow], or fails if the [Flow] throws.
 */
internal fun <T : Any> CoroutineScope.toMulti(
    block: suspend () -> Flow<T>
): Multi<T> {
    val publisher = flow { emitAll(block()) }.asPublisher(this.coroutineContext)
    return Multi.createFrom().publisher(publisher)
}


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
