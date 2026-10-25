package io.factstore.cli.output

import io.factstore.client.model.Fact
import io.factstore.client.model.StoreInfo
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import java.time.Instant
import java.util.Base64

/*
 * The JSON shapes facts and stores are written in. They belong to the command line rather than to
 * the client library: the output format is a promise of the CLI, and the client's model can
 * change without breaking it.
 */

@Serializable
internal class FactJson(
    val id: String,
    val type: String,
    val subject: String,
    @Serializable(with = InstantSerializer::class)
    val appendedAt: Instant,
    val payload: PayloadJson,
    val metadata: Map<String, String>,
    val tags: Map<String, String>,
)

@Serializable
internal class PayloadJson(
    @Serializable(with = ByteArrayAsBase64Serializer::class)
    val data: ByteArray,
)

@Serializable
internal class StoreJson(
    val id: String,
    val name: String,
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant,
)

internal fun Fact.toJson() = FactJson(id, type, subject, appendedAt, PayloadJson(payload.data), metadata, tags)

internal fun StoreInfo.toJson() = StoreJson(id, name, createdAt)

private object InstantSerializer : KSerializer<Instant> {
    override val descriptor = PrimitiveSerialDescriptor("Instant", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: Instant) = encoder.encodeString(value.toString())
    override fun deserialize(decoder: Decoder): Instant = Instant.parse(decoder.decodeString())
}

private object ByteArrayAsBase64Serializer : KSerializer<ByteArray> {
    override val descriptor = PrimitiveSerialDescriptor("ByteArrayAsBase64", PrimitiveKind.STRING)
    override fun serialize(encoder: Encoder, value: ByteArray) = encoder.encodeString(Base64.getEncoder().encodeToString(value))
    override fun deserialize(decoder: Decoder): ByteArray = Base64.getDecoder().decode(decoder.decodeString())
}
