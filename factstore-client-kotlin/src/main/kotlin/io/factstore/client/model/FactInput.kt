package io.factstore.client.model

import io.factstore.client.FactStoreDsl

data class FactInput(
    val type: String,
    val subject: String,
    val payload: FactPayload,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
)

/** Builds a [FactInput]; [type] and [subject] must be set, while the payload may stay empty. */
@FactStoreDsl
class FactInputBuilder {
    var type: String? = null
    var subject: String? = null
    private var payload: FactPayload = FactPayload(ByteArray(0))
    val metadata: MutableMap<String, String> = mutableMapOf()
    val tags: MutableMap<String, String> = mutableMapOf()

    fun payload(data: ByteArray) {
        payload = FactPayload(data)
    }

    /** @throws IllegalArgumentException if [type] or [subject] was not set */
    fun build(): FactInput = FactInput(
        type = requireNotNull(type) { "A fact needs a type." },
        subject = requireNotNull(subject) { "A fact needs a subject." },
        payload = payload,
        metadata = metadata.toMap(),
        tags = tags.toMap(),
    )
}

@FactStoreDsl
class AppendFactsBuilder {
    private val facts = mutableListOf<FactInput>()

    fun fact(block: FactInputBuilder.() -> Unit) {
        facts.add(FactInputBuilder().apply(block).build())
    }

    internal fun build(): List<FactInput> = facts.toList()
}
