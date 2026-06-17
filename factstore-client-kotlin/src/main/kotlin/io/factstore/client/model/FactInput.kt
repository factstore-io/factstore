package io.factstore.client.model

data class FactInput(
    val type: String,
    val subject: String,
    val payload: FactPayload,
    val metadata: Map<String, String> = emptyMap(),
    val tags: Map<String, String> = emptyMap(),
)

class FactInputBuilder {
    var type: String = ""
    var subject: String = ""
    private var payload: FactPayload = FactPayload(ByteArray(0))
    val metadata: MutableMap<String, String> = mutableMapOf()
    val tags: MutableMap<String, String> = mutableMapOf()

    fun payload(data: ByteArray, format: String? = null, schemaRef: String? = null) {
        payload = FactPayload(data, format, schemaRef)
    }

    fun build(): FactInput = FactInput(
        type = type,
        subject = subject,
        payload = payload,
        metadata = metadata.toMap(),
        tags = tags.toMap(),
    )
}

class AppendFactsBuilder {
    private val facts = mutableListOf<FactInput>()

    fun fact(block: FactInputBuilder.() -> Unit) {
        facts.add(FactInputBuilder().apply(block).build())
    }

    internal fun build(): List<FactInput> = facts.toList()
}
