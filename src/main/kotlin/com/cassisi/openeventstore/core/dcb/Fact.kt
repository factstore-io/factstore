package com.cassisi.openeventstore.core.dcb

import java.time.Instant
import java.util.*

data class Fact(
    val id: UUID,
    val type: String,
    val payload: String, // assume JSON
    val data: List<PayloadEntry>? = emptyList(),
    val subject: Subject,
    val createdAt: Instant,
    val metadata: Map<String, String> = emptyMap()
)

data class Subject(
    val type: String,
    val id: String
)

sealed interface PathElement {
    @JvmInline value class Key(val name: String): PathElement
    @JvmInline value class Index(val pos: Long) : PathElement
}

data class PayloadEntry(
    val path: List<PathElement>,
    val value: Any
)