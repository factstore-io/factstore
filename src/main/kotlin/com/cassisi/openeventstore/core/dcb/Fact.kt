package com.cassisi.openeventstore.core.dcb

import java.time.Instant
import java.util.*

data class Fact(
    val id: UUID,
    val type: String,
    val payload: String, // assume JSON
    val subjectType: String,
    val subjectId: String,
    val createdAt: Instant,
    val metadata: Map<String, String> = emptyMap()
)
