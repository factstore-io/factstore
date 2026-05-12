package io.factstore.server.http.converter

import io.factstore.core.ReadDirection
import io.factstore.core.StoreName
import jakarta.ws.rs.ext.ParamConverter
import kotlin.text.lowercase

object ReadDirectionConverter : ParamConverter<ReadDirection> {

    override fun fromString(value: String?): ReadDirection {
        val lowercaseValue = value?.lowercase()
        return when (lowercaseValue) {
            "backward" -> ReadDirection.Backward
            "forward", null -> ReadDirection.Forward
            else -> throw IllegalArgumentException("Invalid direction '$this'. Must be 'forward' or 'backward'.")
        }

    }

    override fun toString(value: ReadDirection?): String? {
        return value?.name
    }
}
