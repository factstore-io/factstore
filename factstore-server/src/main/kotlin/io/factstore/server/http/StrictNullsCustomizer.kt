package io.factstore.server.http

import com.fasterxml.jackson.annotation.JsonSetter
import com.fasterxml.jackson.annotation.Nulls
import com.fasterxml.jackson.databind.ObjectMapper
import io.quarkus.jackson.ObjectMapperCustomizer
import jakarta.inject.Singleton

/**
 * Rejects a JSON `null` inside a list or map of a request body.
 *
 * The request types declare their elements as non-null, which Jackson does not know: without
 * this, a `null` would reach code that relies on there being none.
 */
@Singleton
class StrictNullsCustomizer : ObjectMapperCustomizer {

    override fun customize(objectMapper: ObjectMapper) {
        objectMapper.setDefaultSetterInfo(JsonSetter.Value.forContentNulls(Nulls.FAIL))
    }
}
