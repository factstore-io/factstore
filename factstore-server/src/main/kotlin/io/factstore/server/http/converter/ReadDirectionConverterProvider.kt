package io.factstore.server.http.converter

import io.factstore.core.ReadDirection
import jakarta.ws.rs.ext.ParamConverter
import jakarta.ws.rs.ext.ParamConverterProvider
import jakarta.ws.rs.ext.Provider
import java.lang.reflect.Type

@Provider
class ReadDirectionConverterProvider : ParamConverterProvider {

    override fun <T> getConverter(
        rawType: Class<T>,
        genericType: Type,
        annotations: Array<out Annotation>
    ): ParamConverter<T>? {
        if (rawType == ReadDirection::class.java) {
            @Suppress("UNCHECKED_CAST")
            return ReadDirectionConverter as ParamConverter<T>
        }
        return null
    }
}
