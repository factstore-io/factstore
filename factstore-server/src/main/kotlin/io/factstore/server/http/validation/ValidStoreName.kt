package io.factstore.server.http.validation

import io.factstore.core.StoreName
import jakarta.validation.Constraint
import jakarta.validation.Payload
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size
import kotlin.annotation.AnnotationTarget.FIELD
import kotlin.annotation.AnnotationTarget.VALUE_PARAMETER
import kotlin.reflect.KClass

@Target(FIELD, VALUE_PARAMETER)
@Retention(AnnotationRetention.RUNTIME)
@Constraint(validatedBy = [])
@Size(max = StoreName.MAX_LENGTH)
@Pattern(regexp = StoreName.REGEX_PATTERN)
annotation class ValidStoreName(
    val message: String = "Store name must be valid",
    val groups: Array<KClass<Any>> = [],
    val payload: Array<KClass<Payload>> = []
)
