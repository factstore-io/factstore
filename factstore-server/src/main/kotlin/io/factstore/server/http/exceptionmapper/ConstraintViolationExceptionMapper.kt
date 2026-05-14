package io.factstore.server.http.exceptionmapper

import io.factstore.server.http.Reason
import io.factstore.server.http.apiErrorResponse
import jakarta.validation.ConstraintViolationException
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider

@Provider
class ConstraintViolationExceptionMapper : ExceptionMapper<ConstraintViolationException> {

    override fun toResponse(exception: ConstraintViolationException): Response {
        return apiErrorResponse(
            status = Response.Status.BAD_REQUEST,
            reason = Reason.InvalidInput,
            message = "Validation failed: ${exception.constraintViolations.first().message}"
        )
    }
}
