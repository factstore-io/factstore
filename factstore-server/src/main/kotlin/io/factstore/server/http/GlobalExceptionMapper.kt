package io.factstore.server.http

import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider

@Provider
class GlobalExceptionMapper : ExceptionMapper<RuntimeException> {

    override fun toResponse(exception: RuntimeException): Response =
        apiErrorResponse(
            status = Response.Status.INTERNAL_SERVER_ERROR,
            reason = Reason.InternalError,
            message = "An unexpected error occurred",
        )
}
