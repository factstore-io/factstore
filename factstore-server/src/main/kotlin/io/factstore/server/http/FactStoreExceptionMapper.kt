package io.factstore.server.http

import io.factstore.core.DuplicateFactIdException
import io.factstore.core.FactStoreException
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.CONFLICT
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider

@Provider
class FactStoreExceptionMapper : ExceptionMapper<FactStoreException> {

    override fun toResponse(exception: FactStoreException): Response {
        return when (exception) {
            is DuplicateFactIdException -> Response.status(CONFLICT).entity(exception.message).build()
        }
    }
}
