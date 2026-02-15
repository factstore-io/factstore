package org.factstore.server.http

import jakarta.ws.rs.core.Response
import jakarta.ws.rs.core.Response.Status.CONFLICT
import jakarta.ws.rs.core.Response.Status.NOT_FOUND
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider
import org.factstore.core.DuplicateFactIdException
import org.factstore.core.FactIdNotFoundException
import org.factstore.core.FactStoreException

@Provider
class FactStoreExceptionMapper : ExceptionMapper<FactStoreException> {

    override fun toResponse(exception: FactStoreException): Response {
        return when (exception) {
            is DuplicateFactIdException -> Response.status(CONFLICT).entity(exception.message).build()
            is FactIdNotFoundException -> Response.status(NOT_FOUND).entity(exception.message).build()
        }
    }
}
