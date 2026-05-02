package io.factstore.server.http

import io.factstore.core.FactStoreException
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider

@Provider
class FactStoreExceptionMapper : ExceptionMapper<FactStoreException> {

    override fun toResponse(exception: FactStoreException): Response =
        Response.serverError().build()

}
