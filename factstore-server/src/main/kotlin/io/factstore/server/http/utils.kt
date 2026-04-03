package io.factstore.server.http

import io.factstore.core.FactStoreFinder
import io.factstore.core.FactStoreName
import jakarta.ws.rs.core.Response
import jakarta.ws.rs.ext.ExceptionMapper
import jakarta.ws.rs.ext.Provider


suspend fun FactStoreFinder.resolveStoreOrThrow(name: String) =
    findByName(FactStoreName(name)) ?: throw FactstoreNotFound("Fact store with name $name not found")


class FactstoreNotFound(message: String) : RuntimeException(message)

@Provider
class FactstoreNotFoundExceptionMapper : ExceptionMapper<FactstoreNotFound> {

    override fun toResponse(exception: FactstoreNotFound): Response {
        return Response.status(Response.Status.NOT_FOUND).entity(exception.message).build()
    }

}