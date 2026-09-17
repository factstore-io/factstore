package io.factstore.server.http

import jakarta.ws.rs.GET
import jakarta.ws.rs.Path

/** Fails on purpose, so tests can observe how unexpected failures are reported. */
@Path("/test/failures")
class FailingTestResource {

    @GET
    @Path("illegal-state")
    fun illegalState(): String = throw IllegalStateException(SECRET)

    /** An [IllegalArgumentException] that does not come from parsing client input. */
    @GET
    @Path("illegal-argument")
    fun illegalArgument(): String = throw IllegalArgumentException(SECRET)

    companion object {
        const val SECRET = "internal detail that must not leak"
    }
}
