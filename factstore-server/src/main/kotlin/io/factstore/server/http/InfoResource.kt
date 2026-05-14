package io.factstore.server.http

import io.factstore.server.info.ServerInfo
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType.APPLICATION_JSON

@Path("/v1/info")
class InfoResource(
    private val serverInfo: ServerInfo
) {

    @GET
    @Produces(APPLICATION_JSON)
    fun getInfo(): ServerInfo = serverInfo
    
}
