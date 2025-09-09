package com.cassisi.openeventstore.core.dcb

import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import java.time.Instant
import java.util.*

@Path("/test")
class FactStoreController(
    private val db: FactStore,
) {

    @POST
    suspend fun test() {
        db.append(
            Fact(
                id = UUID.randomUUID(),
                type = "TEST_TYPE",
                payload = """ { "test": 123 } """,
                createdAt = Instant.now(),
                subjectType = "TEST",
                subjectId = "123"
            )
        )

    }

}