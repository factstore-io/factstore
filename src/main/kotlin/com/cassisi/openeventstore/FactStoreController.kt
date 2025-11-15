package com.cassisi.openeventstore

import com.cassisi.openeventstore.core.*
import jakarta.ws.rs.GET
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.core.Response
import java.time.Instant

@Path("/test")
class FactStoreController(
    private val db: FactStore,
) {

    @POST
    suspend fun test() {
        val id = FactId.generate()
        db.append(
            facts = listOf(
                element = Fact(
                    id = id,
                    type = "TEST_TYPE",
                    payload = """ { "test": 123 } """.toByteArray(),
                    createdAt = Instant.now(),
                    subject = Subject(
                        type = "TEST",
                        id = "$id"
                    ),
                    tags = mapOf("TEST" to "$id")
                )
            ),
            condition = TagQueryBasedAppendCondition(
                failIfEventsMatch = TagQuery(
                    queryItems = listOf(
                        TagTypeItem(
                            types = listOf("TEST_TYPE"),
                            tags = listOf("TEST" to "$id")
                        )
                    )
                ),
                after = null
            )
        )

    }

    val query = TagQuery(
        listOf(
            TagTypeItem(
                types = listOf("USER_CREATED"),
                tags = listOf("role" to "custom")
            )
        )
    )

    @GET
    suspend fun testGet(): Response {
        val result = db.findByTagQuery(query)
        return Response.ok(result).build()
    }

}