package org.factstore.core

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Instant

class AppendRequestTest {

    @Test
    fun testUniqueFactId() {

        val fact1 = Fact(
            id = FactId.generate(),
            subjectRef = SubjectRef(
                type = "TEST_TYPE",
                id = "TEST_ID",
            ),
            type = FactType("TEST_FACT_TYPE"),
            payload = """DATA""".toFactPayload(),
            createdAt = Instant.now(),
            tags = emptyMap()
        )

        val fact2 = fact1.copy() // fact2 shares the same factId

        assertThatThrownBy {
            AppendRequest(
                facts = listOf(fact1, fact2),
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        }

        val fact3 = fact1.copy(id = FactId.generate())
        AppendRequest(
            facts = listOf(fact1, fact3), // no duplicated fact id
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.None
        )
    }
}
