package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class AppendRequestTest {

    @Test
    fun testConstructsWithFactInputs() {
        val request = AppendRequest(
            storeName = StoreName("test-store"),
            facts = listOf(
                FactInput(
                    type = FactType("TEST_FACT_TYPE"),
                    subject = Subject("TEST_SUBJECT"),
                    payload = """DATA""".toFactPayload(),
                ),
                FactInput(
                    type = FactType("TEST_FACT_TYPE"),
                    subject = Subject("TEST_SUBJECT"),
                    payload = """MORE_DATA""".toFactPayload(),
                ),
            ),
            idempotencyKey = IdempotencyKey(),
            condition = AppendCondition.None,
        )

        assertThat(request.facts).hasSize(2)
        assertThat(request.condition).isEqualTo(AppendCondition.None)
    }
}
