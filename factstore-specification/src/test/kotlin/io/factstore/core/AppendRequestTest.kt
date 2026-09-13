package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow

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

    @Test
    fun `an append holds at most 512 facts and 1 MiB`() {
        assertThat(AppendRequest.MAX_FACTS).isEqualTo(512)
        assertThat(AppendRequest.MAX_SIZE).isEqualTo(1024 * 1024)
    }

    @Test
    fun `rejects an append without facts`() {
        assertThat(catchThrowable { request(emptyList()) })
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `accepts the maximum number of facts`() {
        assertDoesNotThrow { request(List(AppendRequest.MAX_FACTS) { fact() }) }
    }

    @Test
    fun `rejects one fact beyond the maximum number`() {
        assertThat(catchThrowable { request(List(AppendRequest.MAX_FACTS + 1) { fact() }) })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(AppendRequest.MAX_FACTS.toString())
    }

    @Test
    fun `accepts facts totalling exactly the maximum size`() {
        // The type "T" and subject "s" add two bytes to each payload, so each fact is 64 KiB.
        val facts = List(16) { fact(payloadSize = FactPayload.MAX_SIZE - 2) }

        assertThat(facts.sumOf { it.byteSize }).isEqualTo(AppendRequest.MAX_SIZE)
        assertDoesNotThrow { request(facts) }
    }

    @Test
    fun `rejects facts totalling one byte beyond the maximum size`() {
        val facts = List(15) { fact(payloadSize = FactPayload.MAX_SIZE - 2) } + fact(payloadSize = FactPayload.MAX_SIZE - 1)

        assertThat(facts.sumOf { it.byteSize }).isEqualTo(AppendRequest.MAX_SIZE + 1)
        assertThat(catchThrowable { request(facts) })
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(AppendRequest.MAX_SIZE.toString())
    }

    private fun fact(payloadSize: Int = 0) = FactInput(
        type = FactType("T"),
        subject = Subject("s"),
        payload = FactPayload(ByteArray(payloadSize)),
    )

    private fun request(facts: List<FactInput>) =
        AppendRequest(StoreName("test-store"), facts, IdempotencyKey())
}
