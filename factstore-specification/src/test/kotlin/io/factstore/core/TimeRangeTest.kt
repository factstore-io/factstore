package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.time.Instant

class TimeRangeTest {

    @Test
    fun `should create TimeRange when start is before end`() {
        val start = Instant.parse("2024-01-01T00:00:00Z")
        val end = Instant.parse("2024-01-02T00:00:00Z")

        val range = TimeRange(start, end)

        assertThat(start).isEqualTo(range.start)
        assertThat(end).isEqualTo(range.end)
    }

    @Test
    fun `should throw when start equals end`() {
        val instant = Instant.parse("2024-01-01T00:00:00Z")

        assertThatThrownBy {
            TimeRange(instant, instant)
        }.isExactlyInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `should throw when start is after end`() {
        val start = Instant.parse("2024-01-02T00:00:00Z")
        val end = Instant.parse("2024-01-01T00:00:00Z")

        assertThatThrownBy {
            TimeRange(start, end)
        }.isExactlyInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `data class equality should work`() {
        val start = Instant.parse("2024-01-01T00:00:00Z")
        val end = Instant.parse("2024-01-02T00:00:00Z")

        val range1 = TimeRange(start, end)
        val range2 = TimeRange(start, end)

        assertThat(range1).isEqualTo(range2)
        assertThat(range1.hashCode()).isEqualTo(range2.hashCode())
    }

}