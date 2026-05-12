package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class LimitTest {

    @Nested
    @DisplayName("Factory Method: Limit.of(Int)")
    inner class OfFactory {

        @ParameterizedTest
        @ValueSource(ints = [1, 100, Int.MAX_VALUE])
        fun `should create limit for positive integers`(input: Int) {
            val limit = Limit.of(input)
            assertThat(limit.value).isEqualTo(input)
        }

        @ParameterizedTest
        @ValueSource(ints = [0, -1, -100, Int.MIN_VALUE])
        fun `should throw IllegalArgumentException for non-positive integers`(input: Int) {
            assertThatThrownBy { Limit.of(input) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Limit must be positive")
        }
    }

    @Nested
    @DisplayName("Constant: Limit.None")
    inner class NoneConstant {

        @Test
        fun `should have a null value`() {
            val limit = Limit.None
            assertThat(limit.value).isNull()
        }

        @Test
        fun `should be equivalent to other None instances`() {
            // Testing value class equality semantics
            assertThat(Limit.None).isEqualTo(Limit.None)
        }
    }

    @Nested
    @DisplayName("Value Class Semantics")
    inner class Semantics {

        @Test
        fun `should satisfy equality based on underlying value`() {
            val limitA = Limit.of(10)
            val limitB = Limit.of(10)
            val limitC = Limit.of(20)

            assertThat(limitA).isEqualTo(limitB)
            assertThat(limitA).isNotEqualTo(limitC)
            assertThat(limitA).isNotEqualTo(Limit.None)
        }
    }
}
