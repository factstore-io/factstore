package io.factstore.core

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class Utf8SizeTest {

    @Test
    fun `counts one byte per ASCII character`() {
        assertThat("order/12345".utf8Size).isEqualTo(11)
    }

    @Test
    fun `counts the empty string as zero bytes`() {
        assertThat("".utf8Size).isEqualTo(0)
    }

    @Test
    fun `counts bytes rather than characters for non-ASCII text`() {
        assertThat("München".utf8Size).isEqualTo(8)
        assertThat("顧客".utf8Size).isEqualTo(6)
        assertThat("📦".utf8Size).isEqualTo(4)
    }
}
