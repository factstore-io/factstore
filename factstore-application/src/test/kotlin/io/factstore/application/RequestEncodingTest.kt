package io.factstore.application

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class RequestEncodingTest {

    private data class TwoFields(val first: String, val second: String)

    private fun encodingNamed(typeName: String) = object : RequestEncoding<TwoFields> {
        override val typeName = typeName
        override fun encode(request: TwoFields, encoder: RequestEncoder) {
            encoder.string(request.first)
            encoder.string(request.second)
        }
    }

    private val encoding = encodingNamed("TwoFields")

    @Test
    fun `gives the same request the same fingerprint`() {
        assertThat(encoding.fingerprint(TwoFields("a", "b"))).isEqualTo(encoding.fingerprint(TwoFields("a", "b")))
    }

    @Test
    fun `gives a request with another field value another fingerprint`() {
        assertThat(encoding.fingerprint(TwoFields("a", "b"))).isNotEqualTo(encoding.fingerprint(TwoFields("a", "c")))
    }

    @Test
    fun `gives a request of another type another fingerprint`() {
        assertThat(encoding.fingerprint(TwoFields("a", "b"))).isNotEqualTo(encodingNamed("Other").fingerprint(TwoFields("a", "b")))
    }

    @Test
    fun `keeps the fields apart`() {
        assertThat(encoding.fingerprint(TwoFields("ab", "c"))).isNotEqualTo(encoding.fingerprint(TwoFields("a", "bc")))
    }

    @Test
    fun `writes the fingerprint as a SHA-256 in lowercase hex`() {
        assertThat(encoding.fingerprint(TwoFields("a", "b")).value).matches("[0-9a-f]{64}")
    }
}
