package io.factstore.core

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

class StoreNameTest {

    @Test
    fun `create should reject names with invalid characters`(): Unit = runBlocking {
        listOf(
            "my store", // space
            "my.store", // dot
            "my/store", // slash
            "my@store", // at sign
            "my:store", // colon
            "my\$store", // dollar
            "Mÿ_store", // non-ASCII
            " ",
            "",
            "s a",
            "-",
            "-a",
            "_",
            "a-",
            "lk-",
        ).forEach { invalidName ->
            val exception = catchThrowable {
                runBlocking { StoreName(invalidName) }
            }

            assertThat(exception)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `create should accept alphanumeric and hyphen and underscore`(): Unit = runBlocking {
        val validNames = listOf(
            "my-store",
            "my_store",
            "MyStore123",
            "store-123",
            "store_456",
            "store123",
            "a",
            "A",
            "abc123_-def",
            "a".repeat(255)
        )

        validNames.forEach { validName ->
            assertThat(StoreName(validName)).matches { it.value == validName }

        }
    }

    @Test
    fun `create should reject names exceeding 255 characters`(): Unit = runBlocking {
        val longName = "a".repeat(256)

        val exception = catchThrowable {
            runBlocking { StoreName(longName) }
        }

        assertThat(exception)
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("255")
    }
}
