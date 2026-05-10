package io.factstore.cli.command

import io.factstore.cli.client.*
import jakarta.inject.Inject
import picocli.CommandLine.*
import java.util.*
import java.util.concurrent.Callable
import kotlin.text.Charsets.UTF_8

@Command(
    name = "append",
    description = ["Append a fact to a store"]
)
class AppendFactCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Parameters(index = "0")
    lateinit var storeName: String

    @Parameters(index = "1")
    lateinit var type: String

    @Parameters(index = "2", arity = "0..1")
    var payload: String? = null

    override fun call(): Int {
        // If payload is null, try to read from STDIN
        val rawData = payload ?: System.`in`.bufferedReader().readText()

        if (rawData.isBlank()) {
            System.err.println("❌ Error: No payload provided. Provide an argument or pipe data via STDIN.")
            return ExitCode.SOFTWARE
        }

        val request = AppendHttpRequest(
            facts = listOf(
                FactHttp(
                    type = type,
                    subject = "default",
                    payload = FactPayloadHttp(data = rawData.toByteArray(UTF_8)),
                )
            ),
            idempotencyKey = UUID.randomUUID()
        )

        try {
            client.appendFact(storeName, request)
            println("✅ Fact appended successfully.")
            return ExitCode.OK
        } catch (e: Exception) {
            System.err.println("❌ Failed to append: ${e.message}")
            return ExitCode.SOFTWARE
        }
    }

}
