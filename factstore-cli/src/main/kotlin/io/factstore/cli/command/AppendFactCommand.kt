package io.factstore.cli.command

import io.factstore.cli.client.*
import jakarta.inject.Inject
import picocli.CommandLine.*
import picocli.CommandLine.Model.CommandSpec
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

    @Spec
    lateinit var spec: CommandSpec

    @Option(
        names = ["--store", "-s"],
        description = ["The name of the store to append to"],
        required = true,
    )
    lateinit var storeName: String

    @Option(
        names = ["--type", "-t"],
        description = ["The type of the fact (e.g. ORDER_PLACED, USER_CREATED)"],
        required = true,
    )
    lateinit var type: String

    @Option(
        names = ["--subject"],
        description = ["The subject the fact is associated with (e.g. order/12345)"],
        required = true,
    )
    lateinit var subject: String

    @Option(
        names = ["--tag"],
        description = ["Tag in key=value format (e.g. region=eu). Repeatable."],
    )
    var tags: Map<String, String> = emptyMap()

    @Parameters(
        index = "0",
        arity = "0..1",
        paramLabel = "<payload>",
        description = ["Payload of the fact. Reads from stdin if not provided."],
    )
    var payload: String? = null

    override fun call(): Int {
        val resolvedPayload = resolvePayload() ?: return ExitCode.SOFTWARE
        client.appendFact(storeName, resolvedPayload.toAppendRequest())
        println("✅ Fact appended successfully.")
        return ExitCode.OK
    }

    private fun resolvePayload(): String? {
        val raw = payload ?: System.`in`.bufferedReader().readText()
        if (raw.isBlank()) {
            spec.commandLine().err.println(
                "❌ No payload provided. Pass a JSON argument or pipe data via stdin."
            )
            return null
        }
        return raw
    }

    private fun String.toAppendRequest(): AppendHttpRequest =
        AppendHttpRequest(
            facts = listOf(
                FactHttp(
                    type = type,
                    subject = subject,
                    payload = FactPayloadHttp(data = toByteArray(UTF_8)),
                    tags = tags,
                )
            ),
            idempotencyKey = UUID.randomUUID(),
        )
}
