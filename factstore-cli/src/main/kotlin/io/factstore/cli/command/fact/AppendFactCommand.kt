package io.factstore.cli.command.fact

import io.factstore.cli.config.FactStoreConfigResolver
import io.factstore.client.FactStoreClient
import io.factstore.client.model.AppendOutcome
import io.factstore.client.model.FactInput
import io.factstore.client.model.FactPayload
import jakarta.inject.Inject
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command
import picocli.CommandLine.Option
import picocli.CommandLine.Parameters
import picocli.CommandLine.Spec
import java.util.UUID
import java.util.concurrent.Callable

@Command(
    name = "append",
    description = ["Append a fact to a store"]
)
class AppendFactCommand : Callable<Int> {

    @Inject
    lateinit var client: FactStoreClient

    @Inject
    lateinit var configResolver: FactStoreConfigResolver

    @Spec
    lateinit var spec: CommandLine.Model.CommandSpec

    @Option(
        names = ["--store", "-s"],
        description = ["The name of the store to append to (env: FACTSTORE_STORE, config: store)"],
    )
    var storeName: String? = null

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

    override fun call(): Int = runBlocking {
        val storeName = configResolver.resolveStore(storeName)
        val resolvedPayload = resolvePayload() ?: return@runBlocking CommandLine.ExitCode.SOFTWARE
        val outcome = client.facts.append(
            storeName = storeName,
            facts = listOf(
                resolvedPayload.toFactInput()
            ),
            idempotencyKey = UUID.randomUUID().toString(),
            condition = null
        )
        when (outcome) {
            is AppendOutcome.Appended ->
                println("✅ Fact appended successfully. Id: ${outcome.factIds.single()}")
            AppendOutcome.AlreadyApplied ->
                println("ℹ️ Append already applied; nothing was added.")
        }
        CommandLine.ExitCode.OK
    }

    private fun String.toFactInput(): FactInput = FactInput(
        type = type,
        subject = subject,
        payload = FactPayload(data = toByteArray(Charsets.UTF_8)),
    )

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

}