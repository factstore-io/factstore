package io.factstore.cli.command

import com.github.ajalt.clikt.command.SuspendingNoOpCliktCommand
import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.versionOption
import io.factstore.cli.command.fact.FactCommand
import io.factstore.cli.command.store.StoreCommand
import jakarta.enterprise.context.Dependent
import org.eclipse.microprofile.config.inject.ConfigProperty

/** The root command: it only groups the others and reports the version. */
@Dependent
class FactStoreCommand(
    @ConfigProperty(name = "quarkus.application.version") version: String,
    storeCommand: StoreCommand,
    factCommand: FactCommand,
) : SuspendingNoOpCliktCommand(name = "factstore") {

    init {
        versionOption(version)
        subcommands(storeCommand, factCommand)
    }

    override fun help(context: Context) = "FactStore command line interface"

    override fun aliases() = mapOf(
        "stores" to listOf("store"),
        "facts" to listOf("fact"),
    )

}
