package io.factstore.cli.command

import io.quarkus.picocli.runtime.annotations.TopCommand
import kotlinx.coroutines.Runnable
import picocli.CommandLine.Command
import picocli.CommandLine.Model.CommandSpec
import picocli.CommandLine.Option
import picocli.CommandLine.ScopeType.INHERIT
import picocli.CommandLine.Spec

@TopCommand
@Command(
    name = "factstore",
    mixinStandardHelpOptions = true,
    version = ["0.1.0"],
    description = ["FactStore Command Line Interface"],
    subcommands = [
        StoreCommand::class,
        FactCommand::class,
        FindCommand::class,
    ]
)
class FactStoreCommand : Runnable {

    @Option(
        names = ["--url"],
        scope = INHERIT,
        description = ["Server URL"]
    )
    var url: String? = null

    @Spec
    lateinit var spec: CommandSpec

    override fun run() {
        // print help menu
        spec.commandLine().usage(System.err)
    }

}
