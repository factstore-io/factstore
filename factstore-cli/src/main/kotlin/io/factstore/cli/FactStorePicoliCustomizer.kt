package io.factstore.cli
import io.factstore.cli.exception.FactStoreExecutionExceptionHandler
import io.quarkus.picocli.runtime.PicocliCommandLineFactory
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import picocli.CommandLine


@ApplicationScoped
class FactStorePicoliCustomizer(
    private val exceptionHandler: FactStoreExecutionExceptionHandler
)  {

    @Produces
    fun customCommandLine(factory: PicocliCommandLineFactory): CommandLine =
        factory
            .create()
            .setExecutionExceptionHandler(exceptionHandler)
            .setCaseInsensitiveEnumValuesAllowed(true)

}
