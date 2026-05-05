package io.factstore.cli.exception

import io.factstore.cli.client.FactStoreApiException
import jakarta.enterprise.context.ApplicationScoped
import jakarta.ws.rs.ProcessingException
import picocli.CommandLine
import picocli.CommandLine.ExitCode
import java.lang.Exception
import java.net.UnknownHostException

@ApplicationScoped
class FactStoreExecutionExceptionHandler : CommandLine.IExecutionExceptionHandler {

    override fun handleExecutionException(
        exception: Exception,
        commandLine: CommandLine,
        fullParseResult: CommandLine.ParseResult
    ): Int {
        val message = exception.toCliMessage()
        commandLine.err.println(message)
        return ExitCode.SOFTWARE
    }

}

private fun Exception.toCliMessage(): String = when (this) {
    is FactStoreApiException -> toCliMessage()
    is ProcessingException -> "❌ Network Error: ${this.cause?.message ?: this.message}"
    is UnknownHostException -> "❌ Could not resolve host: ${this.message}"
    else -> "❌ Unexpected Error: ${this.message}"
}

private fun FactStoreApiException.toCliMessage(): String {
    return if (apiError != null) {
        "Error from server (${apiError.reason}): ${apiError.message}"
    } else {
        "❌ Server Error: HTTP $httpStatus"
    }
}
