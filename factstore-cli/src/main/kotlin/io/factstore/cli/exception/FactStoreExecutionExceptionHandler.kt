package io.factstore.cli.exception

import io.factstore.client.exceptions.AppendConditionViolatedException
import io.factstore.client.exceptions.FactNotFoundException
import io.factstore.client.exceptions.FactStoreRpcException
import io.factstore.client.exceptions.FactStoreTimeoutException
import io.factstore.client.exceptions.FactStoreUnavailableException
import io.factstore.client.exceptions.StoreNameAlreadyExistsException
import io.factstore.client.exceptions.StoreNotFoundException
import jakarta.enterprise.context.ApplicationScoped
import picocli.CommandLine
import picocli.CommandLine.ExitCode
import java.lang.Exception

@ApplicationScoped
class FactStoreExecutionExceptionHandler : CommandLine.IExecutionExceptionHandler {

    override fun handleExecutionException(
        exception: Exception,
        commandLine: CommandLine,
        fullParseResult: CommandLine.ParseResult
    ): Int {
        commandLine.err.println(exception.toCliMessage())
        return when (exception) {
            is CliUsageException -> ExitCode.USAGE
            else -> ExitCode.SOFTWARE
        }
    }

}

private fun Exception.toCliMessage(): String = when (this) {
    is CliUsageException -> "❌ ${this.message}"
    is StoreNotFoundException -> "❌ Store not found: '${this.storeName}'"
    is StoreNameAlreadyExistsException -> "❌ A store named '${this.storeName}' already exists"
    is FactNotFoundException -> "❌ Fact not found: '${this.factId}'"
    is AppendConditionViolatedException -> "❌ Append condition violated: a concurrent write changed the store state. Retry with an updated condition."
    is FactStoreUnavailableException -> "❌ Cannot reach the FactStore server. Is it running, and is --url correct?"
    is FactStoreTimeoutException -> "❌ The request timed out. The server may be overloaded or unreachable."
    is FactStoreRpcException -> "❌ Server error [${this.code}]${this.description?.let { ": $it" } ?: ""}"
    else -> "❌ Unexpected error: ${this.message}"
}
