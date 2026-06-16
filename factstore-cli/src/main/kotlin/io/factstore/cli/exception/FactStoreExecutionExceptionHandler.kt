package io.factstore.cli.exception

import io.factstore.client.exceptions.AppendConditionViolatedException
import io.factstore.client.exceptions.DuplicateFactIdsException
import io.factstore.client.exceptions.FactNotFoundException
import io.factstore.client.exceptions.StoreNameAlreadyExistsException
import io.factstore.client.exceptions.StoreNotFoundException
import jakarta.enterprise.context.ApplicationScoped
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
    is CliUsageException -> "❌ ${this.message}"
    is StoreNotFoundException -> "❌ Store not found: '${this.storeName}'"
    is StoreNameAlreadyExistsException -> "❌ A store named '${this.storeName}' already exists"
    is FactNotFoundException -> "❌ Fact not found: '${this.factId}'"
    is AppendConditionViolatedException -> "❌ Append condition violated: a concurrent write changed the store state. Retry with an updated condition."
    is DuplicateFactIdsException -> "❌ Duplicate fact IDs in request: ${this.factIds.joinToString()}"
    is UnknownHostException -> "❌ Could not resolve host: ${this.message}"
    else -> "❌ Unexpected error: ${this.message}"
}

