package io.factstore.cli

import io.factstore.client.exceptions.AppendConditionViolatedException
import io.factstore.client.exceptions.ContinuationNotFoundException
import io.factstore.client.exceptions.FactNotFoundException
import io.factstore.client.exceptions.FactStoreException
import io.factstore.client.exceptions.FactStoreInvalidRequestException
import io.factstore.client.exceptions.FactStoreRpcException
import io.factstore.client.exceptions.FactStoreTimeoutException
import io.factstore.client.exceptions.FactStoreUnavailableException
import io.factstore.client.exceptions.StoreNameAlreadyExistsException
import io.factstore.client.exceptions.StoreNotFoundException

/**
 * What the user reads when a call to the server fails.
 *
 * [FactStoreException] is sealed, so this `when` is exhaustive: a new failure the client learns
 * to report cannot reach the terminal as a stack trace without this file being updated first.
 */
internal fun FactStoreException.toCliMessage(): String = when (this) {
    is StoreNotFoundException -> "Store not found: '$storeName'"
    is StoreNameAlreadyExistsException -> "A store named '$storeName' already exists"
    is FactNotFoundException -> "Fact not found: '$factId'"
    is ContinuationNotFoundException -> "Cannot continue after fact '$factId': the store holds no such fact"
    is AppendConditionViolatedException ->
        "Append condition violated: a concurrent write changed the store. Retry with an updated condition."
    is FactStoreInvalidRequestException -> "Invalid request: $reason"
    is FactStoreUnavailableException -> "Cannot reach the FactStore server. Is it running?"
    is FactStoreTimeoutException -> "The request timed out. The server may be overloaded or unreachable."
    is FactStoreRpcException -> "Server error [$code]${description?.let { ": $it" }.orEmpty()}"
}
