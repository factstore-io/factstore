package io.factstore.client.exceptions

sealed class FactStoreException(message: String, cause: Throwable? = null) : Exception(message, cause)

// ─── Business / domain errors ───────────────────────────────────────────────

class StoreNotFoundException(val storeName: String) :
    FactStoreException("Store '$storeName' not found")

class StoreNameAlreadyExistsException(val storeName: String) :
    FactStoreException("A store named '$storeName' already exists")

class FactNotFoundException(val factId: String) :
    FactStoreException("Fact '$factId' not found")

class AppendConditionViolatedException :
    FactStoreException("Append condition was violated")

class DuplicateFactIdsException(val factIds: List<String>) :
    FactStoreException("Duplicate fact IDs: ${factIds.joinToString()}")

// ─── Transport / connectivity errors ────────────────────────────────────────

sealed class FactStoreConnectivityException(message: String, cause: Throwable?) :
    FactStoreException(message, cause)

/** The server could not be reached (gRPC UNAVAILABLE): down, refused, DNS failure, dropped connection. */
class FactStoreUnavailableException(cause: Throwable?) :
    FactStoreConnectivityException("FactStore server is unavailable", cause)

/** The call exceeded its configured deadline (gRPC DEADLINE_EXCEEDED). */
class FactStoreTimeoutException(cause: Throwable?) :
    FactStoreConnectivityException("Request to FactStore server timed out", cause)

/** Any other RPC-level failure (UNKNOWN, INTERNAL, UNIMPLEMENTED, CANCELLED, …). */
class FactStoreRpcException(val code: String, val description: String?, cause: Throwable?) :
    FactStoreException("RPC failed [$code]${description?.let { ": $it" } ?: ""}", cause)
