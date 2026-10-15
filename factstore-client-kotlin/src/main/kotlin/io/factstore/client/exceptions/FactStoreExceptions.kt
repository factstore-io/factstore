package io.factstore.client.exceptions

sealed class FactStoreException(message: String, cause: Throwable? = null) : Exception(message, cause)

// ─── Business / domain errors ───────────────────────────────────────────────

class StoreNotFoundException(val storeName: String) :
    FactStoreException("Store '$storeName' not found")

class StoreNameAlreadyExistsException(val storeName: String) :
    FactStoreException("A store named '$storeName' already exists")

class FactNotFoundException(val factId: String) :
    FactStoreException("Fact '$factId' not found")

/** The store holds no fact with the id a stream was to continue after. */
class ContinuationNotFoundException(val factId: String) :
    FactStoreException("The fact '$factId' to continue after does not exist")

class AppendConditionViolatedException :
    FactStoreException("Append condition was violated")

// ─── Request errors ──────────────────────────────────────────────────────────

/** The server rejected the request as invalid (gRPC INVALID_ARGUMENT); retrying it unchanged fails again. */
class FactStoreInvalidRequestException(val reason: String, cause: Throwable?) :
    FactStoreException("Invalid request: $reason", cause)

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
