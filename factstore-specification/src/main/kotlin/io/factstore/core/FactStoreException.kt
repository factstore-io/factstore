package io.factstore.core

/**
 * Base exception type for all FactStore-related errors.
 *
 * This exception represents failures that indicate unrecoverable errors within a FactStore operation.
 */
class FactStoreException(message: String) : RuntimeException(message)
