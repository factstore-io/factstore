package io.factstore.core

/**
 * Appends facts to the FactStore.
 *
 * Implementations are responsible for persisting facts atomically and
 * enforcing all store invariants such as uniqueness, idempotency, and
 * conditional append semantics.
 *
 * Append operations may either complete successfully, return an explicit
 * append outcome, or fail by throwing a [FactStoreException] if the request
 * is invalid.
 *
 * @author Domenic Cassisi
 */
interface FactAppender {

    /**
     * Appends a single fact to the store.
     *
     * This convenience overload generates a fresh idempotency key on every call
     * and is therefore **not** idempotent across retries. Use [append] with an
     * explicit [AppendRequest] when idempotency or conditional appends are needed.
     *
     * @param fact the fact to append
     * @throws FactStoreException if the fact violates store invariants or
     *         the request is invalid
     */
    suspend fun append(storeName: StoreName, fact: FactInput): AppendResult

    /**
     * Appends multiple facts to the store atomically.
     *
     * Either all facts are appended successfully, or none are. Like the
     * single-fact overload, this generates a fresh idempotency key per call and
     * is not idempotent across retries.
     *
     * @param facts the facts to append
     * @throws FactStoreException if any fact violates store invariants or
     *         the request is invalid
     */
    suspend fun append(storeName: StoreName, facts: List<FactInput>): AppendResult

    /**
     * Appends facts using an explicit append request.
     *
     * This variant allows callers to specify idempotency keys and conditional
     * append rules and provides a structured [AppendResult] describing the
     * outcome of the operation.
     *
     * @param request the append request
     * @return the outcome of the append operation
     * @throws FactStoreException if the request is invalid or violates
     *         store invariants
     */
    suspend fun append(request: AppendRequest): AppendResult

}
