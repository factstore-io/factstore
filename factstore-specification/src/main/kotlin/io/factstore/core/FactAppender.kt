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
     * @param fact the fact to append
     * @throws FactStoreException if the fact violates store invariants or
     *         the request is invalid
     */
    suspend fun append(fact: Fact)

    /**
     * Appends multiple facts to the store atomically.
     *
     * Either all facts are appended successfully, or none are.
     *
     * @param facts the facts to append
     * @throws FactStoreException if any fact violates store invariants or
     *         the request is invalid
     */
    suspend fun append(facts: List<Fact>)

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
