package io.factstore.core

import java.util.UUID

/**
 * Factory interface for creating, retrieving, and deleting logical fact stores.
 *
 * Each fact store is uniquely identified by a user-facing name (alphanumeric, URL-safe).
 * Internally, a UUID may be used for identification and persistence.
 *
 * This interface provides atomic operations for managing the fact store lifecycle:
 * - **Create**: Provision a new fact store with a unique name
 * - **Get/Retrieve**: Access an existing fact store by name
 * - **Delete**: Remove a fact store and all its associated facts
 *
 * ## Concurrency
 *
 * All operations are thread-safe and handle concurrent requests correctly.
 * Create and delete operations are atomic with respect to name uniqueness.
 *
 * ## Name Validation
 *
 * Fact store names must be URL-safe and consist only of:
 * - Alphanumeric characters (a-z, A-Z, 0-9)
 * - Hyphens (-)
 * - Underscores (_)
 *
 * Invalid names will result in [InvalidFactStoreNameException].
 *
 * ## Lifecycle
 *
 * When a fact store is deleted, all facts belonging to it are cascade-deleted.
 * The name becomes available for reuse.
 *
 * @author Domenic Cassisi
 */
interface FactStoreFactory {

    /**
     * Creates a new fact store with the given name.
     *
     * The operation is atomic: either the fact store is created with a unique ID and
     * current timestamp, or an exception is thrown.
     *
     * @param name the unique identifier for the fact store. Must be URL-safe.
     * @return metadata describing the created fact store
     * @throws InvalidFactStoreNameException if the name is not URL-safe
     * @throws FactStoreAlreadyExistsException if a fact store with this name already exists
     */
    suspend fun create(name: String): FactStoreMetadata

    /**
     * Retrieves an existing fact store by name.
     *
     * @param name the name of the fact store to retrieve
     * @return the fact store instance
     * @throws InvalidFactStoreNameException if the name is not URL-safe
     * @throws FactStoreNotFoundException if no fact store with this name exists
     */
    suspend fun get(name: String): FactStore

    /**
     * Retrieves metadata about an existing fact store without loading the entire fact store.
     *
     * @param name the name of the fact store
     * @return metadata describing the fact store
     * @throws InvalidFactStoreNameException if the name is not URL-safe
     * @throws FactStoreNotFoundException if no fact store with this name exists
     */
    suspend fun getMetadata(name: String): FactStoreMetadata

    /**
     * Checks if a fact store exists.
     *
     * @param name the name of the fact store to check
     * @return true if a fact store with this name exists, false otherwise
     * @throws InvalidFactStoreNameException if the name is not URL-safe
     */
    suspend fun exists(name: String): Boolean

    /**
     * Deletes a fact store and all facts it contains.
     *
     * After deletion, the name is available for reuse. This operation is atomic.
     *
     * @param name the name of the fact store to delete
     * @throws InvalidFactStoreNameException if the name is not URL-safe
     * @throws FactStoreNotFoundException if no fact store with this name exists
     */
    suspend fun delete(name: String)

    /**
     * Lists all fact stores managed by this factory.
     *
     * @return a list of metadata for all fact stores
     */
    suspend fun listAll(): List<FactStoreMetadata>
}

/**
 * Metadata about a fact store.
 *
 * @property name the user-facing name of the fact store
 * @property id the unique UUID identifier assigned to the fact store
 * @property createdAt the timestamp when the fact store was created
 *
 * @author Domenic Cassisi
 */
data class FactStoreMetadata(
    val name: String,
    val id: UUID,
    val createdAt: Long, // Unix timestamp in milliseconds
)

