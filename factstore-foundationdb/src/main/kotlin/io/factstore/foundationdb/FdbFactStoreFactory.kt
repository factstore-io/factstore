package io.factstore.foundationdb

import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.*
import kotlinx.coroutines.future.await
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import java.util.*
import java.util.concurrent.CompletableFuture

/**
 * Metadata storage structure for fact stores in FoundationDB.
 *
 * @property id the unique UUID identifier
 * @property name the user-facing name
 * @property createdAt Unix timestamp in milliseconds
 */
@Serializable
private data class StoredFactStoreMetadata(
    val id: String, // UUID as string for serialization
    val name: String,
    val createdAt: Long,
)

/**
 * FoundationDB implementation of [FactStoreFactory].
 *
 * This factory manages fact store lifecycle using a dedicated metadata keyspace.
 * All operations are atomic and thread-safe.
 *
 * Metadata is stored in `/fact-store-metadata/` with the following structure:
 * - `/fact-store-metadata/by-name/{name}` → serialized StoredFactStoreMetadata
 * - `/fact-store-metadata/by-id/{id}` → {name}
 *
 * @property database the FoundationDB database instance
 *
 * @author Domenic Cassisi
 */
class FdbFactStoreFactory(
    foundationDbContext: FoundationDBFactStoreContext
) : FactStoreFactory {

    private val database = foundationDbContext.database

    companion object {
        const val FACT_STORE_METADATA = "fact-store-metadata"
        const val BY_NAME = "by-name"
        const val BY_ID = "by-id"

        /**
         * Creates a new [FdbFactStoreFactory] instance and initializes the metadata directory.
         */
        suspend fun create(foundationDbContext: FoundationDBFactStoreContext): FdbFactStoreFactory {
            // Ensure metadata directory exists
            val database = foundationDbContext.database
            database.runAsync { tr ->
                DirectoryLayer.getDefault()
                    .createOrOpen(tr, listOf(FACT_STORE_METADATA))
            }.await()

            return FdbFactStoreFactory(foundationDbContext)
        }

        /**
         * Validates that a fact store name is URL-safe.
         *
         * Valid names consist only of alphanumeric characters, hyphens, and underscores.
         *
         * @throws InvalidFactStoreNameException if the name is invalid
         */
        fun validateName(name: String) {
            when {
                name.isEmpty() -> throw InvalidFactStoreNameException(
                    name,
                    "name cannot be empty"
                )
                name.length > 255 -> throw InvalidFactStoreNameException(
                    name,
                    "name cannot exceed 255 characters"
                )
                !name.matches(Regex("^[a-zA-Z0-9_-]+$")) -> throw InvalidFactStoreNameException(
                    name,
                    "name must contain only alphanumeric characters, hyphens, and underscores"
                )
            }
        }
    }

    override suspend fun create(name: String): FactStoreMetadata {
        validateName(name)

        val id = UUID.randomUUID()
        val createdAt = System.currentTimeMillis()
        val metadata = FactStoreMetadata(name, id, createdAt)
        val stored = StoredFactStoreMetadata(id.toString(), name, createdAt)

        database.runAsync { tr ->
            // Get subspaces
            DirectoryLayer.getDefault()
                .open(tr, listOf(FACT_STORE_METADATA))
                .thenCompose { metaRoot ->
                    val byName = metaRoot.subspace(Tuple.from(BY_NAME))
                    val byId = metaRoot.subspace(Tuple.from(BY_ID))

                    // Check if name already exists
                    val nameKey = byName.pack(Tuple.from(name))
                    tr.get(nameKey).thenCompose { existing ->
                        if (existing != null) {
                            CompletableFuture.failedFuture(FactStoreAlreadyExistsException(name))
                        } else {
                            // Store metadata
                            val metadataBytes = Avro.encodeToByteArray(stored)
                            tr.set(nameKey, metadataBytes)
                            tr.set(byId.pack(Tuple.from(id.toString())), name.toByteArray())

                            // Create the fact store directory structure
                            DirectoryLayer.getDefault()
                                .createOrOpen(tr, listOf(FACT_STORE, name))
                        }
                    }
                }
        }.await()

        return metadata
    }

    override suspend fun get(name: String): FactStore {
        validateName(name)
        getMetadata(name) // Verify it exists

        val context = FdbFactStoreContext.create(database, name)
        val fdbFactStore = FdbFactStore(database, context)
        return FactStore(
            factAppender = FdbFactAppender(fdbFactStore),
            factFinder = FdbFactFinder(fdbFactStore),
            factStreamer = FdbFactStreamer(fdbFactStore),
        )
    }

    override suspend fun getMetadata(name: String): FactStoreMetadata {
        validateName(name)

        val metadata = database.runAsync { tr ->
            DirectoryLayer.getDefault()
                .open(tr, listOf(FACT_STORE_METADATA))
                .thenCompose { metaRoot ->
                    val byName = metaRoot.subspace(Tuple.from(BY_NAME))
                    val nameKey = byName.pack(Tuple.from(name))

                    tr.get(nameKey).thenApply { value ->
                        if (value == null) {
                            throw FactStoreNotFoundException(name)
                        }

                        val stored = Avro.decodeFromByteArray<StoredFactStoreMetadata>(value)
                        FactStoreMetadata(
                            name = stored.name,
                            id = UUID.fromString(stored.id),
                            createdAt = stored.createdAt
                        )
                    }
                }
        }.await()

        return metadata
    }

    override suspend fun exists(name: String): Boolean {
        validateName(name)

        return database.runAsync { tr ->
            DirectoryLayer.getDefault()
                .open(tr, listOf(FACT_STORE_METADATA))
                .thenCompose { metaRoot ->
                    val byName = metaRoot.subspace(Tuple.from(BY_NAME))
                    val nameKey = byName.pack(Tuple.from(name))

                    tr.get(nameKey).thenApply { value ->
                        value != null
                    }
                }
        }.await()
    }

    override suspend fun delete(name: String) {
        validateName(name)

        database.runAsync { tr ->
            DirectoryLayer.getDefault()
                .open(tr, listOf(FACT_STORE_METADATA))
                .thenCompose { metaRoot ->
                    val byName = metaRoot.subspace(Tuple.from(BY_NAME))
                    val byId = metaRoot.subspace(Tuple.from(BY_ID))

                    // Get metadata
                    val nameKey = byName.pack(Tuple.from(name))
                    tr.get(nameKey).thenCompose { value ->
                        if (value == null) {
                            CompletableFuture.failedFuture(FactStoreNotFoundException(name))
                        } else {
                            val stored = Avro.decodeFromByteArray<StoredFactStoreMetadata>(value)

                            // Delete metadata
                            tr.clear(nameKey)
                            tr.clear(byId.pack(Tuple.from(stored.id)))

                            // Delete the fact store directory and all its contents
                            DirectoryLayer.getDefault()
                                .remove(tr, listOf(FACT_STORE, name))
                        }
                    }
                }
        }.await()
    }

    override suspend fun listAll(): List<FactStoreMetadata> {
        return database.runAsync { tr ->
            DirectoryLayer.getDefault()
                .open(tr, listOf(FACT_STORE_METADATA))
                .thenCompose { metaRoot ->
                    val byName = metaRoot.subspace(Tuple.from(BY_NAME))

                    tr.getRange(byName.range()).asList().thenApply { kvList ->
                        val results = mutableListOf<FactStoreMetadata>()

                        // Scan all entries in the by-name subspace
                        kvList.forEach { kv ->
                            try {
                                val stored = Avro.decodeFromByteArray<StoredFactStoreMetadata>(kv.value)
                                results.add(
                                    FactStoreMetadata(
                                        name = stored.name,
                                        id = UUID.fromString(stored.id),
                                        createdAt = stored.createdAt
                                    )
                                )
                            } catch (@Suppress("UNUSED_VARIABLE") e: Exception) {
                                // Skip corrupted entries
                            }
                        }

                        results
                    }
                }
        }.await()
    }
}


