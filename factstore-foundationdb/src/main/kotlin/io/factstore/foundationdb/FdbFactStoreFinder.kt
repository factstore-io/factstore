package io.factstore.foundationdb

import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import io.factstore.core.FactStoreFinder
import io.factstore.core.FactStoreId
import io.factstore.core.FactStoreMetadata
import io.factstore.core.FactStoreName
import kotlinx.coroutines.future.await
import java.time.Instant

class FdbFactStoreFinder(
    context: FoundationDBFactStoreContext
) : FactStoreFinder {

    companion object {
        const val FACTSTORE_METADATA_NAME = "factstore-metadata"
        const val BY_ID = "by-id"
        const val BY_NAME = "by-name"
    }

    private val factStoreDirectory = DirectoryLayer
        .getDefault()
        .createOrOpen(context.database, listOf(FACTSTORE_METADATA_NAME))
        .get()

    private val byIdSubspace =
        factStoreDirectory.subspace(Tuple.from(BY_ID))

    private val byNameSubspace =
        factStoreDirectory.subspace(Tuple.from(BY_NAME))

    private val database = context.database

    override suspend fun listAll(): List<FactStoreMetadata> {
        return database.readAsync { tr ->
            tr.getRange(byIdSubspace.range()).asList()
                .thenApply { kvList ->
                    kvList.map { kv ->
                        val keyTuple = byIdSubspace.unpack(kv.key)
                        val valueTuple = Tuple.fromBytes(kv.value)

                        val id = keyTuple.getLastAsUuid()
                        val name = valueTuple.getString(0)
                        val createdAtEpochSecond = valueTuple.getLong(1)

                        FactStoreMetadata(
                            id = FactStoreId(id),
                            name = FactStoreName(name),
                            createdAt = Instant.ofEpochSecond(createdAtEpochSecond)
                        )
                    }
                }
        }.await()
    }

    override suspend fun existsByName(name: FactStoreName): Boolean {
        return database.readAsync { tr ->
            val nameToIdIndexKey = byNameSubspace.pack(Tuple.from(name.value))
            tr[nameToIdIndexKey].thenApply { valueBytes ->
                valueBytes != null
            }
        }.await()
    }
}