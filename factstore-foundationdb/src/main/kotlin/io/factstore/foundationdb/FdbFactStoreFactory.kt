package io.factstore.foundationdb

import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.tuple.Tuple
import io.factstore.core.CreateFactStoreRequest
import io.factstore.core.CreateFactStoreResult
import io.factstore.core.FactStoreFactory
import io.factstore.core.FactStoreId
import kotlinx.coroutines.future.await
import java.time.Instant
import java.time.temporal.ChronoUnit.SECONDS

class FdbFactStoreFactory(
    private val context: FoundationDBFactStoreContext
) : FactStoreFactory {

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

    override suspend fun handle(request: CreateFactStoreRequest): CreateFactStoreResult {
        val factStoreId = FactStoreId.generate()
        val createdAt = Instant.now().truncatedTo(SECONDS)

        // create...
        return context.database.runAsync { tr ->
            // check if name is already taken
            val nameKey = byNameSubspace.pack(Tuple.from(request.factStoreName.value))
            tr.get(nameKey).thenApply { valueBytes ->
                if (valueBytes != null) {
                    // value bytes are not null, hence name is already taken
                    return@thenApply CreateFactStoreResult.NameAlreadyExists(request.factStoreName)
                } else {
                    // name is available
                    // go ahead and create the fact store

                    // write name to Id lookup index
                    tr[nameKey] = Tuple.from(factStoreId.uuid).pack()

                    // write id to metadata
                    val idKey = byIdSubspace.pack(Tuple.from(factStoreId.uuid))
                    tr[idKey] = Tuple.from(request.factStoreName.value, createdAt.epochSecond).pack()

                    CreateFactStoreResult.Created(factStoreId)
                }
            }
        }.await()
    }

}
