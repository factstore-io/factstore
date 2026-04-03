package io.factstore.foundationdb

import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.github.avrokotlin.avro4k.Avro
import io.factstore.core.FactId
import io.factstore.core.FactStoreId
import io.factstore.core.FactStoreMetadata
import io.factstore.core.FactStoreName
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import java.util.concurrent.CompletableFuture

data class FdbFactStoreContext(
    val storeSubspace: Subspace,
    val storeNameToIdIndex: Subspace,
    val globalFactPositionSubspace: Subspace,
    val headSubspace: Subspace,
    val factPositionsSubspace: Subspace,
    val eventTypeIndexSubspace: Subspace,
    val createdAtIndexSubspace: Subspace,
    val subjectIndexSubspace: Subspace,
    val metadataIndexSubspace: Subspace,
    val tagsIndexSubspace: Subspace,
    val tagsTypeIndexSubspace: Subspace,
    val idempotencySubspace: Subspace,
) {

    companion object {

        fun create(rootDirectory: FactStoreRootDirectory): FdbFactStoreContext {
            val root = rootDirectory.rootDirectorySubspace
            return FdbFactStoreContext(
                storeSubspace = root.subspace(Tuple.from(STORES)),
                storeNameToIdIndex = root.subspace(Tuple.from(STORE_INDEX)),
                globalFactPositionSubspace = root.subspace(Tuple.from(FACTS)),
                headSubspace = root.subspace(Tuple.from(HEAD_INDEX)),
                factPositionsSubspace = root.subspace(Tuple.from(FACT_POSITIONS)),
                eventTypeIndexSubspace = root.subspace(Tuple.from(EVENT_TYPE_INDEX)),
                createdAtIndexSubspace = root.subspace(Tuple.from(CREATED_AT_INDEX)),
                subjectIndexSubspace = root.subspace(Tuple.from(SUBJECT_INDEX)),
                metadataIndexSubspace = root.subspace(Tuple.from(METADATA_INDEX)),
                tagsIndexSubspace = root.subspace(Tuple.from(TAGS_INDEX)),
                tagsTypeIndexSubspace = root.subspace(Tuple.from(TAGS_TYPE_INDEX)),
                idempotencySubspace = root.subspace(Tuple.from(IDEMPOTENCY_KEYS))
            )
        }

    }
}

fun FdbFactStoreContext.getMetadata(factStoreId: FactStoreId, tr: ReadTransaction): CompletableFuture<FdbFactStoreMetadata?> =
    tr[storeSubspace.pack(Tuple.from(factStoreId.uuid))].thenApply { valueBytes ->
        valueBytes?.let { Avro.decodeFromByteArray<FdbFactStoreMetadata>(it) }
    }

fun FdbFactStoreContext.saveMetadata(metadata: FdbFactStoreMetadata, tr: Transaction) {
    tr[storeSubspace.pack(Tuple.from(metadata.storeId))] = Avro.encodeToByteArray(metadata)
    tr[storeNameToIdIndex.pack(Tuple.from(metadata.name))] = Tuple.from(metadata.storeId).pack()
}

fun FdbFactStoreContext.lookUpFactstoreIdByName(name: FactStoreName, tr: ReadTransaction): CompletableFuture<FactStoreId?> =
    tr[storeNameToIdIndex.pack(Tuple.from(name.value))].thenApply { valueBytes ->
        valueBytes?.let { FactStoreId(Tuple.fromBytes(it).getUUID(0)) }
    }

@JvmInline
value class HeadSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun head(factstoreId: FactStoreId): CompletableFuture<FactPosition?> =
        tr[subspace.pack(Tuple.from(factstoreId.uuid))].thenApply { valueBytes ->
            valueBytes?.let { Tuple.fromBytes(it).getVersionstamp(0) }
        }

}

@JvmInline
value class FactPositionSubspace(val subspace: Subspace) {

    context(tr: ReadTransaction)
    fun exists(factstoreId: FactStoreId, factId: FactId): CompletableFuture<Boolean> {
        return tr[subspace.pack(Tuple.from(factstoreId.uuid, factId.uuid))].thenApply {
            it != null
        }
    }

}
