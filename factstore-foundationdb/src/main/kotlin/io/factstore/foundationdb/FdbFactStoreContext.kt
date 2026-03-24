package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.directory.DirectoryLayer
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await

data class FdbFactStoreContext(
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

        suspend fun create(db: Database, name: String): FdbFactStoreContext {
            val root = DirectoryLayer
                .getDefault()
                .createOrOpen(db, listOf(FACT_STORE, name))
                .await()

            return FdbFactStoreContext(
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
