package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Versionstamp
import io.factstore.core.Fact
import io.factstore.core.FactId
import io.factstore.core.StoreId
import io.factstore.core.StoreMetadata
import io.factstore.core.StoreName
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.supervisorScope

/**
 * What every FoundationDB component works with: transactions, the keys of the store, and the reads
 * and writes built on them.
 *
 * Components get it by delegation, `FdbAccess by storage`, so they call [inTransaction] and the
 * transaction's operations directly rather than through a reference to the storage.
 */
internal interface FdbAccess {

    val keyspace: FdbKeyspace

    /**
     * Runs [block] in a read transaction, which FoundationDB retries as a whole on a retryable error.
     *
     * The block is a coroutine of the caller, so it keeps the caller's context and is cancelled
     * with it. Like any transaction body it may run more than once, so it must not act outside the
     * transaction.
     */
    suspend fun <T> inReadTransaction(block: suspend ReadTransaction.() -> T): T

    /** Runs [block] in a transaction that may write, committed when it returns; see [inReadTransaction]. */
    suspend fun <T> inTransaction(block: suspend Transaction.() -> T): T

    /** The id of the store called [name], or `null` if there is none. */
    context(tr: ReadTransaction)
    suspend fun storeIdOf(name: StoreName): StoreId?

    /** The metadata of the store, or `null` if there is none. */
    context(tr: ReadTransaction)
    suspend fun metadataOf(storeId: StoreId): StoreMetadata?

    /** The position of the store's newest fact, or `null` if it has none. */
    context(tr: ReadTransaction)
    suspend fun headOf(storeId: StoreId): FactPosition?

    /** The position of a fact of the store, or `null` if the store holds no such fact. */
    context(tr: ReadTransaction)
    suspend fun positionOf(storeId: StoreId, factId: FactId): FactPosition?

    /** A fact of the store, or `null` if the store holds no such fact. */
    context(tr: ReadTransaction)
    suspend fun loadFact(storeId: StoreId, factId: FactId): Fact?

    /** Registers a new store under its name. */
    context(tr: Transaction)
    fun saveMetadata(metadata: StoreMetadata)

    /**
     * Writes [facts] and their index entries, in order.
     *
     * Each fact's position is the transaction's versionstamp with the fact's index in [facts] as
     * user version, so the facts of one append keep their order and share their commit.
     */
    context(tr: Transaction)
    fun storeFacts(storeId: StoreId, facts: List<Fact>)
}

/** The FoundationDB database and the keys FactStore keeps in it. */
internal class FdbStorage(private val db: Database, override val keyspace: FdbKeyspace) : FdbAccess {

    // A supervisor scope, so that a failed attempt completes its future exceptionally for
    // FoundationDB to retry. In a plain coroutineScope the failed attempt would cancel the scope,
    // and with it every retry, which would then start in a cancelled scope and never run.
    override suspend fun <T> inReadTransaction(block: suspend ReadTransaction.() -> T): T =
        supervisorScope { db.readAsync { tr -> future { tr.block() } }.await() }

    override suspend fun <T> inTransaction(block: suspend Transaction.() -> T): T =
        supervisorScope { db.runAsync { tr -> future { tr.block() } }.await() }

    context(tr: ReadTransaction)
    override suspend fun storeIdOf(name: StoreName): StoreId? =
        keyspace.storeNameToIdIndex.storeIdOf(name)

    context(tr: ReadTransaction)
    override suspend fun metadataOf(storeId: StoreId): StoreMetadata? =
        keyspace.storeSubspace.metadataOf(storeId)

    context(tr: ReadTransaction)
    override suspend fun headOf(storeId: StoreId): FactPosition? =
        keyspace.headSubspace.headOf(storeId)

    context(tr: ReadTransaction)
    override suspend fun positionOf(storeId: StoreId, factId: FactId): FactPosition? =
        keyspace.factPositionIndexSubspace.positionOf(storeId, factId)

    context(tr: ReadTransaction)
    override suspend fun loadFact(storeId: StoreId, factId: FactId): Fact? {
        val position = positionOf(storeId, factId) ?: return null
        return keyspace.factSubspace.factAt(storeId, position).await()?.toSerializableFdbFact()?.toFact()
    }

    context(tr: Transaction)
    override fun saveMetadata(metadata: StoreMetadata) {
        keyspace.storeSubspace.save(metadata)
        keyspace.storeNameToIdIndex.save(metadata.name, metadata.id)
    }

    context(tr: Transaction)
    override fun storeFacts(storeId: StoreId, facts: List<Fact>) {
        facts.forEachIndexed { index, fact ->
            val position = Versionstamp.incomplete(index)
            with(keyspace) {
                factSubspace.save(storeId, position, fact.toSerializableFdbFact().encodeToByteArray())
                factPositionIndexSubspace.save(storeId, fact.id, position)
                headSubspace.save(storeId, position)
                typeIndexSubspace.indexRange(storeId, fact.type).save(fact.id, position)
                subjectIndexSubspace.indexRange(storeId, fact.subject).save(fact.id, position)
                fact.tags.forEach { tag ->
                    tagsIndexSubspace.indexRange(storeId, tag).save(fact.id, position)
                    tagsTypeIndexSubspace.indexRange(storeId, fact.type, tag).save(fact.id, position)
                }
            }
        }
    }
}
