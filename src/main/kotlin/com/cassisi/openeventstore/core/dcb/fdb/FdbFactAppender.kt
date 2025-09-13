package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.FactAppender
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture
import kotlin.text.Charsets.UTF_8

class FdbFactAppender(
    private val store: FdbFactStore,
) : FactAppender {

    private val db = store.db

    override suspend fun append(fact: Fact) {
        db.runAsync { tr ->
            val idKey = store.factIdSubspace.pack(Tuple.from(fact.id))

            tr[idKey].thenApply { existing ->
                check(existing == null) { "Fact with ID ${fact.id} already exists!" }
                tr.store(fact)
            }
        }.await()
    }

    override suspend fun append(facts: List<Fact>) {
        db.runAsync { tr ->
            val checks = facts.map { fact ->
                val idKey = store.factIdSubspace.pack(Tuple.from(fact.id))
                tr[idKey].thenApply { existing ->
                    check(existing == null) { "Fact with ID ${fact.id} already exists!" }
                }
            }

            CompletableFuture.allOf(*checks.toTypedArray()).thenApply {
                facts.forEachIndexed { index, fact ->
                    tr.store(fact, index)
                }
            }
        }.await()
    }

    private fun Transaction.store(fact: Fact, index: Int = DEFAULT_INDEX) {
        storeFact(fact)
        storeIndexes(fact, index)
    }

    private fun Transaction.storeFact(fact: Fact) {
        val factIdTuple = Tuple.from(fact.id)

        this[store.factIdSubspace.pack(factIdTuple)] = EMPTY_BYTE_ARRAY
        this[store.factTypeSubspace.pack(factIdTuple)] = fact.type.toByteArray(UTF_8)
        this[store.factPayloadSubspace.pack(factIdTuple)] = fact.payload.toByteArray(UTF_8)
        this[store.subjectTypeSubspace.pack(factIdTuple)] = fact.subjectType.toByteArray(UTF_8)
        this[store.subjectIdSubspace.pack(factIdTuple)] = fact.subjectId.toByteArray(UTF_8)
        this[store.createdAtSubspace.pack(factIdTuple)] = Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano).pack()

        fact.metadata.forEach { (key, value) ->
            this[store.metadataSubspace.pack(factIdTuple.add(key))] = value.toByteArray(UTF_8)
        }
    }

    private fun Transaction.storeIndexes(fact: Fact, index: Int) {
        val factId = fact.id

        val globalPositionKey = store.globalFactPositionSubspace.packWithVersionstamp(
            Tuple.from(Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, EMPTY_BYTE_ARRAY)

        val eventTypeIndexKey = store.eventTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.type, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, EMPTY_BYTE_ARRAY)

        val createdAtIndexKey = store.createdAtIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, EMPTY_BYTE_ARRAY)

        val subjectIndex = store.subjectIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.subjectType, fact.subjectId, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, subjectIndex, EMPTY_BYTE_ARRAY)

        fact.metadata.forEach { (key, value) ->
            val metadataEntryIndex = store.metadataIndexSubspace.packWithVersionstamp(
                Tuple.from(key, value, Versionstamp.incomplete(), index, factId)
            )
            mutate(SET_VERSIONSTAMPED_KEY, metadataEntryIndex, EMPTY_BYTE_ARRAY)
        }
    }
}
