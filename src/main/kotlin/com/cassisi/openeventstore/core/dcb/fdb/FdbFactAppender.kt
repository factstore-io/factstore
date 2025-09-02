package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.FactAppender
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlin.text.Charsets.UTF_8

class FdbFactAppender(
    private val store: FdbFactStore
) : FactAppender {

    private val db = store.db

    override suspend fun append(fact: Fact): Unit = coroutineScope {
        db.runAsync { tr ->
            future {
                tr.store(fact)
            }
        }.await()
    }

    override suspend fun append(facts: List<Fact>): Unit = coroutineScope {
        db.runAsync { tr ->
            future {
                tr.store(facts)
            }
        }.await()
    }

    private suspend fun Transaction.store(facts: List<Fact>) {
        facts.forEachIndexed { index, fact ->
            storeFactData(fact)
            storeIndexes(fact, index)
        }
    }

    private suspend fun Transaction.store(fact: Fact) {
        storeFactData(fact)
        storeIndexes(fact)
    }

    private suspend fun Transaction.storeFactData(fact: Fact) {
        val factId = fact.id // stable key (fact identifier)
        val factIdTuple = Tuple.from(factId)

        // idempotence/existence check
        val idKey = store.factIdSubspace.pack(factIdTuple)
        check(this[idKey].await() == null) { "Fact with ID $factId already exists!" }

        // store fact ID
        this[idKey] = EMPTY_BYTE_ARRAY

        // store fact type
        val typeKey = store.factTypeSubspace.pack(factIdTuple)
        this[typeKey] = fact.type.toByteArray(UTF_8)

        // store fact payload
        val factPayloadKey = store.factPayloadSubspace.pack(factIdTuple)
        this[factPayloadKey] = fact.payload.toByteArray(UTF_8)

        // store createdAt
        val createdAtKey = store.createdAtSubspace.pack(factIdTuple)
        this[createdAtKey] = Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano).pack()
    }

    private fun Transaction.storeIndexes(fact: Fact, index: Int = DEFAULT_INDEX) {
        val factId = fact.id

        // global position index
        // this will add an index like this:
        // /fact-store/global/{versionstamp}/{index}/{factId} = ∅
        val globalPositionKey = store.globalFactPositionSubspace.packWithVersionstamp(
            Tuple.from(Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, EMPTY_BYTE_ARRAY)

        // event type index
        // this will add an index like this:
        // /fact-store/type-index/{type}/{versionstamp}/{index}/{factId} = ∅
        val eventTypeIndexKey = store.eventTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.type, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, EMPTY_BYTE_ARRAY)

        // createdAt index
        // this will add an index like this:
        // /fact-store/created-at-index/{epochSecond}/{nano}/{vs}/{index}/{factId} = ∅
        val createdAtIndexKey = store.createdAtIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, EMPTY_BYTE_ARRAY)
    }
}
