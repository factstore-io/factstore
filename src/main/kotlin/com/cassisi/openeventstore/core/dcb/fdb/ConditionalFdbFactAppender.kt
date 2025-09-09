package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.cassisi.openeventstore.core.dcb.ConditionalSubjectFactAppender
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.SubjectAppendCondition
import kotlinx.coroutines.future.await
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.text.Charsets.UTF_8

class ConditionalFdbFactAppender(
    private val store: FdbFactStore
) : ConditionalSubjectFactAppender {

    private val db = store.db

    override suspend fun append(fact: Fact, preCondition: SubjectAppendCondition) {
        db.runAsync { tr ->
            tr.evaluatePreCondition(fact, preCondition)
            tr.store(fact)
            CompletableFuture.completedFuture(null)
        }.await()
    }

    private fun Transaction.evaluatePreCondition(fact: Fact, preCondition: SubjectAppendCondition) {
        val subjectType = fact.subjectType
        val subjectId = fact.subjectId

        val actualLastFactId = getLastFactId(subjectType, subjectId)
        val expectedLastFactId = preCondition.expectedLatestEventId

        if (actualLastFactId != expectedLastFactId) {
            throw IllegalStateException("PreCondition not met: Expected last fact ID $expectedLastFactId, but got $actualLastFactId")
        }
    }

    private fun Transaction.getLastFactId(subjectType: String, subjectId: String) : UUID? {
        val subjectIndexKeyBegin = Tuple.from(subjectType, subjectId)
        val subjectRange = store.subjectTypeIndexSubspace.range(subjectIndexKeyBegin)
        val latestFactKeyValue = this.getRange(subjectRange, 1, true).firstOrNull()
        return latestFactKeyValue?.let {
            val keyTuple = store.subjectTypeIndexSubspace.unpack(it.key)
            keyTuple.getUUID(4)
        }
    }

    private fun Transaction.store(fact: Fact) {
        store(fact, DEFAULT_INDEX)
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

        val subjectIndex = store.subjectTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.subjectType, fact.subjectId, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, subjectIndex, EMPTY_BYTE_ARRAY)
    }
}