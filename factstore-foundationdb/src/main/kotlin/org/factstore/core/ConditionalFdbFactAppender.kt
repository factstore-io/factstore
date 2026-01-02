package org.factstore.core

import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import kotlinx.coroutines.future.await
import java.util.concurrent.CompletableFuture

const val ERROR_MESSAGE_TEMPLATE = "PreCondition not met for subject (%s, %s): expected %s but got %s"

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

    override suspend fun append(facts: List<Fact>, preCondition: MultiSubjectAppendCondition) {
        db.runAsync { tr ->
            tr.evaluatePreCondition(preCondition)

            facts.forEachIndexed { index, fact ->
                tr.store(fact, index)
            }

            CompletableFuture.completedFuture(null)
        }.await()
    }

    private fun Transaction.evaluatePreCondition(preCondition: MultiSubjectAppendCondition) {
        preCondition.expectedLastEventIds.forEach { (subjectKey, expectedLastFactId) ->
            val (subjectType, subjectId) = subjectKey
            val actualLastFactId = getLastFactId(subjectType, subjectId)
            if (actualLastFactId != expectedLastFactId) {
                throw AppendConditionViolationException(
                    message = ERROR_MESSAGE_TEMPLATE.format(subjectType, subjectId, expectedLastFactId, actualLastFactId)
                )
            }
        }
    }

    private fun Transaction.evaluatePreCondition(fact: Fact, preCondition: SubjectAppendCondition) {
        val subjectType = fact.subject.type
        val subjectId = fact.subject.id

        val actualLastFactId = getLastFactId(subjectType, subjectId)
        val expectedLastFactId = preCondition.expectedLatestEventId

        if (actualLastFactId != expectedLastFactId) {
            throw AppendConditionViolationException(
                message = ERROR_MESSAGE_TEMPLATE.format(subjectType, subjectId, expectedLastFactId, actualLastFactId)
            )
        }
    }

    private fun Transaction.getLastFactId(subjectType: String, subjectId: String) : FactId? {
        val subjectIndexKeyBegin = Tuple.from(subjectType, subjectId)
        val subjectRange = store.subjectIndexSubspace.range(subjectIndexKeyBegin)
        val latestFactKeyValue = this.getRange(subjectRange, 1, true).firstOrNull()
        return latestFactKeyValue?.let {
            store.subjectIndexSubspace.unpack(it.key).getLastAsFactId()
        }
    }

    private fun Transaction.store(fact: Fact) {
        store(fact, DEFAULT_INDEX)
    }

    private fun Transaction.store(fact: Fact, index: Int = DEFAULT_INDEX) = with(store) {
        this@store.store(fact, index)
    }

}