package io.factstore.foundationdb

import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import io.factstore.core.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
import kotlinx.coroutines.supervisorScope
import java.time.Instant

private const val REVERSED = true
private const val LIMIT_ONE = 1

/**
 * The number of keys a cursor of an append condition reads at a time.
 *
 * A condition stops at its first match, so a single index range rarely needs more than one key,
 * while an intersection walks its ranges in step and is cheaper with larger batches. Every key is
 * read within the append's transaction.
 */
private const val CONDITION_BATCH_SIZE = 256

class FdbFactAppender(
    private val store: FdbFactStore,
) : FactAppender {

    /**
     * Appends within one transaction, which FoundationDB retries as a whole on a conflict.
     *
     * The transaction's body is a coroutine, because an append condition walks the same cursors
     * a stream does. It runs in a supervisor scope: a failed attempt completes its future
     * exceptionally, for FoundationDB to retry, rather than cancelling the caller.
     */
    override suspend fun append(request: AppendRequest): AppendResult = supervisorScope {
        store.db.runAsync { tr -> future { with(tr) { appendToStore(request) } } }.await()
    }

    context(tr: Transaction)
    private suspend fun appendToStore(request: AppendRequest): AppendResult {
        val storeId = store.context.lookUpStoreIdByName(request.storeName).await()
            ?: return AppendResult.StoreNotFound(request.storeName)

        return with(storeId) {
            if (tr[request.idempotencyKeyBytes()].await() != null) return AppendResult.AlreadyApplied

            // A condition naming a fact the store does not have cannot be judged at all
            val positions = request.condition.afterPositions()
            positions.entries.firstOrNull { it.value == null }?.let { return AppendResult.AfterFactNotFound(it.key) }

            if (!request.condition.isSatisfied(positions)) return AppendResult.AppendConditionViolated

            val appendedAt = Instant.now()
            val facts = request.facts.map { it.toFact(FactId.generate(), appendedAt) }
            with(request) { facts.store() }
            store.context.idempotencySubspace.save(storeId, request.idempotencyKey)
            AppendResult.Appended(facts.map { it.id }, appendedAt)
        }
    }

    /**
     * The position of every fact a [AppendCondition.NoneMatch] in this condition checks after, in
     * the order the condition names them, or `null` for a fact the store does not have.
     */
    context(tr: Transaction, storeId: StoreId)
    private suspend fun AppendCondition.afterPositions(): Map<FactId, FactPosition?> =
        afterFactIds().distinct()
            .associateWith { store.context.factPositionIndexSubspace.getPosition(storeId, it) }
            .mapValues { (_, position) -> position.await() }

    private fun AppendCondition.afterFactIds(): List<FactId> = when (this) {
        AppendCondition.None, is AppendCondition.ExpectedLastFact -> emptyList()
        is AppendCondition.All -> conditions.flatMap { it.afterFactIds() }
        is AppendCondition.NoneMatch -> listOfNotNull(after)
    }

    /** Whether the condition holds, given the [positions] of the facts it checks after. */
    context(tr: Transaction, storeId: StoreId)
    private suspend fun AppendCondition.isSatisfied(positions: Map<FactId, FactPosition?>): Boolean =
        when (this) {
            AppendCondition.None -> true
            is AppendCondition.ExpectedLastFact -> subject.getLastFactId() == expectedLastFactId
            is AppendCondition.All -> coroutineScope {
                conditions.map { async { it.isSatisfied(positions) } }.awaitAll().all { it }
            }
            is AppendCondition.NoneMatch -> isSatisfied(positions)
        }

    /**
     * Whether no fact matching the query follows the fact the condition checks after.
     *
     * The query is read with the cursors of a stream, within the append's transaction and to the
     * end of every index range. A concurrent append of a matching fact lands at the end of a range
     * this read covered, so the two transactions conflict and this one is retried. Reading stops
     * at the first match.
     */
    context(tr: Transaction, storeId: StoreId)
    private suspend fun AppendCondition.NoneMatch.isSatisfied(positions: Map<FactId, FactPosition?>): Boolean {
        val sources = QuerySources(store.context, KeyReader.within(tr), CONDITION_BATCH_SIZE)
        val source = with(sources) { query.toSource(storeId, head = null, ReadDirection.Forward) }
        return source.continuingAfter(after?.let { positions.getValue(it) }).peek() == null
    }

    context(tr: Transaction, storeId: StoreId)
    private suspend fun Subject.getLastFactId(): FactId? {
        val subjectRange = store.context.subjectIndexSubspace.range(storeId, this)
        return tr.getRange(subjectRange, LIMIT_ONE, REVERSED)
            .asList()
            .await()
            .firstOrNull()?.let { Tuple.fromBytes(it.value).getFirstAsFactId() }
    }

    context(storeId: StoreId)
    private fun AppendRequest.idempotencyKeyBytes(): ByteArray =
        store.context.idempotencySubspace.pack(storeId, idempotencyKey)

    context(tr: Transaction, appendRequest: AppendRequest, storeId: StoreId)
    private fun List<Fact>.store() = with(store) {
        appendRequest.storeName.apply { this@store.store() }
    }

}
