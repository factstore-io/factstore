package io.factstore.foundationdb

import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import io.factstore.core.*
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
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

internal class FdbFactAppender(storage: FdbStorage) : FactAppender, FdbAccess by storage {

    /** Appends within one transaction, which FoundationDB retries as a whole on a conflict. */
    override suspend fun append(request: AppendRequest): AppendResult = inTransaction {
        val storeId = storeIdOf(request.storeName)
            ?: return@inTransaction AppendResult.StoreNotFound(request.storeName)

        val idempotencyKey = keyspace.idempotencySubspace.pack(storeId, request.idempotencyKey)
        if (get(idempotencyKey).await() != null) return@inTransaction AppendResult.AlreadyApplied

        // A condition naming a fact the store does not have cannot be judged at all
        val positions = request.condition.afterPositions(storeId)
        positions.entries.firstOrNull { it.value == null }
            ?.let { return@inTransaction AppendResult.AfterFactNotFound(it.key) }

        if (!request.condition.isSatisfied(storeId, positions)) return@inTransaction AppendResult.AppendConditionViolated

        val appendedAt = Instant.now()
        val facts = request.facts.map { it.toFact(FactId.generate(), appendedAt) }
        storeFacts(storeId, facts)
        keyspace.idempotencySubspace.save(storeId, request.idempotencyKey)
        AppendResult.Appended(facts.map { it.id }, appendedAt)
    }

    /**
     * The position of every fact a [AppendCondition.NoneMatch] in this condition checks after, in
     * the order the condition names them, or `null` for a fact the store does not have.
     */
    context(tr: Transaction)
    private suspend fun AppendCondition.afterPositions(storeId: StoreId): Map<FactId, FactPosition?> = coroutineScope {
        afterFactIds().distinct()
            .associateWith { async { positionOf(storeId, it) } }
            .mapValues { (_, position) -> position.await() }
    }

    private fun AppendCondition.afterFactIds(): List<FactId> = when (this) {
        AppendCondition.None, is AppendCondition.ExpectedLastFact -> emptyList()
        is AppendCondition.All -> conditions.flatMap { it.afterFactIds() }
        is AppendCondition.NoneMatch -> listOfNotNull(after)
    }

    /** Whether the condition holds, given the [positions] of the facts it checks after. */
    context(tr: Transaction)
    private suspend fun AppendCondition.isSatisfied(storeId: StoreId, positions: Map<FactId, FactPosition?>): Boolean =
        when (this) {
            AppendCondition.None -> true
            is AppendCondition.ExpectedLastFact -> subject.lastFactId(storeId) == expectedLastFactId
            is AppendCondition.All -> coroutineScope {
                conditions.map { async { it.isSatisfied(storeId, positions) } }.awaitAll().all { it }
            }
            is AppendCondition.NoneMatch -> isSatisfied(storeId, positions)
        }

    /**
     * Whether no fact matching the condition's query follows the fact it checks after.
     *
     * The query is read with the cursors of a stream, within the append's transaction and to the
     * end of every index range. A concurrent append of a matching fact lands at the end of a range
     * this read covered, so the two transactions conflict and this one is retried. Reading stops
     * at the first match.
     */
    context(tr: Transaction)
    private suspend fun AppendCondition.NoneMatch.isSatisfied(storeId: StoreId, positions: Map<FactId, FactPosition?>): Boolean {
        val sources = QuerySources(keyspace, KeyReader.within(tr), CONDITION_BATCH_SIZE)
        val source = with(sources) { query.toSource(storeId, head = null, ReadDirection.Forward) }
        return source.continuingAfter(after?.let { positions.getValue(it) }).peek() == null
    }

    /** The id of the subject's newest fact, or `null` if it has none. */
    context(tr: Transaction)
    private suspend fun Subject.lastFactId(storeId: StoreId): FactId? =
        tr.getRange(keyspace.subjectIndexSubspace.range(storeId, this), LIMIT_ONE, REVERSED)
            .asList()
            .await()
            .firstOrNull()?.let { Tuple.fromBytes(it.value).getFirstAsFactId() }

}
