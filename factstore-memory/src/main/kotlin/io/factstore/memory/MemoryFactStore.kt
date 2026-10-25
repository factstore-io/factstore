package io.factstore.memory

import io.factstore.core.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant

/**
 * In-memory implementation of FactStore for testing and evaluation purposes.
 *
 * Stores are kept by name, each with everything that belongs to it. Thread-safety is ensured by
 * one coroutine mutex, [lock], which guards every store and every read of one.
 */
class MemoryFactStore : FactStore {

    /** One store: its metadata, its facts in append order, and the idempotency keys it has seen. */
    private class MemoryStore(val metadata: StoreMetadata) {

        val facts = mutableListOf<Fact>()

        val idempotencyKeys = mutableSetOf<IdempotencyKey>()

        /** The number of facts, which subscriptions watch to learn that facts were appended. */
        val head = MutableStateFlow(0)
    }

    private val stores = mutableMapOf<StoreName, MemoryStore>()

    private val lock = Mutex()

    // ===== StoreFactory Implementation =====

    override suspend fun createStore(request: CreateStoreRequest): CreateStoreResult = lock.withLock {
        if (request.storeName in stores) {
            return CreateStoreResult.NameAlreadyExists(request.storeName)
        }

        val id = StoreId.generate()
        stores[request.storeName] = MemoryStore(StoreMetadata(id = id, name = request.storeName, createdAt = Instant.now()))
        CreateStoreResult.Created(id)
    }

    // ===== StoreFinder Implementation =====

    override suspend fun listStores(): List<StoreMetadata> = lock.withLock {
        stores.values.map { it.metadata }
    }

    override suspend fun existsStoreByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult = lock.withLock {
        if (request.storeName in stores) ExistsStoreByNameResult.Exists else ExistsStoreByNameResult.DoesNotExist
    }

    override suspend fun findStoreByName(request: FindStoreByNameRequest): FindStoreByNameResult = lock.withLock {
        stores[request.storeName]?.let { FindStoreByNameResult.Found(it.metadata) }
            ?: FindStoreByNameResult.NotFound(request.storeName)
    }

    // ===== StoreRemover Implementation =====

    override suspend fun removeStore(request: RemoveStoreRequest): RemoveStoreResult = lock.withLock {
        if (stores.remove(request.storeName) != null) RemoveStoreResult.Removed(request.storeName)
        else RemoveStoreResult.NotFound(request.storeName)
    }

    // ===== FactAppender Implementation =====

    override suspend fun append(request: AppendRequest): AppendResult = lock.withLock {
        val store = stores[request.storeName] ?: return AppendResult.StoreNotFound(request.storeName)

        if (request.idempotencyKey in store.idempotencyKeys) return AppendResult.AlreadyApplied

        // A condition naming a fact the store does not have cannot be judged at all
        request.condition.afterFactIds()
            .firstOrNull { store.indexOf(it) == null }
            ?.let { return AppendResult.AfterFactNotFound(it) }

        if (!store.satisfies(request.condition)) return AppendResult.AppendConditionViolated

        val appendedAt = Instant.now()
        val facts = request.facts.map { it.toFact(FactId.generate(), appendedAt) }
        store.facts += facts
        store.idempotencyKeys += request.idempotencyKey
        store.head.value = store.facts.size

        AppendResult.Appended(facts.map { it.id }, appendedAt)
    }

    /** Whether the condition holds for this store's facts as they are now. Must be called while holding [lock]. */
    private fun MemoryStore.satisfies(condition: AppendCondition): Boolean = when (condition) {
        AppendCondition.None -> true
        is AppendCondition.ExpectedLastFact ->
            facts.findLast { it.subject == condition.subject }?.id == condition.expectedLastFactId
        is AppendCondition.All -> condition.conditions.all { satisfies(it) }
        is AppendCondition.NoneMatch -> {
            // The fact exists: unknown ones are turned away before conditions are checked
            val start = condition.after?.let { after -> facts.indexOfFirst { it.id == after } + 1 } ?: 0
            facts.subList(start, facts.size).none { condition.query.matches(it) }
        }
    }

    /** The facts every [AppendCondition.NoneMatch] in this condition checks after. */
    private fun AppendCondition.afterFactIds(): List<FactId> = when (this) {
        AppendCondition.None, is AppendCondition.ExpectedLastFact -> emptyList()
        is AppendCondition.All -> conditions.flatMap { it.afterFactIds() }
        is AppendCondition.NoneMatch -> listOfNotNull(after)
    }

    // ===== FactFinder Implementation =====

    override suspend fun findFactById(request: FindFactByIdRequest): FindFactByIdResult = lock.withLock {
        val store = stores[request.storeName] ?: return FindFactByIdResult.StoreNotFound(request.storeName)
        store.facts.find { it.id == request.factId }?.let { FindFactByIdResult.Found(it) }
            ?: FindFactByIdResult.NotFound(request.factId)
    }

    override suspend fun existsFactById(request: ExistsFactByIdRequest): ExistsFactByIdResult = lock.withLock {
        val store = stores[request.storeName] ?: return ExistsFactByIdResult.StoreNotFound(request.storeName)
        if (store.indexOf(request.factId) != null) ExistsFactByIdResult.Exists else ExistsFactByIdResult.DoesNotExist
    }

    // ===== FactStreamer Implementation =====

    override suspend fun streamFacts(request: StreamFactsRequest): StreamFactsResult = lock.withLock {
        val store = stores[request.storeName] ?: return StreamFactsResult.StoreNotFound(request.storeName)
        val continuation = request.continueAfter?.let {
            store.indexOf(it) ?: return StreamFactsResult.ContinuationNotFound(it)
        }
        val query = request.query
        StreamFactsResult.FactStream(
            store.facts.pinnedStream(continuation, request.direction, request.limit) { query == null || query.matches(it) }
        )
    }

    /**
     * Pins the head of this store's facts when called, and reads the matching facts
     * up to it only when collected.
     *
     * The list is append-only, so the facts up to the pinned head never change and
     * every collection emits the same facts. Must be called while holding [lock].
     */
    private fun List<Fact>.pinnedStream(
        continuation: Int?,
        direction: ReadDirection,
        limit: Limit,
        predicate: (Fact) -> Boolean,
    ): Flow<Fact> {
        val head = size
        // The facts still to read: those after the continuation reading forward, those before it
        // reading backward.
        val from = if (direction == ReadDirection.Forward) continuation?.plus(1) ?: 0 else 0
        val to = if (direction == ReadDirection.Forward) head else continuation ?: head

        return flow {
            // Select under the lock, emit outside of it: a collector may append in between. The
            // facts are visited in reading order, so the selection stops once the limit is reached
            // instead of copying every match.
            val selected = lock.withLock {
                subList(from, to).inReadingOrder(direction).filter(predicate).limitedTo(limit).toList()
            }
            emitAll(selected.asFlow())
        }
    }

    // ===== FactSubscriber Implementation =====

    override suspend fun subscribe(request: SubscribeRequest): SubscribeResult = lock.withLock {
        val store = stores[request.storeName] ?: return SubscribeResult.StoreNotFound(request.storeName)

        val startIndex = when (val position = request.startPosition) {
            StartPosition.Beginning -> 0
            StartPosition.End -> store.facts.size
            is StartPosition.ContinueAfter ->
                store.indexOf(position.factId)?.plus(1) ?: return SubscribeResult.ContinuationNotFound(position.factId)
        }

        SubscribeResult.FactStream(store.follow(startIndex))
    }

    /**
     * Follows the store from [startIndex] onwards; a subscription never ends on its own.
     *
     * It wakes up whenever [MemoryStore.head] moves and reads everything appended since its last
     * read, so several appends in quick succession are read together.
     */
    private fun MemoryStore.follow(startIndex: Int): Flow<Fact> = flow {
        var next = startIndex
        head.collect { size ->
            if (size > next) {
                val batch = lock.withLock { facts.subList(next, size).toList() }
                next = size
                // The subscription follows the store's order: each fact is emitted on its own, and
                // batching is left to whoever transports it.
                batch.forEach { emit(it) }
            }
        }
    }

    // ===== Helpers =====

    /** Where the fact sits among this store's facts, or `null` if the store does not hold it. */
    private fun MemoryStore.indexOf(factId: FactId): Int? =
        facts.indexOfFirst { it.id == factId }.takeIf { it >= 0 }

    /** The facts in reading order, visited lazily and without copying them. */
    private fun List<Fact>.inReadingOrder(direction: ReadDirection): Sequence<Fact> = when (direction) {
        ReadDirection.Forward -> asSequence()
        ReadDirection.Backward -> asReversed().asSequence()
    }

    private fun Sequence<Fact>.limitedTo(limit: Limit): Sequence<Fact> = when (val cap = limit.value) {
        null -> this
        else -> take(cap)
    }
}
