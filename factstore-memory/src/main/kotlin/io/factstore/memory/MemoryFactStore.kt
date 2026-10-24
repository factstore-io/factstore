package io.factstore.memory

import io.factstore.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.util.*
import kotlin.time.Duration.Companion.milliseconds

/**
 * In-memory implementation of FactStore for testing and evaluation purposes.
 *
 * This implementation abstracts logical [StoreName]s from internal [UUID] identifiers.
 * Thread-safety is ensured through the use of coroutine mutexes.
 */
class MemoryFactStore : FactStore {

    // Store metadata: Internal ID -> StoreMetadata
    private val stores = mutableMapOf<UUID, StoreMetadata>()

    // Logical Name -> Internal ID (The "Name Resolver")
    private val nameToId = mutableMapOf<String, UUID>()

    // Store facts: Internal ID -> list of facts
    private val facts = mutableMapOf<UUID, MutableList<Fact>>()

    // Store idempotency tracking: Internal ID -> (IdempotencyKey -> exists flag)
    private val idempotencyKeys = mutableMapOf<UUID, MutableSet<UUID>>()

    private val lock = Mutex()

    // ===== Helper: Name Resolution =====

    private fun resolveId(name: StoreName): UUID? = nameToId[name.value]

    // ===== FactStoreFactory Implementation =====

    override suspend fun create(request: CreateStoreRequest): CreateStoreResult = lock.withLock {
        if (nameToId.containsKey(request.storeName.value)) {
            return CreateStoreResult.NameAlreadyExists(request.storeName)
        }

        val id = StoreId.generate()
        val metadata = StoreMetadata(
            id = id,
            name = request.storeName,
            createdAt = Instant.now()
        )

        stores[id.uuid] = metadata
        nameToId[request.storeName.value] = id.uuid
        facts[id.uuid] = mutableListOf()
        idempotencyKeys[id.uuid] = mutableSetOf()

        CreateStoreResult.Created(id)
    }

    // ===== FactStoreFinder Implementation =====

    override suspend fun listAll(): List<StoreMetadata> = lock.withLock {
        stores.values.toList()
    }

    override suspend fun existsByName(request: ExistsStoreByNameRequest): ExistsStoreByNameResult = lock.withLock {
        if (nameToId.containsKey(request.storeName.value)) {
            ExistsStoreByNameResult.StoreExists
        } else {
            ExistsStoreByNameResult.StoreAbsent
        }
    }

    override suspend fun findByName(request: FindStoreByNameRequest): FindStoreByNameResult = lock.withLock {
        resolveId(request.storeName)?.let { stores[it] }?.let { FindStoreByNameResult.Found(it) } ?: FindStoreByNameResult.NotFound(request.storeName)
    }

    override suspend fun append(request: AppendRequest): AppendResult = lock.withLock {
        val storeId = resolveId(request.storeName) ?: return AppendResult.StoreNotFound(request.storeName)

        // Check idempotency
        val idempotencySet = idempotencyKeys[storeId] ?: throw IllegalStateException("Idempotency set should be initialized for store $storeId")
        if (idempotencySet.contains(request.idempotencyKey.value)) {
            return AppendResult.AlreadyApplied
        }

        val appendedAt = Instant.now()
        val materializedFacts = request.facts.map { it.toFact(FactId.generate(), appendedAt) }

        // A condition naming a fact the store does not have cannot be judged at all
        request.condition.afterFactIds()
            .firstOrNull { after -> facts[storeId]?.none { it.id == after } ?: true }
            ?.let { return AppendResult.AfterFactNotFound(it) }

        // Check append condition
        if (!checkAppendCondition(storeId, request.condition)) {
            return AppendResult.AppendConditionViolated
        }

        // Append facts
        facts[storeId]?.addAll(materializedFacts)
        idempotencySet.add(request.idempotencyKey.value)

        AppendResult.Appended(materializedFacts.map { it.id }, appendedAt)
    }

    // ===== FactFinder Implementation =====

    override suspend fun findById(request: FindByIdRequest): FindByIdResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return FindByIdResult.StoreNotFound(request.storeName)
        val fact = facts[internalId]?.find { it.id == request.factId }
        fact?.let { FindByIdResult.Found(it) } ?: FindByIdResult.NotFound(request.factId)
    }

    override suspend fun existsById(request: ExistsByIdRequest): ExistsByIdResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return ExistsByIdResult.StoreNotFound(request.storeName)
        val exists = facts[internalId]?.any { it.id == request.factId } ?: false
        if (exists) ExistsByIdResult.Exists else ExistsByIdResult.DoesNotExist
    }

    // ===== FactStreamer Implementation =====

    override suspend fun streamFacts(request: StreamFactsRequest): StreamFactsResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return StreamFactsResult.StoreNotFound(request.storeName)
        val store = facts[internalId] ?: return StreamFactsResult.StoreNotFound(request.storeName)
        val continuation = request.continueAfter?.let {
            store.continuationIndex(it) ?: return StreamFactsResult.ContinuationNotFound(it)
        }
        val query = request.query
        StreamFactsResult.FactStream(
            store.pinnedStream(continuation, request.direction, request.limit) { query == null || query.matches(it) }
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

    /** Where the fact sits among this store's facts, or `null` if the store does not hold it. */
    private fun List<Fact>.continuationIndex(factId: FactId): Int? =
        indexOfFirst { it.id == factId }.takeIf { it >= 0 }

    // ===== FactSubscriber Implementation =====

    override suspend fun subscribe(request: SubscribeRequest): SubscribeResult = lock.withLock {
        val storeName = request.storeName
        val internalId = resolveId(storeName) ?: return SubscribeResult.StoreNotFound(storeName)
        val store = facts[internalId] ?: return SubscribeResult.StoreNotFound(storeName)

        val startIndex = when (val position = request.startPosition) {
            StartPosition.Beginning -> 0
            StartPosition.End -> store.size
            is StartPosition.ContinueAfter -> {
                val index = store.indexOfFirst { it.id == position.factId }
                if (index == -1) return SubscribeResult.ContinuationNotFound(position.factId)
                index + 1
            }
        }

        SubscribeResult.FactStream(scanFlow(internalId, startIndex))
    }

    /** Follows the store from [startIndex] onwards; a subscription never ends on its own. */
    private fun scanFlow(internalId: UUID, startIndex: Int) = flow {
        var currentIndex = startIndex
        while (true) {
            val batch = lock.withLock {
                val store = facts[internalId]
                val head = store?.size ?: 0
                if (store != null && currentIndex < head) {
                    val newFacts = store.subList(currentIndex, head).toList()
                    currentIndex = head
                    newFacts
                } else {
                    emptyList()
                }
            }
            if (batch.isEmpty()) delay(100.milliseconds)
            // The subscription follows the store's order: each fact is emitted on its own, and
            // batching is left to whoever transports it.
            else batch.forEach { fact -> emit(fact) }
        }
    }

    private fun checkAppendCondition(storeId: UUID, condition: AppendCondition): Boolean {
        return when (condition) {
            AppendCondition.None -> true
            is AppendCondition.ExpectedLastFact -> {
                val store = facts[storeId]?.toList() ?: return false
                val lastFact = store.findLast { it.subject == condition.subject }
                lastFact?.id == condition.expectedLastFactId
            }
            is AppendCondition.All -> condition.conditions.all { checkAppendCondition(storeId, it) }
            is AppendCondition.NoneMatch -> {
                val store = facts[storeId]?.toList() ?: return false
                // The fact exists: unknown ones are turned away before conditions are checked
                val startIndex = condition.after?.let { after -> store.indexOfFirst { it.id == after } + 1 } ?: 0
                store.drop(startIndex).none { condition.query.matches(it) }
            }
        }
    }

    /** The facts every [AppendCondition.NoneMatch] in this condition checks after. */
    private fun AppendCondition.afterFactIds(): List<FactId> = when (this) {
        AppendCondition.None, is AppendCondition.ExpectedLastFact -> emptyList()
        is AppendCondition.All -> conditions.flatMap { it.afterFactIds() }
        is AppendCondition.NoneMatch -> listOfNotNull(after)
    }

    override suspend fun remove(request: RemoveStoreRequest): RemoveStoreResult {
        val storeId = resolveId(request.storeName) ?: return RemoveStoreResult.StoreNotFound(request.storeName)
        lock.withLock {
            nameToId.remove(request.storeName.value)
            stores.remove(storeId)
            facts.remove(storeId)
            idempotencyKeys.remove(storeId)
        }
        return RemoveStoreResult.StoreRemoved(request.storeName)
    }

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
