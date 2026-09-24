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

    override suspend fun append(storeName: StoreName, fact: FactInput): AppendResult =
        append(storeName, listOf(fact))

    override suspend fun append(storeName: StoreName, facts: List<FactInput>): AppendResult =
        append(
            AppendRequest(
                storeName = storeName,
                facts = facts,
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        )

    // ===== FactAppender Implementation =====

    override suspend fun append(request: AppendRequest): AppendResult = lock.withLock {
        val storeId = resolveId(request.storeName) ?: return AppendResult.StoreNotFound(request.storeName)

        // Check idempotency
        val idempotencySet = idempotencyKeys[storeId] ?: throw IllegalStateException("Idempotency set should be initialized for store $storeId")
        if (idempotencySet.contains(request.idempotencyKey.value)) {
            return AppendResult.AlreadyApplied
        }

        val appendedAt = Instant.now()
        val materializedFacts = request.facts.map { it.toFact(FactId.generate(), appendedAt) }

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
        StreamFactsResult.FactStream(
            store.pinnedStream(continuation, request.direction, request.limit) { true }
        )
    }

    override suspend fun streamFactsBySubject(request: StreamFactsBySubjectRequest): StreamFactsBySubjectResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return StreamFactsBySubjectResult.StoreNotFound(request.storeName)
        val store = facts[internalId] ?: return StreamFactsBySubjectResult.StoreNotFound(request.storeName)
        val continuation = request.continueAfter?.let {
            store.continuationIndex(it) ?: return StreamFactsBySubjectResult.ContinuationNotFound(it)
        }
        StreamFactsBySubjectResult.FactStream(
            store.pinnedStream(continuation, request.direction, request.limit) { it.subject == request.subject }
        )
    }

    override suspend fun streamFactsByType(request: StreamFactsByTypeRequest): StreamFactsByTypeResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return StreamFactsByTypeResult.StoreNotFound(request.storeName)
        val store = facts[internalId] ?: return StreamFactsByTypeResult.StoreNotFound(request.storeName)
        val continuation = request.continueAfter?.let {
            store.continuationIndex(it) ?: return StreamFactsByTypeResult.ContinuationNotFound(it)
        }
        StreamFactsByTypeResult.FactStream(
            store.pinnedStream(continuation, request.direction, request.limit) { it.type == request.type }
        )
    }

    override suspend fun streamFactsByTags(request: StreamFactsByTagsRequest): StreamFactsByTagsResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return StreamFactsByTagsResult.StoreNotFound(request.storeName)
        val store = facts[internalId] ?: return StreamFactsByTagsResult.StoreNotFound(request.storeName)
        val continuation = request.continueAfter?.let {
            store.continuationIndex(it) ?: return StreamFactsByTagsResult.ContinuationNotFound(it)
        }
        StreamFactsByTagsResult.FactStream(
            store.pinnedStream(continuation, request.direction, request.limit) { fact ->
                request.tags.all { (key, value) -> fact.tags[key] == value }
            }
        )
    }

    override suspend fun streamFactsByQuery(request: StreamFactsByQueryRequest): StreamFactsByQueryResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return StreamFactsByQueryResult.StoreNotFound(request.storeName)
        val store = facts[internalId] ?: return StreamFactsByQueryResult.StoreNotFound(request.storeName)
        val continuation = request.continueAfter?.let {
            store.continuationIndex(it) ?: return StreamFactsByQueryResult.ContinuationNotFound(it)
        }
        StreamFactsByQueryResult.FactStream(
            store.pinnedStream(continuation, request.direction, request.limit) { fact ->
                request.query.filters.any { it.matches(fact) }
            }
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
            // Copy under the lock, emit outside of it: a collector may append in between.
            val selected = lock.withLock { subList(from, to).filter(predicate) }
            emitAll(selected.applyDirection(direction).applyLimit(limit).asFlow())
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
            is StartPosition.After -> {
                val index = store.indexOfFirst { it.id == position.factId }
                if (index == -1) return SubscribeResult.FactIdNotFound(position.factId)
                index + 1
            }
        }

        SubscribeResult.FactStream(scanFlow(internalId, startIndex))
    }


    /** Follows the store from [startIndex] onwards; a subscription never ends on its own. */
    private fun scanFlow(internalId: UUID, startIndex: Int) = flow<List<Fact>> {
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
            if (batch.isNotEmpty()) emit(batch) else delay(100.milliseconds)
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
            is AppendCondition.TagQueryBased -> {
                val store = facts[storeId]?.toList() ?: return false
                val startIndex = if (condition.after != null) {
                    val index = store.indexOfFirst { it.id == condition.after }
                    if (index == -1) return false
                    index + 1
                } else {
                    0
                }

                val matchingFacts = store.drop(startIndex).filter { fact ->
                    condition.failIfEventsMatch.queryItems.any { queryItem ->
                        when (queryItem) {
                            is TagTypeItem -> {
                                fact.type in queryItem.types &&
                                        queryItem.tags.all { (key, value) ->
                                            fact.tags[key] == value
                                        }
                            }
                            is TagOnlyQueryItem -> {
                                queryItem.tags.all { (key, value) ->
                                    fact.tags[key] == value
                                }
                            }
                        }
                    }
                }
                matchingFacts.isEmpty()
            }
        }
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

    private fun List<Fact>.applyDirection(direction: ReadDirection): List<Fact> = when (direction) {
        ReadDirection.Forward -> this
        ReadDirection.Backward -> asReversed()
    }

    private fun List<Fact>.applyLimit(limit: Limit): List<Fact> = when (val cap = limit.value) {
        null -> this
        else -> take(cap)
    }
}
