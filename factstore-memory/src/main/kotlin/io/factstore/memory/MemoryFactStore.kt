package io.factstore.memory

import io.factstore.core.*
import kotlinx.coroutines.delay
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

    override suspend fun findInTimeRange(request: FindInTimeRangeRequest): FindInTimeRangeResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return FindInTimeRangeResult.StoreNotFound(request.storeName)
        val start = request.timeRange.start
        val end = request.timeRange.end
        val foundFacts = facts[internalId]
            ?.filter { (start == null || it.appendedAt >= start) && (end == null || it.appendedAt < end) }
            ?.applyDirection(request.direction)
            ?.applyLimit(request.limit)
            ?: emptyList()
        FindInTimeRangeResult.Found(foundFacts)
    }

    override suspend fun findBySubject(request: FindBySubjectRequest): FindBySubjectResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return FindBySubjectResult.StoreNotFound(request.storeName)
        val foundFacts = facts[internalId]
            ?.filter { it.subject == request.subject }
            ?.applyDirection(request.direction)
            ?.applyLimit(request.limit)
            ?: emptyList()
        FindBySubjectResult.Found(foundFacts)
    }

    override suspend fun findByTags(request: FindByTagsRequest): FindByTagsResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return FindByTagsResult.StoreNotFound(request.storeName)
        val foundFacts = facts[internalId]
            ?.filter { fact -> request.tags.all { (key, value) -> fact.tags[key] == value } }
            ?.applyDirection(request.direction)
            ?.applyLimit(request.limit)
            ?: emptyList()
        FindByTagsResult.Found(foundFacts)
    }

    override suspend fun findByTagQuery(request: FindByTagQueryRequest): FindByTagQueryResult = lock.withLock {
        val internalId = resolveId(request.storeName) ?: return FindByTagQueryResult.StoreNotFound(request.storeName)
        val foundFacts = facts[internalId]?.filter { fact ->
            request.query.queryItems.any { it.matches(fact) }
        } ?: emptyList()
        FindByTagQueryResult.Found(foundFacts)
    }

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

        // No upper bound: the subscription follows the live tail indefinitely.
        SubscribeResult.FactStream(scanFlow(internalId, startIndex, endIndex = null))
    }

    // ===== FactReplayer Implementation =====

    override suspend fun replay(request: ReplayRequest): ReplayResult = lock.withLock {
        val storeName = request.storeName
        val internalId = resolveId(storeName) ?: return ReplayResult.StoreNotFound(storeName)
        val store = facts[internalId] ?: return ReplayResult.StoreNotFound(storeName)

        val startIndex = when (val start = request.start) {
            ReplayStart.Beginning -> 0
            is ReplayStart.After -> {
                val index = store.indexOfFirst { it.id == start.factId }
                if (index == -1) return ReplayResult.FactIdNotFound(start.factId)
                index + 1
            }
        }

        // Pin the end at replay start; the flow completes once it is reached.
        ReplayResult.FactStream(scanFlow(internalId, startIndex, endIndex = store.size))
    }

    private fun scanFlow(internalId: UUID, startIndex: Int, endIndex: Int?) = flow<List<Fact>> {
        var currentIndex = startIndex
        while (true) {
            val batch = lock.withLock {
                val store = facts[internalId]
                // A replay (endIndex != null) never reads past its pinned end.
                val upperBound = endIndex ?: store?.size ?: 0
                if (store != null && currentIndex < upperBound) {
                    val newFacts = store.subList(currentIndex, upperBound).toList()
                    currentIndex = upperBound
                    newFacts
                } else {
                    emptyList()
                }
            }
            when {
                batch.isNotEmpty() -> emit(batch)
                // Replay reached its pinned end => complete.
                endIndex != null -> return@flow
                else -> delay(100.milliseconds)
            }
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

/**
 * Extension for cleaner matching logic in the memory implementation
 */
private fun TagQueryItem.matches(fact: Fact): Boolean = when (this) {
    is TagTypeItem -> fact.type in this.types && this.tags.all { (k, v) -> fact.tags[k] == v }
    is TagOnlyQueryItem -> this.tags.all { (k, v) -> fact.tags[k] == v }
}
