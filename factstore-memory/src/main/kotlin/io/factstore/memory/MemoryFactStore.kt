package io.factstore.memory

import io.factstore.core.*
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.time.Instant
import java.util.*

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

    override suspend fun handle(request: CreateStoreRequest): CreateStoreResult = lock.withLock {
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

    override suspend fun existsByName(name: StoreName): Boolean = lock.withLock {
        nameToId.containsKey(name.value)
    }

    override suspend fun findByName(name: StoreName): StoreMetadata? = lock.withLock {
        resolveId(name)?.let { stores[it] }
    }

    override suspend fun append(storeName: StoreName, fact: Fact): AppendResult =
        append(storeName, listOf(fact))

    override suspend fun append(storeName: StoreName, facts: List<Fact>): AppendResult =
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
        val storeId = resolveId(request.storeName) ?: return AppendResult.StoreNotFound

        // Check idempotency
        val idempotencySet = idempotencyKeys[storeId] ?: throw IllegalStateException("Idempotency set should be initialized for store $storeId")
        if (idempotencySet.contains(request.idempotencyKey.value)) {
            return AppendResult.AlreadyApplied
        }

        // Check for duplicate fact IDs
        val store = facts[storeId] ?: throw IllegalStateException("Facts list should be initialized for store $storeId")
        val existingIds = store.map { it.id.uuid }.toSet()
        val duplicates = request.facts.filter { it.id.uuid in existingIds }
        if (duplicates.isNotEmpty()) {
            return AppendResult.DuplicateFactIds(duplicates.map { it.id })
        }

        // Check append condition
        if (!checkAppendCondition(storeId, request.condition)) {
            return AppendResult.AppendConditionViolated
        }

        // Append facts
        facts[storeId]?.addAll(request.facts)
        idempotencySet.add(request.idempotencyKey.value)

        AppendResult.Appended
    }

    // ===== FactFinder Implementation =====

    override suspend fun findById(storeName: StoreName, factId: FactId): FindByIdResult = lock.withLock {
        val internalId = resolveId(storeName) ?: return FindByIdResult.StoreNotFound

        val fact = facts[internalId]?.find { it.id == factId }
        fact?.let { FindByIdResult.Found(it) } ?: FindByIdResult.NotFound(factId)
    }

    override suspend fun existsById(storeName: StoreName, factId: FactId): ExistsByIdResult = lock.withLock {
        val internalId = resolveId(storeName) ?: return ExistsByIdResult.StoreNotFound

        val exists = facts[internalId]?.any { it.id == factId } ?: false
        if (exists) ExistsByIdResult.Exists else ExistsByIdResult.DoesNotExist
    }

    override suspend fun findInTimeRange(storeName: StoreName, timeRange: TimeRange): FindInTimeRangeResult = lock.withLock {
        val internalId = resolveId(storeName) ?: return FindInTimeRangeResult.StoreNotFound

        val foundFacts = facts[internalId]?.filter { it.appendedAt in timeRange.start..timeRange.end } ?: emptyList()
        FindInTimeRangeResult.Found(foundFacts)
    }

    override suspend fun findBySubject(storeName: StoreName, subjectRef: SubjectRef): FindBySubjectResult = lock.withLock {
        val internalId = resolveId(storeName) ?: return FindBySubjectResult.StoreNotFound

        val foundFacts = facts[internalId]?.filter { it.subjectRef == subjectRef } ?: emptyList()
        FindBySubjectResult.Found(foundFacts)
    }

    override suspend fun findByTags(storeName: StoreName, tags: List<Pair<TagKey, TagValue>>): FindByTagsResult = lock.withLock {
        val internalId = resolveId(storeName) ?: return FindByTagsResult.StoreNotFound

        val foundFacts = facts[internalId]?.filter { fact ->
            tags.any { (key, value) -> fact.tags[key] == value }
        } ?: emptyList()
        FindByTagsResult.Found(foundFacts)
    }

    override suspend fun findByTagQuery(storeName: StoreName, query: TagQuery): FindByTagQueryResult = lock.withLock {
        val internalId = resolveId(storeName) ?: return FindByTagQueryResult.StoreNotFound

        val foundFacts = facts[internalId]?.filter { fact ->
            query.queryItems.any { it.matches(fact) }
        } ?: emptyList()
        FindByTagQueryResult.Found(foundFacts)
    }

    // ===== FactStreamer Implementation =====

    override suspend fun stream(storeName: StoreName, streamingOptions: StreamingOptions): StreamResult {
        val internalId = lock.withLock { resolveId(storeName) } ?: return StreamResult.StoreNotFound

        val startIndex = when (val position = streamingOptions.startPosition) {
            StartPosition.Beginning -> 0
            StartPosition.End -> lock.withLock { facts[internalId]?.size ?: 0 }
            is StartPosition.After -> {
                lock.withLock {
                    val store = facts[internalId] ?: return StreamResult.StoreNotFound
                    val index = store.indexOfFirst { it.id == position.factId }
                    if (index == -1) return StreamResult.InvalidStartPosition(position.factId)
                    index + 1
                }
            }
        }

        return StreamResult.FactStream(streamFacts(internalId, startIndex))
    }

    private fun streamFacts(internalId: UUID, startIndex: Int) = flow {
        var currentIndex = startIndex
        while (true) {
            var factToEmit: Fact? = null
            lock.withLock {
                val store = facts[internalId]
                if (store != null && currentIndex < store.size) {
                    factToEmit = store[currentIndex]
                    currentIndex++
                }
            }

            factToEmit?.let { emit(it) } ?: delay(100)
        }
    }

    private fun checkAppendCondition(storeId: UUID, condition: AppendCondition): Boolean {
        return when (condition) {
            AppendCondition.None -> true
            is AppendCondition.ExpectedLastFact -> {
                val store = facts[storeId]?.toList() ?: return false
                val lastFact = store.findLast { it.subjectRef == condition.subjectRef }
                lastFact?.id == condition.expectedLastFactId
            }
            is AppendCondition.ExpectedMultiSubjectLastFact -> {
                val store = facts[storeId]?.toList() ?: return false
                condition.expectations.all { (subjectRef, expectedId) ->
                    val lastFact = store.findLast { it.subjectRef == subjectRef }
                    lastFact?.id == expectedId
                }
            }
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
}

/**
 * Extension for cleaner matching logic in the memory implementation
 */
private fun TagQueryItem.matches(fact: Fact): Boolean = when (this) {
    is TagTypeItem -> fact.type in this.types && this.tags.all { (k, v) -> fact.tags[k] == v }
    is TagOnlyQueryItem -> this.tags.all { (k, v) -> fact.tags[k] == v }
}
