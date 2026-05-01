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
 * This implementation stores all facts and metadata in memory without any persistence.
 * It is not suitable for production use but provides a simple, fast alternative for
 * testing and prototyping.
 *
 * Thread-safety is ensured through the use of coroutine mutexes.
 *
 * @author Domenic Cassisi
 */
class MemoryFactStore : FactStore {

    // Store metadata: StoreId -> StoreMetadata
    private val stores = mutableMapOf<UUID, StoreMetadata>()

    // Store name -> StoreId index
    private val nameIndex = mutableMapOf<String, UUID>()

    // Store facts: StoreId -> list of facts
    private val facts = mutableMapOf<UUID, MutableList<Fact>>()

    // Store idempotency tracking: StoreId -> (IdempotencyKey -> exists flag)
    private val idempotencyKeys = mutableMapOf<UUID, MutableSet<UUID>>()

    private val lock = Mutex()

    // ===== FactStoreFactory Implementation =====

    override suspend fun handle(request: CreateStoreRequest): CreateStoreResult = lock.withLock {
        val existingId = nameIndex[request.storeName.value]
        if (existingId != null) {
            return CreateStoreResult.NameAlreadyExists(request.storeName)
        }

        val id = StoreId.generate()
        val metadata = StoreMetadata(
            id = id,
            name = request.storeName,
            createdAt = Instant.now()
        )

        stores[id.uuid] = metadata
        nameIndex[request.storeName.value] = id.uuid
        facts[id.uuid] = mutableListOf()
        idempotencyKeys[id.uuid] = mutableSetOf()

        CreateStoreResult.Created(id)
    }

    // ===== FactStoreFinder Implementation =====

    override suspend fun listAll(): List<StoreMetadata> = lock.withLock {
        stores.values.toList()
    }

    override suspend fun existsByName(name: StoreName): Boolean = lock.withLock {
        nameIndex.containsKey(name.value)
    }

    override suspend fun findByName(name: StoreName): StoreMetadata? = lock.withLock {
        nameIndex[name.value]?.let { id ->
            stores[id]
        }
    }

    // ===== FactAppender Implementation =====

    override suspend fun append(storeId: StoreId, fact: Fact): AppendResult =
        append(storeId, listOf(fact))

    override suspend fun append(storeId: StoreId, facts: List<Fact>): AppendResult =
        append(
            AppendRequest(
                storeId = storeId,
                facts = facts,
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        )

    override suspend fun append(request: AppendRequest): AppendResult = lock.withLock {
        // Check if fact store exists
        val storeMetadata = stores[request.storeId.uuid]
            ?: return AppendResult.StoreNotFound

        // Check idempotency
        val idempotencySet = idempotencyKeys[request.storeId.uuid]!!
        if (idempotencySet.contains(request.idempotencyKey.value)) {
            return AppendResult.AlreadyApplied
        }

        // Check for duplicate fact IDs
        val store = facts[request.storeId.uuid]!!
        val existingIds = store.map { it.id.uuid }.toSet()
        val duplicates = request.facts.filter { it.id.uuid in existingIds }
        if (duplicates.isNotEmpty()) {
            throw DuplicateFactIdException(duplicates.map { it.id })
        }

        // Check append condition
        val conditionSatisfied = checkAppendCondition(request.storeId.uuid, request.condition)
        if (!conditionSatisfied) {
            return AppendResult.AppendConditionViolated
        }

        // Append facts
        store.addAll(request.facts)

        // Mark idempotency key as used
        idempotencySet.add(request.idempotencyKey.value)

        AppendResult.Appended
    }

    // ===== FactFinder Implementation =====

    override suspend fun findById(storeId: StoreId, factId: FactId): FindByIdResult = lock.withLock {
        if (!stores.containsKey(storeId.uuid)) {
            FindByIdResult.StoreNotFound
        } else {
            val fact = facts[storeId.uuid]?.find { it.id == factId }
            fact?.let { FindByIdResult.Found(it) } ?: FindByIdResult.NotFound(factId)
        }
    }

    override suspend fun existsById(storeId: StoreId, factId: FactId): ExistsByIdResult = lock.withLock {
        if (!stores.containsKey(storeId.uuid)) {
            ExistsByIdResult.StoreNotFound
        } else {
            val exists = facts[storeId.uuid]?.any { it.id == factId } ?: false
            if (exists) ExistsByIdResult.Exists else ExistsByIdResult.DoesNotExist
        }
    }

    override suspend fun findInTimeRange(storeId: StoreId, timeRange: TimeRange): FindInTimeRangeResult = lock.withLock {
        if (!stores.containsKey(storeId.uuid)) {
            FindInTimeRangeResult.StoreNotFound
        } else {
            val start = timeRange.start
            val end = timeRange.end
            val foundFacts = facts[storeId.uuid]
                ?.filter { fact -> fact.appendedAt in start..end }
                ?: emptyList()
            FindInTimeRangeResult.Found(foundFacts)
        }
    }

    override suspend fun findBySubject(storeId: StoreId, subjectRef: SubjectRef): FindBySubjectResult = lock.withLock {
        if (!stores.containsKey(storeId.uuid)) {
            FindBySubjectResult.StoreNotFound
        } else {
            val foundFacts = facts[storeId.uuid]
                ?.filter { fact -> fact.subjectRef == subjectRef }
                ?: emptyList()
            FindBySubjectResult.Found(foundFacts)
        }
    }

    override suspend fun findByTags(storeId: StoreId, tags: List<Pair<TagKey, TagValue>>): FindByTagsResult = lock.withLock {
        if (!stores.containsKey(storeId.uuid)) {
            FindByTagsResult.StoreNotFound
        } else {
            if (tags.isEmpty()) return FindByTagsResult.Found(emptyList())

            val foundFacts = facts[storeId.uuid]?.filter { fact ->
                // OR semantics: fact matches if it has any of the specified tag pairs
                tags.any { (key, value) ->
                    fact.tags[key] == value
                }
            } ?: emptyList()
            FindByTagsResult.Found(foundFacts)
        }
    }

    override suspend fun findByTagQuery(storeId: StoreId, query: TagQuery): FindByTagQueryResult = lock.withLock {
        if (!stores.containsKey(storeId.uuid)) {
            FindByTagQueryResult.StoreNotFound
        } else {
            val foundFacts = facts[storeId.uuid]?.filter { fact ->
                // OR semantics: fact matches if it matches any query item
                query.queryItems.any { queryItem ->
                    when (queryItem) {
                        is TagTypeItem -> {
                            // Type must match AND all tags must match
                            fact.type in queryItem.types &&
                                    queryItem.tags.all { (key, value) ->
                                        fact.tags[key] == value
                                    }
                        }
                        is TagOnlyQueryItem -> {
                            // All tags must match
                            queryItem.tags.all { (key, value) ->
                                fact.tags[key] == value
                            }
                        }
                    }
                }
            } ?: emptyList()
            FindByTagQueryResult.Found(foundFacts)
        }
    }

    // ===== FactStreamer Implementation =====

    override suspend fun stream(storeId: StoreId, streamingOptions: StreamingOptions): StreamResult {
        if (!stores.containsKey(storeId.uuid)) {
            return StreamResult.StoreNotFound
        }

        val startIndex = when (val position = streamingOptions.startPosition) {
            StartPosition.Beginning -> 0
            StartPosition.End -> {
                lock.withLock {
                    facts[storeId.uuid]?.size ?: 0
                }
            }
            is StartPosition.After -> {
                lock.withLock {
                    val store = facts[storeId.uuid] ?: return StreamResult.StoreNotFound
                    val index = store.indexOfFirst { it.id == position.factId }
                    if (index == -1) {
                        return StreamResult.InvalidStartPosition(position.factId)
                    }
                    index + 1
                }
            }
        }

        return StreamResult.FactStream(
            stream = streamFacts(storeId, startIndex)
        )
    }

    private fun streamFacts(storeId: StoreId, startIndex: Int) = flow {
        var currentIndex = startIndex
        while (true) {
            lock.withLock {
                val store = facts[storeId.uuid]
                if (store != null && currentIndex < store.size) {
                    val fact = store[currentIndex]
                    emit(fact)
                    currentIndex++
                }
            }

            // If no more facts, wait a bit and try again (simulating watch behavior)
            if (currentIndex >= (facts[storeId.uuid]?.size ?: 0)) {
                delay(100)
            }
        }
    }

    // ===== Helper Methods =====

    private fun checkAppendCondition(storeId: UUID, condition: AppendCondition): Boolean {
        return when (condition) {
            AppendCondition.None -> true
            is AppendCondition.ExpectedLastFact -> {
                val store = facts[storeId] ?: return false
                val lastFact = store.findLast { it.subjectRef == condition.subjectRef }
                lastFact?.id == condition.expectedLastFactId
            }
            is AppendCondition.ExpectedMultiSubjectLastFact -> {
                val store = facts[storeId] ?: return false
                condition.expectations.all { (subjectRef, expectedId) ->
                    val lastFact = store.findLast { it.subjectRef == subjectRef }
                    lastFact?.id == expectedId
                }
            }
            is AppendCondition.TagQueryBased -> {
                val store = facts[storeId] ?: return false
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