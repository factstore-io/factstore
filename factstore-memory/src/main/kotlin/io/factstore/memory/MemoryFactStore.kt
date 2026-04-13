package io.factstore.memory

import io.factstore.core.*
import kotlinx.coroutines.flow.Flow
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

    // Store metadata: FactStoreId -> FactStoreMetadata
    private val stores = mutableMapOf<UUID, FactStoreMetadata>()
    
    // Store name -> FactStoreId index
    private val nameIndex = mutableMapOf<String, UUID>()
    
    // Store facts: FactStoreId -> list of facts
    private val facts = mutableMapOf<UUID, MutableList<Fact>>()
    
    // Store idempotency tracking: FactStoreId -> (IdempotencyKey -> exists flag)
    private val idempotencyKeys = mutableMapOf<UUID, MutableSet<UUID>>()
    
    private val lock = Mutex()

    // ===== FactStoreFactory Implementation =====

    override suspend fun handle(request: CreateFactStoreRequest): CreateFactStoreResult = lock.withLock {
        val existingId = nameIndex[request.factStoreName.value]
        if (existingId != null) {
            return CreateFactStoreResult.NameAlreadyExists(request.factStoreName)
        }

        val id = FactStoreId.generate()
        val metadata = FactStoreMetadata(
            id = id,
            name = request.factStoreName,
            createdAt = Instant.now()
        )

        stores[id.uuid] = metadata
        nameIndex[request.factStoreName.value] = id.uuid
        facts[id.uuid] = mutableListOf()
        idempotencyKeys[id.uuid] = mutableSetOf()

        CreateFactStoreResult.Created(id)
    }

    // ===== FactStoreFinder Implementation =====

    override suspend fun listAll(): List<FactStoreMetadata> = lock.withLock {
        stores.values.toList()
    }

    override suspend fun existsByName(name: FactStoreName): Boolean = lock.withLock {
        nameIndex.containsKey(name.value)
    }

    override suspend fun findByName(name: FactStoreName): FactStoreMetadata? = lock.withLock {
        nameIndex[name.value]?.let { id ->
            stores[id]
        }
    }

    // ===== FactAppender Implementation =====

    override suspend fun append(factStoreId: FactStoreId, fact: Fact): AppendResult =
        append(factStoreId, listOf(fact))

    override suspend fun append(factStoreId: FactStoreId, facts: List<Fact>): AppendResult =
        append(
            AppendRequest(
                factStoreId = factStoreId,
                facts = facts,
                idempotencyKey = IdempotencyKey(),
                condition = AppendCondition.None
            )
        )

    override suspend fun append(request: AppendRequest): AppendResult = lock.withLock {
        // Check if fact store exists
        val storeMetadata = stores[request.factStoreId.uuid]
            ?: return AppendResult.FactStoreNotFound

        // Check idempotency
        val idempotencySet = idempotencyKeys[request.factStoreId.uuid]!!
        if (idempotencySet.contains(request.idempotencyKey.value)) {
            return AppendResult.AlreadyApplied
        }

        // Check for duplicate fact IDs
        val factStore = facts[request.factStoreId.uuid]!!
        val existingIds = factStore.map { it.id.uuid }.toSet()
        val duplicates = request.facts.filter { it.id.uuid in existingIds }
        if (duplicates.isNotEmpty()) {
            throw DuplicateFactIdException(duplicates.map { it.id })
        }

        // Check append condition
        val conditionSatisfied = checkAppendCondition(request.factStoreId.uuid, request.condition)
        if (!conditionSatisfied) {
            return AppendResult.AppendConditionViolated
        }

        // Append facts
        factStore.addAll(request.facts)
        
        // Mark idempotency key as used
        idempotencySet.add(request.idempotencyKey.value)

        AppendResult.Appended
    }

    // ===== FactFinder Implementation =====

    override suspend fun findById(factStoreId: FactStoreId, factId: FactId): FindByIdResult = lock.withLock {
        if (!stores.containsKey(factStoreId.uuid)) {
            FindByIdResult.FactstoreNotFound
        } else {
            val fact = facts[factStoreId.uuid]?.find { it.id == factId }
            fact?.let { FindByIdResult.Found(it) } ?: FindByIdResult.NotFound(factId)
        }
    }

    override suspend fun existsById(factStoreId: FactStoreId, factId: FactId): Boolean = lock.withLock {
        facts[factStoreId.uuid]?.any { it.id == factId } ?: false
    }

    override suspend fun findInTimeRange(factStoreId: FactStoreId, start: Instant, end: Instant): List<Fact> = lock.withLock {
        facts[factStoreId.uuid]
            ?.filter { fact -> fact.appendedAt >= start && fact.appendedAt <= end }
            ?: emptyList()
    }

    override suspend fun findBySubject(factStoreId: FactStoreId, subjectRef: SubjectRef): List<Fact> = lock.withLock {
        facts[factStoreId.uuid]
            ?.filter { fact -> fact.subjectRef == subjectRef }
            ?: emptyList()
    }

    override suspend fun findByTags(factStoreId: FactStoreId, tags: List<Pair<TagKey, TagValue>>): List<Fact> = lock.withLock {
        if (tags.isEmpty()) return emptyList()

        facts[factStoreId.uuid]?.filter { fact ->
            // OR semantics: fact matches if it has any of the specified tag pairs
            tags.any { (key, value) ->
                fact.tags[key] == value
            }
        } ?: emptyList()
    }

    override suspend fun findByTagQuery(factStoreId: FactStoreId, query: TagQuery): List<Fact> = lock.withLock {
        facts[factStoreId.uuid]?.filter { fact ->
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
    }

    // ===== FactStreamer Implementation =====

    override fun stream(factStoreId: FactStoreId, streamingOptions: StreamingOptions): Flow<Fact> = flow {
        var startIndex = when (val position = streamingOptions.startPosition) {
            StartPosition.Beginning -> 0
            StartPosition.End -> {
                lock.withLock {
                    facts[factStoreId.uuid]?.size ?: 0
                }
            }
            is StartPosition.After -> {
                lock.withLock {
                    val factStore = facts[factStoreId.uuid] ?: return@flow
                    val index = factStore.indexOfFirst { it.id == position.factId }
                    if (index == -1) {
                        throw FactIdNotFoundException(position.factId)
                    }
                    index + 1
                }
            }
        }

        while (true) {
            lock.withLock {
                val factStore = facts[factStoreId.uuid]
                if (factStore != null && startIndex < factStore.size) {
                    val fact = factStore[startIndex]
                    emit(fact)
                    startIndex++
                }
            }

            // If no more facts, wait a bit and try again (simulating watch behavior)
            if (startIndex >= (facts[factStoreId.uuid]?.size ?: 0)) {
                kotlinx.coroutines.delay(100)
            }
        }
    }

    // ===== Helper Methods =====

    private fun checkAppendCondition(storeId: UUID, condition: AppendCondition): Boolean {
        return when (condition) {
            AppendCondition.None -> true
            is AppendCondition.ExpectedLastFact -> {
                val factStore = facts[storeId] ?: return false
                val lastFact = factStore.findLast { it.subjectRef == condition.subjectRef }
                lastFact?.id == condition.expectedLastFactId
            }
            is AppendCondition.ExpectedMultiSubjectLastFact -> {
                val factStore = facts[storeId] ?: return false
                condition.expectations.all { (subjectRef, expectedId) ->
                    val lastFact = factStore.findLast { it.subjectRef == subjectRef }
                    lastFact?.id == expectedId
                }
            }
            is AppendCondition.TagQueryBased -> {
                val factStore = facts[storeId] ?: return false
                val startIndex = if (condition.after != null) {
                    val index = factStore.indexOfFirst { it.id == condition.after }
                    if (index == -1) return false
                    index + 1
                } else {
                    0
                }

                val matchingFacts = factStore.drop(startIndex).filter { fact ->
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