package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.tuple.Tuple
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.FactFinder
import com.cassisi.openeventstore.core.dcb.PayloadAttributeCondition
import com.cassisi.openeventstore.core.dcb.PayloadQuery
import com.cassisi.openeventstore.core.dcb.PayloadQueryItem
import com.cassisi.openeventstore.core.dcb.Subject
import kotlinx.coroutines.future.await
import java.time.Instant
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.text.Charsets.UTF_8

class FdbFactFinder(fdbFactStore: FdbFactStore) : FactFinder {

    private val db = fdbFactStore.db

    private val factIdSubspace = fdbFactStore.factIdSubspace
    private val factTypeSubspace = fdbFactStore.factTypeSubspace
    private val createdAtSubspace = fdbFactStore.createdAtSubspace
    private val positionSubspace = fdbFactStore.positionSubspace
    private val factPayloadSubspace = fdbFactStore.factPayloadSubspace
    private val subjectTypeSubspace = fdbFactStore.subjectTypeSubspace
    private val subjectIdSubspace = fdbFactStore.subjectIdSubspace
    private val metadataSubspace = fdbFactStore.metadataSubspace
    private val tagsSubspace = fdbFactStore.tagsSubspace

    private val createdAtIndexSubspace = fdbFactStore.createdAtIndexSubspace
    private val subjectIndexSubspace = fdbFactStore.subjectIndexSubspace
    private val tagsIndexSubspace = fdbFactStore.tagsIndexSubspace

    override suspend fun findById(factId: UUID): Fact? {
        return db.readAsync { tr ->
            val factIdKey = factIdSubspace.pack(Tuple.from(factId))
            tr[factIdKey].thenCompose { exists ->
                if (exists == null) {
                    CompletableFuture.completedFuture(null)
                } else {
                    tr.loadFact(factId)
                }
            }
        }.await()?.fact
    }

    private fun ReadTransaction.loadFact(factId: UUID): CompletableFuture<InternalFact?> {
        val factIdTuple = Tuple.from(factId)
        val typeKey = factTypeSubspace.pack(factIdTuple)
        val createdAtKey = createdAtSubspace.pack(factIdTuple)
        val positionKey = positionSubspace.pack(factIdTuple)
        val payloadKey = factPayloadSubspace.pack(factIdTuple)
        val subjectTypeKey = subjectTypeSubspace.pack(factIdTuple)
        val subjectIdKey = subjectIdSubspace.pack(factIdTuple)
        val metadataKey = metadataSubspace.range(factIdTuple)
        val tagsRange = tagsSubspace.range(factIdTuple)

        val typeFuture = this[typeKey]
        val createdAtFuture = this[createdAtKey]
        val positionKeyFuture = this[positionKey]
        val payloadFuture = this[payloadKey]
        val subjectTypeFuture = this[subjectTypeKey]
        val subjectIdFuture = this[subjectIdKey]
        val metadataFuture = this.getRange(metadataKey).asList()
        val tagsFuture = this.getRange(tagsRange).asList()

        return CompletableFuture.allOf(
            typeFuture,
            createdAtFuture,
            positionKeyFuture,
            payloadFuture,
            subjectTypeFuture,
            subjectIdFuture,
            metadataFuture,
            tagsFuture
        ).thenApply {
            val typeBytes = typeFuture.getNow(null) ?: return@thenApply null
            val createdAtBytes = createdAtFuture.getNow(null) ?: return@thenApply null
            val payloadBytes = payloadFuture.getNow(null) ?: return@thenApply null
            val subjectTypeBytes = subjectTypeFuture.getNow(null) ?: return@thenApply null
            val subjectIdBytes = subjectIdFuture.getNow(null) ?: return@thenApply null
            val createdAtTuple = Tuple.fromBytes(createdAtBytes)
            val createdAtInstant = Instant.ofEpochSecond(
                createdAtTuple.getLong(0),
                createdAtTuple.getLong(1)
            )

            val positionBytes = positionKeyFuture.getNow(null) ?: return@thenApply null
            val positionTuple = Tuple.fromBytes(positionBytes)

            val metadata: Map<String, String> = metadataFuture.getNow(emptyList()).associate { kv ->
                val tuple = Tuple.fromBytes(kv.key)
                val key = tuple.getString(3)
                val value = kv.value.toString(UTF_8)
                key to value
            }

            val tags: Map<String, String> = tagsFuture.getNow(emptyList()).associate { kv ->
                val tuple = Tuple.fromBytes(kv.key)
                val key = tuple.getString(tuple.size() - 1) // /fact-store/tags/{factId}/{key}
                val value = kv.value.toString(UTF_8)
                key to value
            }

            val fact = Fact(
                id = factId,
                type = typeBytes.toString(UTF_8),
                payload = payloadBytes,
                createdAt = createdAtInstant,
                subject = Subject(
                    type = subjectTypeBytes.toString(UTF_8),
                    id = subjectIdBytes.toString(UTF_8)
                ),
                metadata = metadata,
                tags = tags,
            )

            InternalFact(
                fact = fact,
                positionTuple = positionTuple
            )
        }
    }


    override suspend fun existsById(factId: UUID): Boolean {
        return db.readAsync { tr ->
            val factIdKey = factIdSubspace.pack(Tuple.from(factId))
            tr[factIdKey]
        }.await() != null
    }

    override suspend fun findInTimeRange(start: Instant, end: Instant): List<Fact> {
        val startTuple = Tuple.from(start.epochSecond, start.nano)
        val endTuple = Tuple.from(end.epochSecond, end.nano)

        return db.readAsync { tr ->
            val begin = createdAtIndexSubspace.pack(startTuple)
            val endKey = createdAtIndexSubspace.pack(endTuple)

            tr.getRange(begin, endKey).asList().thenCompose { kvs ->
                val factFutures: List<CompletableFuture<InternalFact?>> = kvs.map { kv ->
                    val tuple = createdAtIndexSubspace.unpack(kv.key)
                    val factId = tuple.getUUID(tuple.size() - 1)
                    tr.loadFact(factId)
                }

                // wait for all facts to complete
                CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                    factFutures.mapNotNull { it.getNow(null)?.fact }
                }
            }
        }.await()
    }

    override suspend fun findBySubject(subjectType: String, subjectId: String): List<Fact> {
        return db.readAsync { tr ->
            val subjectRange = subjectIndexSubspace.range(Tuple.from(subjectType, subjectId))
            tr.getRange(subjectRange).asList().thenCompose { kvs ->
                val factFutures: List<CompletableFuture<InternalFact?>> = kvs.map { kv ->
                    val tuple = subjectIndexSubspace.unpack(kv.key)
                    val factId = tuple.getUUID(tuple.size() - 1)
                    tr.loadFact(factId)
                }

                CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                    factFutures.mapNotNull { it.getNow(null)?.fact }
                }
            }
        }.await()
    }

    override suspend fun findByTags(tags: List<Pair<String, String>>): List<Fact> {
        if (tags.isEmpty()) return emptyList()
        return db.readAsync { tr ->
            // For each (key, value) pair, get matching factIds
            val tagFutures: List<CompletableFuture<Set<UUID>>> = tags.map { (key, value) ->
                val range = tagsIndexSubspace.range(Tuple.from(key, value))
                tr.getRange(range).asList().thenApply { kvs ->
                    kvs.mapTo(mutableSetOf()) { kv ->
                        val tuple = tagsIndexSubspace.unpack(kv.key)
                        tuple.getUUID(tuple.size() - 1)
                    }
                }
            }

            // Once all tag lookups finish, union them and load facts
            CompletableFuture.allOf(*tagFutures.toTypedArray()).thenCompose {
                val allFactIds: Set<UUID> = tagFutures
                    .flatMap { it.getNow(emptySet()) }
                    .toSet() // OR semantics = union

                val loadFutures: List<CompletableFuture<InternalFact?>> = allFactIds.map { tr.loadFact(it) }

                CompletableFuture.allOf(*loadFutures.toTypedArray()).thenApply {
                    loadFutures
                        .mapNotNull { it.getNow(null) }
                        .sortedBy { it.positionTuple }
                        .map { it.fact }
                }
            }
        }.await()
    }

}

data class InternalFact(
    val fact: Fact,
    val positionTuple: Tuple
)
