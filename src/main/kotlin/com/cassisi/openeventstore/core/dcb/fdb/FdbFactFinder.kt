package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.tuple.Tuple
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.FactFinder
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
    private val factPayloadSubspace = fdbFactStore.factPayloadSubspace
    private val subjectTypeSubspace = fdbFactStore.subjectTypeSubspace
    private val subjectIdSubspace = fdbFactStore.subjectIdSubspace

    private val createdAtIndexSubspace = fdbFactStore.createdAtIndexSubspace
    private val subjectIndexSubspace = fdbFactStore.subjectIndexSubspace

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
        }.await()
    }

    private fun ReadTransaction.loadFact(factId: UUID): CompletableFuture<Fact?> {
        val factIdTuple = Tuple.from(factId)
        val typeKey = factTypeSubspace.pack(factIdTuple)
        val createdAtKey = createdAtSubspace.pack(factIdTuple)
        val payloadKey = factPayloadSubspace.pack(factIdTuple)
        val subjectTypeKey = subjectTypeSubspace.pack(factIdTuple)
        val subjectIdKey = subjectIdSubspace.pack(factIdTuple)

        val typeFuture = this[typeKey]
        val createdAtFuture = this[createdAtKey]
        val payloadFuture = this[payloadKey]
        val subjectTypeFuture = this[subjectTypeKey]
        val subjectIdFuture = this[subjectIdKey]

        return CompletableFuture.allOf(
            typeFuture,
            createdAtFuture,
            payloadFuture,
            subjectTypeFuture,
            subjectIdFuture
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

            Fact(
                id = factId,
                type = typeBytes.toString(UTF_8),
                payload = payloadBytes.toString(UTF_8),
                createdAt = createdAtInstant,
                subjectType = subjectTypeBytes.toString(UTF_8),
                subjectId = subjectIdBytes.toString(UTF_8)
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
                val factFutures: List<CompletableFuture<Fact?>> = kvs.map { kv ->
                    val tuple = createdAtIndexSubspace.unpack(kv.key)
                    val factId = tuple.getUUID(tuple.size() - 1)
                    tr.loadFact(factId)
                }

                // wait for all facts to complete
                CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                    factFutures.mapNotNull { it.getNow(null) }
                }
            }
        }.await()
    }

    override suspend fun findBySubject(subjectType: String, subjectId: String): List<Fact> {
        return db.readAsync { tr ->
            val subjectRange = subjectIndexSubspace.range(Tuple.from(subjectType, subjectId))
            tr.getRange(subjectRange).asList().thenCompose { kvs ->
                val factFutures: List<CompletableFuture<Fact?>> = kvs.map { kv ->
                    val tuple = subjectIndexSubspace.unpack(kv.key)
                    val factId = tuple.getUUID(tuple.size() - 1)
                    tr.loadFact(factId)
                }

                CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                    factFutures.mapNotNull { it.getNow(null) }
                }
            }
        }.await()
    }
}