package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.tuple.Tuple
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.FactFinder
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.future.await
import kotlinx.coroutines.future.future
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

    private val createdAtIndexSubspace = fdbFactStore.createdAtIndexSubspace

    override suspend fun findById(factId: UUID): Fact? {
        return db.readAsync { tr ->
            val factIdTuple = Tuple.from(factId)
            val factIdKey = factIdSubspace.pack(factIdTuple)

            tr[factIdKey].thenCompose { exists ->
                if (exists == null) {
                    CompletableFuture.completedFuture(null)
                } else {
                    val typeKey = factTypeSubspace.pack(factIdTuple)
                    val createdAtKey = createdAtSubspace.pack(factIdTuple)
                    val payloadKey = factPayloadSubspace.pack(factIdTuple)

                    val typeFuture = tr[typeKey]
                    val createdAtFuture = tr[createdAtKey]
                    val payloadFuture = tr[payloadKey]

                    CompletableFuture.allOf(typeFuture, createdAtFuture, payloadFuture).thenApply {
                        val typeBytes = typeFuture.getNow(null) ?: return@thenApply null
                        val createdAtBytes = createdAtFuture.getNow(null) ?: return@thenApply null
                        val payloadBytes = payloadFuture.getNow(null) ?: return@thenApply null

                        val createdAtTuple = Tuple.fromBytes(createdAtBytes)
                        val createdAtInstant = Instant.ofEpochSecond(
                            createdAtTuple.getLong(0),
                            createdAtTuple.getLong(1)
                        )

                        Fact(
                            id = factId,
                            type = typeBytes.toString(UTF_8),
                            payload = payloadBytes.toString(UTF_8),
                            createdAt = createdAtInstant
                        )
                    }
                }
            }
        }.await()
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
                    val factIdTuple = Tuple.from(factId)

                    val typeFut = tr[factTypeSubspace.pack(factIdTuple)]
                    val payloadFut = tr[factPayloadSubspace.pack(factIdTuple)]
                    val createdAtFut = tr[createdAtSubspace.pack(factIdTuple)]

                    // run all three futures in parallel
                    CompletableFuture.allOf(typeFut, payloadFut, createdAtFut).thenApply {
                        val typeBytes = typeFut.getNow(null) ?: return@thenApply null
                        val payloadBytes = payloadFut.getNow(null) ?: return@thenApply null
                        val createdAtBytes = createdAtFut.getNow(null) ?: return@thenApply null

                        val createdAtTuple = Tuple.fromBytes(createdAtBytes)
                        val createdAtInstant = Instant.ofEpochSecond(
                            createdAtTuple.getLong(0),
                            createdAtTuple.getLong(1)
                        )

                        Fact(
                            id = factId,
                            type = typeBytes.toString(UTF_8),
                            payload = payloadBytes.toString(UTF_8),
                            createdAt = createdAtInstant
                        )
                    }
                }

                // wait for all facts to complete
                CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                    factFutures.mapNotNull { it.getNow(null) }
                }
            }
        }.await()
    }
}