package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.ReadTransaction
import com.apple.foundationdb.tuple.Tuple
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.FactStreamer
import com.cassisi.openeventstore.core.dcb.StreamingOptionSet
import com.cassisi.openeventstore.core.dcb.Subject
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.isActive
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import kotlin.text.Charsets.UTF_8

class FdbFactStreamer(private val store: FdbFactStore) : FactStreamer {

    override fun streamAll(streamingOptionSet: StreamingOptionSet): Flow<Fact> = flow {


        // stream over /fact-store/global/{versionstamp}/{index}/{factId}
        var lastSeenKey: ByteArray? = null
        val globalRange = store.globalFactPositionSubspace.range()

        while (currentCoroutineContext().isActive) {
            // 1) Read the next batch of global positions after the cursor
            val events = store.db.readAsync { tr ->
                val beginSel =
                    if (lastSeenKey == null)
                        KeySelector.firstGreaterOrEqual(globalRange.begin)
                    else
                        KeySelector.firstGreaterThan(lastSeenKey)

                val endSel = KeySelector.firstGreaterOrEqual(globalRange.end)
                val batchSize = streamingOptionSet.batchSize
                tr.getRange(beginSel, endSel, batchSize).asList().thenCompose { keyValues ->
                    val factFutures: List<CompletableFuture<Fact?>> = keyValues.mapNotNull { keyValue ->
                        val k = store.globalFactPositionSubspace.unpack(keyValue.key)
                        val factId = k.getUUID(k.size()-1)
                        lastSeenKey = keyValue.key // VERY DIRTY, FIX ME!!!
                        tr.loadFact(factId)
                    }

                    CompletableFuture.allOf(*factFutures.toTypedArray()).thenApply {
                        factFutures.mapNotNull { it.getNow(null) }
                    }
                }
            }.await()

            if (events.isEmpty()) {
                delay(streamingOptionSet.pollDelayMs)
                continue
            }

            emitAll(events.asFlow())
        }
    }

    private fun ReadTransaction.loadFact(factId: UUID): CompletableFuture<Fact?> {
        val factIdTuple = Tuple.from(factId)
        val typeKey = store.factTypeSubspace.pack(factIdTuple)
        val createdAtKey = store.createdAtSubspace.pack(factIdTuple)
        val positionKey = store.positionSubspace.pack(factIdTuple)
        val payloadKey = store.factPayloadSubspace.pack(factIdTuple)
        val subjectTypeKey = store.subjectTypeSubspace.pack(factIdTuple)
        val subjectIdKey = store.subjectIdSubspace.pack(factIdTuple)
        val metadataKey = store.metadataSubspace.range(factIdTuple)
        val tagsRange = store.tagsSubspace.range(factIdTuple)

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

            Fact(
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
        }
    }

}