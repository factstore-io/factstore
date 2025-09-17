package com.cassisi.openeventstore.core.dcb.fdb

import com.apple.foundationdb.MutationType.SET_VERSIONSTAMPED_KEY
import com.apple.foundationdb.Transaction
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.tuple.Versionstamp
import com.cassisi.openeventstore.core.dcb.Fact
import com.cassisi.openeventstore.core.dcb.FactAppender
import com.cassisi.openeventstore.core.dcb.PathElement
import kotlinx.coroutines.future.await
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.util.concurrent.CompletableFuture
import kotlin.text.Charsets.UTF_8

class FdbFactAppender(
    private val store: FdbFactStore,
) : FactAppender {

    private val db = store.db

    override suspend fun append(fact: Fact) {
        db.runAsync { tr ->
            val idKey = store.factIdSubspace.pack(Tuple.from(fact.id))

            tr[idKey].thenApply { existing ->
                check(existing == null) { "Fact with ID ${fact.id} already exists!" }
                tr.store(fact)
            }
        }.await()
    }

    override suspend fun append(facts: List<Fact>) {
        db.runAsync { tr ->
            val checks = facts.map { fact ->
                val idKey = store.factIdSubspace.pack(Tuple.from(fact.id))
                tr[idKey].thenApply { existing ->
                    check(existing == null) { "Fact with ID ${fact.id} already exists!" }
                }
            }

            CompletableFuture.allOf(*checks.toTypedArray()).thenApply {
                facts.forEachIndexed { index, fact ->
                    tr.store(fact, index)
                }
            }
        }.await()
    }

    private fun Transaction.store(fact: Fact, index: Int = DEFAULT_INDEX) {
        storeFact(fact)
        storeIndexes(fact, index)
    }

    private fun Transaction.storeFact(fact: Fact) {
        val factIdTuple = Tuple.from(fact.id)

        this[store.factIdSubspace.pack(factIdTuple)] = EMPTY_BYTE_ARRAY
        this[store.factTypeSubspace.pack(factIdTuple)] = fact.type.toByteArray(UTF_8)
        this[store.factPayloadSubspace.pack(factIdTuple)] = fact.payload.toByteArray(UTF_8)
        this[store.subjectTypeSubspace.pack(factIdTuple)] = fact.subject.type.toByteArray(UTF_8)
        this[store.subjectIdSubspace.pack(factIdTuple)] = fact.subject.id.toByteArray(UTF_8)
        this[store.createdAtSubspace.pack(factIdTuple)] = Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano).pack()

        // store payload/data
        val subspace = store.factPayloadSubspace.subspace(factIdTuple)
        fact.data?.forEach { entry ->
            val entryTuple = entry.path.toTuple()
            this[subspace.pack(entryTuple)] = Tuple.from(entry.value).pack()
        }

        fact.metadata.forEach { (key, value) ->
            this[store.metadataSubspace.pack(factIdTuple.add(key))] = value.toByteArray(UTF_8)
        }
    }

    private fun Transaction.storeIndexes(fact: Fact, index: Int) {
        val factId = fact.id

        val globalPositionKey = store.globalFactPositionSubspace.packWithVersionstamp(
            Tuple.from(Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, globalPositionKey, EMPTY_BYTE_ARRAY)

        val eventTypeIndexKey = store.eventTypeIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.type, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, eventTypeIndexKey, EMPTY_BYTE_ARRAY)

        val createdAtIndexKey = store.createdAtIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.createdAt.epochSecond, fact.createdAt.nano, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, createdAtIndexKey, EMPTY_BYTE_ARRAY)

        val subjectIndex = store.subjectIndexSubspace.packWithVersionstamp(
            Tuple.from(fact.subject.type, fact.subject.id, Versionstamp.incomplete(), index, factId)
        )
        mutate(SET_VERSIONSTAMPED_KEY, subjectIndex, EMPTY_BYTE_ARRAY)

        fact.metadata.forEach { (key, value) ->
            val metadataEntryIndex = store.metadataIndexSubspace.packWithVersionstamp(
                Tuple.from(key, value, Versionstamp.incomplete(), index, factId)
            )
            mutate(SET_VERSIONSTAMPED_KEY, metadataEntryIndex, EMPTY_BYTE_ARRAY)
        }

        storePayloadAttributeIndexes(fact, index)
    }
    private fun Transaction.storePayloadAttributeIndexes(fact: Fact, index: Int) {
        val payloadJson = Json.parseToJsonElement(fact.payload)
        val flattened = flattenJson(payloadJson) // Map<String, String>

        flattened.forEach { (path, value) ->
            val key = store.payloadAttrIndexSubspace.packWithVersionstamp(
                Tuple.from(fact.type, path, value, Versionstamp.incomplete(), index, fact.id)
            )
            mutate(SET_VERSIONSTAMPED_KEY, key, EMPTY_BYTE_ARRAY)
        }
    }

    fun flattenJson(element: JsonElement, prefix: String = ""): Map<String, String> {
        val result = mutableMapOf<String, String>()

        when (element) {
            is JsonObject -> {
                for ((key, child) in element) {
                    val path = if (prefix.isEmpty()) key else "$prefix.$key"
                    result.putAll(flattenJson(child, path))
                }
            }

            is JsonArray -> {
                element.forEachIndexed { index, child ->
                    val path = if (prefix.isEmpty()) index.toString() else "$prefix[$index]"
                    result.putAll(flattenJson(child, path))
                }
            }

            is JsonPrimitive -> {
                result[prefix] = element.content
            }
        }

        return result
    }

    private fun List<PathElement>.toTuple(): Tuple {
        var tuple = Tuple()
        forEach { pathElement ->
            when (pathElement) {
                is PathElement.Key -> tuple = tuple.add(pathElement.name)
                is PathElement.Index -> tuple = tuple.add(pathElement.pos)
            }
        }
        return tuple
    }

}
