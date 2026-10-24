package io.factstore.foundationdb

import io.factstore.core.FactQuery
import io.factstore.core.FactQueryItem
import io.factstore.core.FactType
import io.factstore.core.ReadDirection
import io.factstore.core.StoreId
import io.factstore.core.Subject
import io.factstore.core.TagKey
import io.factstore.core.TagValue

/**
 * Turns queries into trees of [PositionSource]s over the store's indexes.
 *
 * The same tree serves a stream and an append condition; they differ only in how the keys are
 * read, which is the [reader]'s choice, and in where the index ranges end: a stream stops at the
 * head it pinned, while a condition passes no head and reads every range to its end.
 *
 * @param batchSize the number of keys a cursor reads at a time
 */
internal class QuerySources(
    private val keyspace: FdbKeyspace,
    private val reader: KeyReader,
    private val batchSize: Int,
) {

    /**
     * The positions of the facts this query matches: an item is an intersection of its
     * predicates, and the query the union of its items.
     *
     * Building the sources reads nothing, so a query can be inspected before it is read.
     *
     * @param head the last position the ranges include, or `null` to read them to their ends
     */
    fun FactQuery.toSource(storeId: StoreId, head: FactPosition?, direction: ReadDirection): PositionSource =
        anyOf(items.map { it.toSource(storeId, head, direction) }, direction)

    /**
     * The positions of the facts this item matches: every predicate it sets must hold, while a
     * predicate holding several values matches any of them.
     */
    private fun FactQueryItem.toSource(storeId: StoreId, head: FactPosition?, direction: ReadDirection): PositionSource {
        val tags = tags.map { (key, value) -> key to value }

        val predicates = buildList {
            if (subjects.isNotEmpty()) {
                add(anyOf(subjects.map { subjectCursor(storeId, it, head, direction) }, direction))
            }

            if (types.isNotEmpty() && tags.isNotEmpty()) {
                // Facts are indexed by type and tag together, and that index holds only the facts
                // carrying both, so reading a tag from it skips every fact of the type without it.
                // Each type read this way costs its own cursor, so tags are folded into the types
                // only as far as that keeps the number of cursors the same.
                if (types.size == 1) {
                    val type = types.single()
                    tags.forEach { add(typeTagCursor(storeId, type, it, head, direction)) }
                } else {
                    val folded = tags.first()
                    add(anyOf(types.map { typeTagCursor(storeId, it, folded, head, direction) }, direction))
                    tags.drop(1).forEach { add(tagCursor(storeId, it, head, direction)) }
                }
            } else {
                if (types.isNotEmpty()) {
                    add(anyOf(types.map { typeCursor(storeId, it, head, direction) }, direction))
                }
                tags.forEach { add(tagCursor(storeId, it, head, direction)) }
            }
        }

        return if (predicates.size == 1) predicates.single() else AllOf(predicates, direction)
    }

    /** Combines sources into a union, without wrapping a single one. */
    private fun anyOf(sources: List<PositionSource>, direction: ReadDirection): PositionSource =
        if (sources.size == 1) sources.single() else AnyOf(sources, direction)

    private fun subjectCursor(storeId: StoreId, subject: Subject, head: FactPosition?, direction: ReadDirection) =
        keyspace.subjectIndexSubspace.let { index ->
            cursor(
                keys = index.pinnedKeys(storeId, subject, head, direction),
                positionOf = { key -> index.unpackPosition(key) },
                keyOf = { position -> index.getKey(storeId, subject, position) },
            )
        }

    private fun typeCursor(storeId: StoreId, type: FactType, head: FactPosition?, direction: ReadDirection) =
        keyspace.eventTypeIndexSubspace.let { index ->
            cursor(
                keys = index.pinnedKeys(storeId, type, head, direction),
                positionOf = { key -> index.unpackPosition(key) },
                keyOf = { position -> index.getKey(storeId, type, position) },
            )
        }

    private fun typeTagCursor(
        storeId: StoreId,
        type: FactType,
        tag: Pair<TagKey, TagValue>,
        head: FactPosition?,
        direction: ReadDirection,
    ) = keyspace.tagsTypeIndexSubspace.let { index ->
        cursor(
            keys = index.pinnedKeys(storeId, type, tag, head, direction),
            positionOf = { key -> index.unpackPosition(key) },
            keyOf = { position -> index.getKey(storeId, type, tag, position) },
        )
    }

    private fun tagCursor(storeId: StoreId, tag: Pair<TagKey, TagValue>, head: FactPosition?, direction: ReadDirection) =
        keyspace.tagsIndexSubspace.let { index ->
            cursor(
                keys = index.pinnedKeys(storeId, tag, head, direction),
                positionOf = { key -> index.unpackPosition(key) },
                keyOf = { position -> index.getKey(storeId, tag, position) },
            )
        }

    /** A cursor over the [keys] of one index, which says how its keys and positions map to each other. */
    private fun cursor(
        keys: PinnedKeys,
        positionOf: (ByteArray) -> FactPosition,
        keyOf: (FactPosition) -> ByteArray,
    ) = PositionCursor(reader, keys, batchSize, positionOf, keyOf)
}
