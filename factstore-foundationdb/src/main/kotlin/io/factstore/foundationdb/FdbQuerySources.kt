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
     * How this query is read up to [head]: from one index range, or by combining several.
     *
     * A source without anything combined is a single cursor, and this is the one place that
     * relies on it: the sources are built here, so here is where their shape is known.
     */
    fun FactQuery.toPlan(storeId: StoreId, head: FactPosition?, direction: ReadDirection): QueryPlan =
        when (val source = toSource(storeId, head, direction)) {
            is PositionCursor -> QueryPlan.SingleRange(source.index, source.keys)
            else -> QueryPlan.Combined { toSource(storeId, head, direction) }
        }

    /**
     * The positions of the facts this item matches: every predicate it sets must hold, while a
     * predicate holding several values matches any of them.
     */
    private fun FactQueryItem.toSource(storeId: StoreId, head: FactPosition?, direction: ReadDirection): PositionSource {
        val tags = tags.entries

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
        cursor(keyspace.subjectIndexSubspace.indexRange(storeId, subject), head, direction)

    private fun typeCursor(storeId: StoreId, type: FactType, head: FactPosition?, direction: ReadDirection) =
        cursor(keyspace.typeIndexSubspace.indexRange(storeId, type), head, direction)

    private fun typeTagCursor(
        storeId: StoreId,
        type: FactType,
        tag: Map.Entry<TagKey, TagValue>,
        head: FactPosition?,
        direction: ReadDirection,
    ) = cursor(keyspace.tagsTypeIndexSubspace.indexRange(storeId, type, tag), head, direction)

    private fun tagCursor(storeId: StoreId, tag: Map.Entry<TagKey, TagValue>, head: FactPosition?, direction: ReadDirection) =
        cursor(keyspace.tagsIndexSubspace.indexRange(storeId, tag), head, direction)

    /** A cursor over one index range, reading its keys up to [head] in [direction]. */
    private fun cursor(index: IndexRange, head: FactPosition?, direction: ReadDirection) =
        PositionCursor(reader, index, index.pinnedKeys(head, direction), batchSize)
}

/** How a query is read: one index range scanned as it is, or several combined by cursors. */
internal sealed interface QueryPlan {

    /**
     * The query is one index range, whose keys are scanned directly, index entries and facts
     * together. That is the case for one subject, one type, one tag, or one type and tag.
     */
    class SingleRange(val index: IndexRange, val keys: PinnedKeys) : QueryPlan

    /**
     * The query combines several index ranges through a tree of cursors.
     *
     * Cursors are consumed as they are read, so every read builds a tree of its own: that is what
     * keeps a stream repeatable, rather than letting a second collection find the cursors of the
     * first used up.
     */
    class Combined(private val build: () -> PositionSource) : QueryPlan {
        fun newSource(): PositionSource = build()
    }
}
