package io.factstore.foundationdb

import com.apple.foundationdb.Database
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.StreamingMode
import io.factstore.core.ReadDirection
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await

/**
 * Walks the positions behind the keys of one pinned index range, one position at a time.
 *
 * The keys are read in batches, each in its own transaction, so a cursor that is held while
 * another one is read never keeps a transaction open. Positions are visited in reading order:
 * ascending for [ReadDirection.Forward], descending for [ReadDirection.Backward].
 *
 * @param positionOf reads the position out of an index key
 * @param keyOf builds the index key a position would have, which lets the cursor skip ahead
 */
internal class PositionCursor(
    private val db: Database,
    private val keys: PinnedKeys,
    private val batchSize: Int,
    private val positionOf: (ByteArray) -> FactPosition,
    private val keyOf: (FactPosition) -> ByteArray,
) : PositionSource {

    private var batch: List<KeyValue> = emptyList()
    private var next = 0

    /** The key the next batch continues after; `null` before the first batch. */
    private var after: ByteArray? = null

    /** Set by [seekTo]: the key the next batch starts at, this time including it. */
    private var from: ByteArray? = null

    private var exhausted = false

    /** The position the cursor points at, or `null` once the range is exhausted. */
    override suspend fun peek(): FactPosition? {
        while (next >= batch.size) {
            if (exhausted) return null
            readNextBatch()
        }
        return positionOf(batch[next].key)
    }

    /** Moves to the next position. */
    override suspend fun advance() {
        if (peek() != null) next++
    }

    /**
     * Moves to the first position at or after [position] in reading order.
     *
     * Skipping straight to a position keeps an intersection from reading the entries in between,
     * which is what makes a rare tag cheap to combine with a common one.
     */
    override fun seekTo(position: FactPosition) {
        val order = keys.readingOrder
        // The wanted position may still be inside the batch at hand.
        while (next < batch.size) {
            if (order.compare(positionOf(batch[next].key), position) >= 0) return
            next++
        }
        // Nothing left on this page: continue from the wanted position, unless the range is
        // already exhausted, in which case there is nothing after it either.
        from = keyOf(position)
        batch = emptyList()
        next = 0
    }

    private suspend fun readNextBatch() {
        val (begin, end) = from?.let { keys.remainingFrom(it) } ?: keys.remainingAfter(after)
        from = null

        val read = db.readAsync { tr ->
            tr.getRange(begin, end, batchSize, keys.reverse, StreamingMode.WANT_ALL).asList()
        }.await()

        batch = read
        next = 0
        after = read.lastOrNull()?.key ?: after
        // A batch smaller than requested means the range up to the pinned key is exhausted.
        exhausted = read.size < batchSize
    }
}

/**
 * A stream of fact positions in reading order, which can be combined with others.
 *
 * Sources form a tree: [PositionCursor]s read one index range each, while [AnyOf] and [AllOf]
 * combine them. Every source visits positions in the same reading order, which is what lets them
 * be merged, and can skip ahead with [seekTo], which is what keeps a rare source cheap to combine
 * with a common one.
 */
internal interface PositionSource {

    /** The position this source points at, or `null` once it has none left. */
    suspend fun peek(): FactPosition?

    /** Moves past the current position. */
    suspend fun advance()

    /** Moves to the first position at or after [position] in reading order. */
    fun seekTo(position: FactPosition)
}

/**
 * The positions of any of the [sources]: their union, each position visited once.
 *
 * This is how a filter matches several subjects or types, and how a query combines its filters.
 */
internal class AnyOf(private val sources: List<PositionSource>, direction: ReadDirection) : PositionSource {

    init {
        require(sources.isNotEmpty()) { "A union needs at least one source." }
    }

    private val order = direction.toComparator()

    /** The next position of whichever source is furthest behind: the first one in reading order. */
    override suspend fun peek(): FactPosition? =
        sources.mapNotNull { it.peek() }.minWithOrNull(order)

    override suspend fun advance() {
        // Every source sitting on this position moves on, so a fact several sources hold is
        // visited once.
        val current = peek() ?: return
        sources.forEach { source -> if (source.peek() == current) source.advance() }
    }

    override fun seekTo(position: FactPosition) = sources.forEach { it.seekTo(position) }
}

/**
 * The positions every one of the [sources] has: their intersection.
 *
 * This is how a filter requires a subject and a type and several tags at once. The sources are
 * advanced in step, like a merge join: whichever source is furthest ahead sets the position to
 * meet, and the others skip to it.
 */
internal class AllOf(private val sources: List<PositionSource>, direction: ReadDirection) : PositionSource {

    init {
        require(sources.isNotEmpty()) { "An intersection needs at least one source." }
    }

    private val order = direction.toComparator()

    override suspend fun peek(): FactPosition? {
        while (true) {
            // A source without a position means nothing can match from here on.
            val positions = sources.map { it.peek() ?: return null }

            val furthest = positions.reduce { left, right -> if (order.compare(left, right) >= 0) left else right }
            if (positions.all { it == furthest }) return furthest

            sources.forEachIndexed { index, source ->
                if (positions[index] != furthest) source.seekTo(furthest)
            }
        }
    }

    override suspend fun advance() {
        val current = peek() ?: return
        sources.forEach { source -> if (source.peek() == current) source.advance() }
    }

    override fun seekTo(position: FactPosition) = sources.forEach { it.seekTo(position) }
}

/** Drains the source into a flow of positions, in reading order. */
internal fun PositionSource.positions(): Flow<FactPosition> = flow {
    while (true) {
        emit(peek() ?: return@flow)
        advance()
    }
}
