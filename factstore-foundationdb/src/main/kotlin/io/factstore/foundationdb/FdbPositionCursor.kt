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
) {

    private var batch: List<KeyValue> = emptyList()
    private var next = 0

    /** The key the next batch continues after; `null` before the first batch. */
    private var after: ByteArray? = null

    /** Set by [seekTo]: the key the next batch starts at, this time including it. */
    private var from: ByteArray? = null

    private var exhausted = false

    /** The position the cursor points at, or `null` once the range is exhausted. */
    suspend fun peek(): FactPosition? {
        while (next >= batch.size) {
            if (exhausted) return null
            readNextBatch()
        }
        return positionOf(batch[next].key)
    }

    /** Moves to the next position. */
    suspend fun advance() {
        if (peek() != null) next++
    }

    /**
     * Moves to the first position at or after [position] in reading order.
     *
     * Skipping straight to a position keeps an intersection from reading the entries in between,
     * which is what makes a rare tag cheap to combine with a common one.
     */
    fun seekTo(position: FactPosition) {
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
 * Emits the positions every cursor has, in reading order: the facts that match all of them.
 *
 * The cursors are advanced in step, like a merge join: whichever cursor is furthest ahead sets the
 * position to meet, and the others skip to it. A position is emitted when all cursors agree on it.
 */
internal fun intersect(cursors: List<PositionCursor>, direction: ReadDirection): Flow<FactPosition> = flow {
    require(cursors.isNotEmpty()) { "An intersection needs at least one cursor." }
    val order = direction.toComparator()

    while (true) {
        // A cursor without a position means its range ended: nothing can match from here on.
        val positions = cursors.map { it.peek() ?: return@flow }

        val furthest = positions.reduce { left, right -> if (order.compare(left, right) >= 0) left else right }
        if (positions.all { it == furthest }) {
            emit(furthest)
            cursors.forEach { it.advance() }
        } else {
            cursors.forEachIndexed { index, cursor ->
                if (positions[index] != furthest) cursor.seekTo(furthest)
            }
        }
    }
}
