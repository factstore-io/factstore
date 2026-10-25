package io.factstore.foundationdb

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.KeyValue
import com.apple.foundationdb.StreamingMode
import com.apple.foundationdb.Transaction
import io.factstore.core.ReadDirection
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch

/**
 * Reads up to `limit` keys between two selectors, in reverse when asked to.
 *
 * Where the keys are read is the reader's choice: a stream reads every batch in its own
 * transaction ([inOwnTransactions]), while an append condition reads within the append's
 * transaction ([within]), so that its reads conflict with a concurrent append they would miss.
 */
internal fun interface KeyReader {

    suspend fun read(begin: KeySelector, end: KeySelector, limit: Int, reverse: Boolean): List<KeyValue>

    companion object {

        /** Reads every batch in a short transaction of its own, so no transaction stays open. */
        fun inOwnTransactions(access: FdbAccess) = KeyReader { begin, end, limit, reverse ->
            access.inReadTransaction { getRange(begin, end, limit, reverse, StreamingMode.WANT_ALL).asList().await() }
        }

        /** Reads within [tr], which records every range read for conflict checking at commit. */
        fun within(tr: Transaction) = KeyReader { begin, end, limit, reverse ->
            tr.getRange(begin, end, limit, reverse, StreamingMode.WANT_ALL).asList().await()
        }
    }
}

/**
 * Walks the positions behind the keys of one index range, one position at a time.
 *
 * The keys are read in batches through [reader]. Positions are visited in reading order:
 * ascending for [ReadDirection.Forward], descending for [ReadDirection.Backward].
 *
 * @param index the index range, which maps keys to positions and back; building the key a
 *        position would have is what lets the cursor skip ahead
 * @param keys the keys of [index] the cursor reads
 */
internal class PositionCursor(
    private val reader: KeyReader,
    val index: IndexRange,
    val keys: PinnedKeys,
    private val batchSize: Int,
) : PositionSource {

    /** The positions of the batch at hand, read out of its keys once. */
    private var batch: List<FactPosition> = emptyList()
    private var next = 0

    /** The key the next batch continues after; `null` before the first batch. */
    private var after: ByteArray? = null

    /** Set by [seekTo]: the key the next batch starts at, this time including it. */
    private var from: ByteArray? = null

    private var exhausted = false

    override val needsRead: Boolean
        get() = next >= batch.size && !exhausted

    /** The position the cursor points at, or `null` once the range is exhausted. */
    override suspend fun peek(): FactPosition? {
        while (next >= batch.size) {
            if (exhausted) return null
            readNextBatch()
        }
        return batch[next]
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
            if (order.compare(batch[next], position) >= 0) return
            next++
        }
        // Nothing left on this page: continue from the wanted position, unless the range is
        // already exhausted, in which case there is nothing after it either.
        from = index.keyOf(position)
        batch = emptyList()
        next = 0
    }

    private suspend fun readNextBatch() {
        val (begin, end) = from?.let { keys.remainingFrom(it) } ?: keys.remainingAfter(after)
        from = null

        val read = reader.read(begin, end, batchSize, keys.reverse)

        batch = read.map { index.positionOf(it.key) }
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

    /** Whether the next [peek] has to read from the database before it can answer. */
    val needsRead: Boolean
}

/**
 * Lets the sources that need a read do it at the same time.
 *
 * Their reads are independent, so together they cost the latency of one round trip rather than
 * one each. Every source is read by a coroutine of its own and by no other, and nothing runs
 * when fewer than two sources need a read, which is the case for most positions.
 */
private suspend fun List<PositionSource>.readTogether() {
    if (count { it.needsRead } < 2) return
    coroutineScope {
        forEach { source -> if (source.needsRead) launch { source.peek() } }
    }
}

/**
 * The positions of any of the [sources]: their union, each position visited once.
 *
 * This is how a query item matches several subjects or types, and how a query combines its items.
 */
internal class AnyOf(private val sources: List<PositionSource>, direction: ReadDirection) : PositionSource {

    init {
        require(sources.isNotEmpty()) { "A union needs at least one source." }
    }

    private val order = direction.toComparator()

    /** The next position of whichever source is furthest behind: the first one in reading order. */
    override suspend fun peek(): FactPosition? {
        sources.readTogether()
        return sources.mapNotNull { it.peek() }.minWithOrNull(order)
    }

    override suspend fun advance() {
        // Every source sitting on this position moves on, so a fact several sources hold is
        // visited once.
        val current = peek() ?: return
        sources.forEach { source -> if (source.peek() == current) source.advance() }
    }

    override fun seekTo(position: FactPosition) = sources.forEach { it.seekTo(position) }

    override val needsRead: Boolean
        get() = sources.any { it.needsRead }
}

/**
 * The positions every one of the [sources] has: their intersection.
 *
 * This is how a query item requires a subject and a type and several tags at once. The sources are
 * advanced in step, like a merge join: whichever source is furthest ahead sets the position to
 * meet, and the others skip to it.
 */
internal class AllOf(private val sources: List<PositionSource>, direction: ReadDirection) : PositionSource {

    init {
        require(sources.isNotEmpty()) { "An intersection needs at least one source." }
    }

    private val order = direction.toComparator()

    /** Set once a source has run out: sources only move on, so nothing can match after that. */
    private var exhausted = false

    override suspend fun peek(): FactPosition? {
        while (!exhausted) {
            sources.readTogether()
            val positions = sources.map { it.peek() ?: return null.also { exhausted = true } }

            val furthest = positions.reduce { left, right -> if (order.compare(left, right) >= 0) left else right }
            if (positions.all { it == furthest }) return furthest

            sources.forEachIndexed { index, source ->
                if (positions[index] != furthest) source.seekTo(furthest)
            }
        }
        return null
    }

    override suspend fun advance() {
        val current = peek() ?: return
        sources.forEach { source -> if (source.peek() == current) source.advance() }
    }

    override fun seekTo(position: FactPosition) = sources.forEach { it.seekTo(position) }

    override val needsRead: Boolean
        get() = !exhausted && sources.any { it.needsRead }
}

/**
 * Continues the source after [position]: the named position is a marker, so a source sitting
 * exactly on it moves past it.
 */
internal suspend fun PositionSource.continuingAfter(position: FactPosition?): PositionSource {
    if (position == null) return this
    seekTo(position)
    if (peek() == position) advance()
    return this
}

/** Drains the source into a flow of positions, in reading order. */
internal fun PositionSource.positions(): Flow<FactPosition> = flow {
    while (true) {
        emit(peek() ?: return@flow)
        advance()
    }
}
