package io.factstore.foundationdb

import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.produceIn
import java.util.PriorityQueue

private class MergeHead<T>(val value: T, val sourceIndex: Int)

/**
 * Streaming k-way merge of already-ordered [this] sources into a single ordered flow.
 *
 * Each source **must** already be ordered according to [comparator]; the merged output
 * preserves that order. Only one head element per source is buffered at a time, so memory
 * stays bounded by the number of sources regardless of how many elements each emits.
 */
@OptIn(FlowPreview::class)
fun <T> List<Flow<T>>.mergeSortedBy(comparator: Comparator<in T>): Flow<T> = channelFlow {
    if (isEmpty()) return@channelFlow

    val channels: List<ReceiveChannel<T>> = map { it.produceIn(this) }
    val heads = PriorityQueue<MergeHead<T>>(size) { a, b -> comparator.compare(a.value, b.value) }

    channels.forEachIndexed { index, channel ->
        channel.receiveCatching().getOrNull()?.let { heads.add(MergeHead(it, index)) }
    }

    while (heads.isNotEmpty()) {
        val head = heads.poll()
        send(head.value)
        channels[head.sourceIndex].receiveCatching().getOrNull()?.let { heads.add(MergeHead(it, head.sourceIndex)) }
    }
}

/**
 * Groups consecutive elements of the flow into lists of at most [size] elements. The final
 * chunk may be smaller. Order is preserved.
 */
fun <T> Flow<T>.chunked(size: Int): Flow<List<T>> = flow {
    require(size > 0) { "size must be positive, got: $size" }
    val buffer = ArrayList<T>(size)
    collect { element ->
        buffer.add(element)
        if (buffer.size == size) {
            emit(ArrayList(buffer))
            buffer.clear()
        }
    }
    if (buffer.isNotEmpty()) emit(buffer)
}
