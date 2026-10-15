package io.factstore.foundationdb

import com.apple.foundationdb.tuple.Versionstamp
import io.factstore.core.ReadDirection.Forward
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger

class PositionSourceTest {

    @Test
    fun `a union visits the positions of all its sources once, in reading order`(): Unit = runBlocking {
        val union = AnyOf(listOf(source(1, 4, 5), source(2, 4), source(3, 5, 6)), Forward)

        assertThat(union.positions().toList()).isEqualTo(positions(1, 2, 3, 4, 5, 6))
    }

    @Test
    fun `an intersection visits the positions all its sources share`(): Unit = runBlocking {
        val intersection = AllOf(listOf(source(1, 3, 4, 7, 9), source(3, 5, 7, 8, 9), source(2, 3, 7, 9, 10)), Forward)

        assertThat(intersection.positions().toList()).isEqualTo(positions(3, 7, 9))
    }

    @Test
    fun `nested sources combine like flat ones`(): Unit = runBlocking {
        val filters = AnyOf(
            listOf(
                AllOf(listOf(source(1, 2, 3, 6), AnyOf(listOf(source(2), source(6, 8)), Forward)), Forward),
                AllOf(listOf(source(4, 5, 6), source(5, 6, 7)), Forward),
            ),
            Forward,
        )

        assertThat(filters.positions().toList()).isEqualTo(positions(2, 5, 6))
    }

    @Test
    fun `a union reads its sources at the same time`(): Unit = runBlocking {
        val together = ReadTogether(expected = 3)
        val union = AnyOf(listOf(1L, 2L, 3L).map { FakeSource(positions(it), together::read) }, Forward)

        // Read one after another, the first read would wait for the others forever.
        val first = withTimeout(TIMEOUT_MS) { union.peek() }

        assertThat(first).isEqualTo(position(1))
    }

    @Test
    fun `an intersection reads its sources at the same time`(): Unit = runBlocking {
        val together = ReadTogether(expected = 3)
        val intersection = AllOf(List(3) { FakeSource(positions(5), together::read) }, Forward)

        val first = withTimeout(TIMEOUT_MS) { intersection.peek() }

        assertThat(first).isEqualTo(position(5))
    }

    @Test
    fun `nested sources read at the same time`(): Unit = runBlocking {
        val together = ReadTogether(expected = 4)
        val union = AnyOf(
            listOf(
                AnyOf(listOf(FakeSource(positions(1), together::read), FakeSource(positions(2), together::read)), Forward),
                AllOf(listOf(FakeSource(positions(3), together::read), FakeSource(positions(3), together::read)), Forward),
            ),
            Forward,
        )

        val first = withTimeout(TIMEOUT_MS) { union.peek() }

        assertThat(first).isEqualTo(position(1))
    }

    @Test
    fun `a source is read once for a position, however often it is peeked`(): Unit = runBlocking {
        val sources = listOf(FakeSource(positions(1, 2)), FakeSource(positions(1, 3)))
        val union = AnyOf(sources, Forward)

        repeat(3) { union.peek() }

        assertThat(sources.map { it.reads }).containsExactly(1, 1)
    }

    @Test
    fun `an intersection needs no read once one of its sources has run out`(): Unit = runBlocking {
        val lasting = FakeSource(positions(1, 2, 3))
        val intersection = AllOf(listOf(FakeSource(positions(1)), lasting), Forward)

        assertThat(intersection.positions().toList()).isEqualTo(positions(1))
        val readsWhenDone = lasting.reads
        lasting.seekTo(position(3))

        assertThat(intersection.needsRead).isFalse()
        assertThat(intersection.peek()).isNull()
        assertThat(lasting.reads).isEqualTo(readsWhenDone)
    }

    private fun source(vararg positions: Long) = FakeSource(positions(*positions))

    /**
     * A source over fixed positions that, like a cursor, has to read before it can answer: at
     * first, and again whenever it was moved with [seekTo].
     */
    private class FakeSource(
        positions: List<FactPosition>,
        private val read: suspend () -> Unit = {},
    ) : PositionSource {

        private val remaining = ArrayDeque(positions)
        var reads = 0
            private set

        override var needsRead = true
            private set

        override suspend fun peek(): FactPosition? {
            if (needsRead) {
                reads++
                read()
                needsRead = false
            }
            return remaining.firstOrNull()
        }

        override suspend fun advance() {
            peek()
            remaining.removeFirstOrNull()
        }

        override fun seekTo(position: FactPosition) {
            while (remaining.isNotEmpty() && remaining.first() < position) remaining.removeFirst()
            needsRead = true
        }
    }

    /** A read that returns only once [expected] reads are under way at the same time. */
    private class ReadTogether(private val expected: Int) {
        private val started = AtomicInteger()
        private val allStarted = CompletableDeferred<Unit>()

        suspend fun read() {
            if (started.incrementAndGet() == expected) allStarted.complete(Unit)
            allStarted.await()
        }
    }

    private companion object {
        const val TIMEOUT_MS = 5_000L

        fun position(value: Long): FactPosition =
            Versionstamp.complete(ByteBuffer.allocate(10).putLong(value).array())

        fun positions(vararg values: Long): List<FactPosition> = values.map(::position)
    }
}
