package io.factstore.core

/**
 * The main interface for interacting with a FactStore.
 *
 * A [FactStore] provides a unified API to append, find, and stream facts.
 * It combines the capabilities of [FactAppender], [FactFinder], and
 * [FactStreamer] into a single entry point.
 *
 * Implementations may choose to separate responsibilities internally, but
 * the public API guarantees consistent behavior across all operations.
 *
 * Usage of [FactStore] should respect append-only semantics, idempotency
 * guarantees, and conditional append rules defined in [AppendRequest].
 *
 * @author Domenic Cassisi
 */
interface FactStore :
    FactAppender,
    FactFinder,
    FactStreamer

/**
 * Factory function to create a [FactStore] from separate components.
 *
 * This allows combining distinct implementations of [FactAppender],
 * [FactFinder], and [FactStreamer] into a single [FactStore] instance.
 *
 * Example usage:
 * ```
 * val store: FactStore = FactStore(
 *     factAppender = myAppender,
 *     factFinder = myFinder,
 *     factStreamer = myStreamer
 * )
 * ```
 *
 * @param factAppender the component responsible for appending facts
 * @param factFinder the component responsible for reading and querying facts
 * @param factStreamer the component responsible for streaming facts
 * @return a [FactStore] instance delegating operations to the provided components
 *
 * @author Domenic Cassisi
 */
fun FactStore(
    factAppender: FactAppender,
    factFinder: FactFinder,
    factStreamer: FactStreamer,
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder,
        FactStreamer by factStreamer {}
