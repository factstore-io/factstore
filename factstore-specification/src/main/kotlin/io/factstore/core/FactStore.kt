package io.factstore.core

/**
 * The main interface for interacting with a FactStore.
 *
 * A [FactStore] provides a unified API to append, find, stream and subscribe to
 * facts. It combines the capabilities of [FactAppender], [FactFinder],
 * [FactStreamer], [FactSubscriber] and [StoreFactory] into a single entry point.
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
    FactStreamer,
    FactSubscriber,
    StoreFactory,
    StoreFinder,
    StoreRemover

/**
 * Factory function to create a [FactStore] from separate components.
 *
 * This allows combining distinct implementations of [FactAppender],
 * [FactFinder], [FactStreamer] and [FactSubscriber] into a single [FactStore] instance.
 *
 * Example usage:
 * ```
 * val store: FactStore = FactStore(
 *     factAppender = myAppender,
 *     factFinder = myFinder,
 *     factStreamer = myStreamer,
 *     factSubscriber = mySubscriber,
 * )
 * ```
 *
 * @param factAppender the component responsible for appending facts
 * @param factFinder the component responsible for reading and querying facts
 * @param factStreamer the component responsible for bounded fact streams
 * @param factSubscriber the component responsible for live subscriptions
 * @param storeFactory the component responsible for creating fact stores
 * @param storeFinder the component responsible for finding fact stores
 * @param storeRemover the component responsible for removing fact stores
 * @return a [FactStore] instance delegating operations to the provided components
 *
 * @author Domenic Cassisi
 */
fun FactStore(
    factAppender: FactAppender,
    factFinder: FactFinder,
    factStreamer: FactStreamer,
    factSubscriber: FactSubscriber,
    storeFactory: StoreFactory,
    storeFinder: StoreFinder,
    storeRemover: StoreRemover,
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder,
        FactStreamer by factStreamer,
        FactSubscriber by factSubscriber,
        StoreFactory by storeFactory,
        StoreFinder by storeFinder,
        StoreRemover by storeRemover {}
