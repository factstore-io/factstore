package io.factstore.core

/**
 * The main interface for interacting with a FactStore.
 *
 * A [FactStore] provides a unified API to append, find, subscribe to, and
 * replay facts. It combines the capabilities of [FactAppender], [FactFinder],
 * [FactSubscriber], [FactReplayer], and [StoreFactory] into a single entry point.
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
    FactSubscriber,
    FactReplayer,
    StoreFactory,
    StoreFinder,
    StoreRemover

/**
 * Factory function to create a [FactStore] from separate components.
 *
 * This allows combining distinct implementations of [FactAppender],
 * [FactFinder], [FactSubscriber], and [FactReplayer] into a single
 * [FactStore] instance.
 *
 * Example usage:
 * ```
 * val store: FactStore = FactStore(
 *     factAppender = myAppender,
 *     factFinder = myFinder,
 *     factSubscriber = mySubscriber,
 *     factReplayer = myReplayer,
 * )
 * ```
 *
 * @param factAppender the component responsible for appending facts
 * @param factFinder the component responsible for reading and querying facts
 * @param factSubscriber the component responsible for live subscriptions
 * @param factReplayer the component responsible for bounded replays
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
    factSubscriber: FactSubscriber,
    factReplayer: FactReplayer,
    storeFactory: StoreFactory,
    storeFinder: StoreFinder,
    storeRemover: StoreRemover,
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder,
        FactSubscriber by factSubscriber,
        FactReplayer by factReplayer,
        StoreFactory by storeFactory,
        StoreFinder by storeFinder,
        StoreRemover by storeRemover {}
