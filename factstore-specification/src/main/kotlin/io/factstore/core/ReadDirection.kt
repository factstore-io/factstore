package io.factstore.core

/**
 * Represents the direction in which facts are read from the store.
 *
 * The default direction for all read operations is [Forward], which reflects
 * the natural ordering of an append-only fact log — oldest fact first.
 */
enum class ReadDirection {

    /**
     * Facts are returned in chronological order, from oldest to newest.
     *
     * This is the natural ordering of an append-only log and the default
     * for all read operations.
     */
    Forward,

    /**
     * Facts are returned in reverse chronological order, from newest to oldest.
     *
     * Useful for operational queries such as "what are the last N facts",
     * typically combined with a [Limit].
     */
    Backward,
}
