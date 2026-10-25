package io.factstore.core

/**
 * Represents the direction in which facts are read from the store.
 *
 * Every read states its direction; the specification has no default. [Forward] is the natural
 * ordering of an append-only fact log, oldest fact first, which is why transports that let a
 * client leave the direction out read forward.
 */
enum class ReadDirection {

    /**
     * Facts are returned in store order, from oldest to newest.
     *
     * This is the natural ordering of an append-only log.
     */
    Forward,

    /**
     * Facts are returned in reverse store order, from newest to oldest.
     *
     * Useful for operational queries such as "what are the last N facts",
     * typically combined with a [Limit].
     */
    Backward,
}
