package io.factstore.client

/**
 * Marks the client's builders, so that a block only sees the builder it configures: inside
 * `fact { }`, the enclosing `append { }` builder's `fact` is out of reach.
 */
@DslMarker
annotation class FactStoreDsl
