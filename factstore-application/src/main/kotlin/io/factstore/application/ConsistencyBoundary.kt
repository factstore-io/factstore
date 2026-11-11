package io.factstore.application

/** Decides what a decision reads, and on which condition its facts may be appended. */
interface ConsistencyBoundary<C, Q, A, V> {

    fun query(command: C): Q

    fun condition(command: C, query: Q, version: V?): A
}
