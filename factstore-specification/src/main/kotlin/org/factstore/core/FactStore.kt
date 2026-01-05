package org.factstore.core

interface FactStore :
    FactAppender,
    FactFinder,
    FactStreamer

fun FactStore(
    factAppender: FactAppender,
    factFinder: FactFinder,
    factStreamer: FactStreamer,
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder,
        FactStreamer by factStreamer {}
