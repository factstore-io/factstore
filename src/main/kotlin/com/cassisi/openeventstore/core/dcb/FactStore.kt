package com.cassisi.openeventstore.core.dcb

interface FactStore : FactAppender, FactFinder

fun FactStore(
    factAppender: FactAppender,
    factFinder: FactFinder,
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder {}