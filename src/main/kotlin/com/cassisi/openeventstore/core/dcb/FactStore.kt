package com.cassisi.openeventstore.core.dcb

interface FactStore : FactAppender, FactFinder, FactStreamer, ConditionalSubjectFactAppender

fun FactStore(
    factAppender: FactAppender,
    factFinder: FactFinder,
    factStreamer: FactStreamer,
    conditionalSubjectFactAppender: ConditionalSubjectFactAppender
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder,
        FactStreamer by factStreamer,
        ConditionalSubjectFactAppender by conditionalSubjectFactAppender {}