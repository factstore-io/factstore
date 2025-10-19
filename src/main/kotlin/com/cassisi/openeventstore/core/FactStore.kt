package com.cassisi.openeventstore.core

interface FactStore : FactAppender, FactFinder, FactStreamer, ConditionalSubjectFactAppender, ConditionalTagQueryFactAppender

fun FactStore(
    factAppender: FactAppender,
    factFinder: FactFinder,
    factStreamer: FactStreamer,
    conditionalSubjectFactAppender: ConditionalSubjectFactAppender,
    conditionalTagQueryFactAppender: ConditionalTagQueryFactAppender
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder,
        FactStreamer by factStreamer,
        ConditionalSubjectFactAppender by conditionalSubjectFactAppender,
        ConditionalTagQueryFactAppender by conditionalTagQueryFactAppender {}
