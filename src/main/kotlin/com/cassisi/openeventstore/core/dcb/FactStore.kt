package com.cassisi.openeventstore.core.dcb

interface FactStore : FactAppender, FactFinder, ConditionalSubjectFactAppender

fun FactStore(
    factAppender: FactAppender,
    factFinder: FactFinder,
    conditionalSubjectFactAppender: ConditionalSubjectFactAppender
): FactStore =
    object : FactStore,
        FactAppender by factAppender,
        FactFinder by factFinder,
        ConditionalSubjectFactAppender by conditionalSubjectFactAppender {}