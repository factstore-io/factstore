package io.factstore.core

/**
 * Marks DSL builder classes so that Kotlin's `@DslMarker` mechanism prevents
 * query-level options ([FactQueryBuilder.limit], [FactQueryBuilder.direction],
 * [FactQueryBuilder.cursor]) from being set inside filter blocks.
 */
@DslMarker
annotation class FactQueryDsl

/**
 * Builds a [FactQuery] using a type-safe DSL.
 *
 * Multiple predicates added at the root level are implicitly ANDed:
 * - Zero predicates → [FactFilter.All]
 * - One predicate → that predicate directly
 * - Two or more predicates → [FactFilter.Predicate.AllOf]
 *
 * The built [FactFilter] is automatically normalised via
 * [FactFilter.withNormalizedBoundedSelectors] before the [FactQuery] is
 * constructed, injecting ancestor [FactFilter.Predicate.AllOf] leaf constraints
 * into any nested [FactFilter.Predicate.Last] or [FactFilter.Predicate.First] nodes.
 *
 * @author Domenic Cassisi
 */
@FactQueryDsl
class FactQueryBuilder @PublishedApi internal constructor(private val storeName: StoreName) {

    /** Caps the total number of facts delivered. Defaults to no limit. */
    var limit: Limit = Limit.None

    /** Order in which the merged result is delivered. Defaults to [ReadDirection.Forward]. */
    var direction: ReadDirection = ReadDirection.Forward

    /**
     * Exclusive cursor position. A [Forward] query starts after this fact;
     * a [Backward] query starts before it. Absent means start from the boundary
     * of the store (beginning for Forward, end for Backward).
     */
    var cursor: FactId? = null

    private val rootPredicates = mutableListOf<FactFilter.Predicate>()

    /** Matches facts whose subject equals [value]. */
    fun subject(value: String) {
        rootPredicates += FactFilter.Predicate.Subject(Subject(value))
    }

    /** Matches facts whose subject value starts with [prefix]. */
    fun subjectPrefix(prefix: String) {
        rootPredicates += FactFilter.Predicate.SubjectPrefix(prefix)
    }

    /** Matches facts whose type equals [value]. */
    fun type(value: String) {
        rootPredicates += FactFilter.Predicate.Type(FactType(value))
    }

    /** Matches facts that carry the tag [key]=[value]. */
    fun tag(key: String, value: String) {
        rootPredicates += FactFilter.Predicate.Tag(TagKey(key), TagValue(value))
    }

    /** Matches facts whose metadata contains the entry [key]=[value]. */
    fun metadata(key: String, value: String) {
        rootPredicates += FactFilter.Predicate.Metadata(key, value)
    }

    /** Matches facts whose [Fact.appendedAt] falls within [range]. */
    fun timeRange(range: TimeRange) {
        rootPredicates += FactFilter.Predicate.TimeRange(range)
    }

    /**
     * Logical OR: matches facts that satisfy at least one predicate in [block].
     * Must contain at least one predicate.
     */
    fun anyOf(block: PredicateBuilder.() -> Unit) {
        rootPredicates += PredicateBuilder().apply(block).buildAnyOf()
    }

    /**
     * Logical AND: matches facts that satisfy all predicates in [block].
     * Must contain at least one predicate. [FactFilter.Predicate.Last] and
     * [FactFilter.Predicate.First] may not appear as direct children — place
     * them inside [anyOf] instead.
     */
    fun allOf(block: PredicateBuilder.() -> Unit) {
        rootPredicates += PredicateBuilder().apply(block).buildAllOf()
    }

    /**
     * Selects the [n] most recent facts matching the predicate in [block].
     * [n] must be ≥ 1. The block must add exactly the inner predicate;
     * multiple calls are implicitly ANDed.
     */
    fun last(n: Int, block: PredicateBuilder.() -> Unit) {
        rootPredicates += PredicateBuilder().apply(block).buildLast(n)
    }

    /**
     * Selects the [n] oldest facts matching the predicate in [block].
     * [n] must be ≥ 1. The block must add exactly the inner predicate;
     * multiple calls are implicitly ANDed.
     */
    fun first(n: Int, block: PredicateBuilder.() -> Unit) {
        rootPredicates += PredicateBuilder().apply(block).buildFirst(n)
    }

    @PublishedApi
    internal fun build(): FactQuery {
        val filter: FactFilter = when (rootPredicates.size) {
            0 -> FactFilter.All
            1 -> rootPredicates[0]
            else -> FactFilter.Predicate.AllOf(rootPredicates.toList())
        }
        return FactQuery(
            storeName = storeName,
            filter = filter.withNormalizedBoundedSelectors(),
            limit = limit,
            direction = direction,
            cursor = cursor,
        )
    }
}

/**
 * Builds a nested predicate inside an [anyOf], [allOf], [last], or [first] block.
 *
 * @author Domenic Cassisi
 */
@FactQueryDsl
class PredicateBuilder @PublishedApi internal constructor() {

    private val predicates = mutableListOf<FactFilter.Predicate>()

    /** @see FactQueryBuilder.subject */
    fun subject(value: String) {
        predicates += FactFilter.Predicate.Subject(Subject(value))
    }

    /** @see FactQueryBuilder.subjectPrefix */
    fun subjectPrefix(prefix: String) {
        predicates += FactFilter.Predicate.SubjectPrefix(prefix)
    }

    /** @see FactQueryBuilder.type */
    fun type(value: String) {
        predicates += FactFilter.Predicate.Type(FactType(value))
    }

    /** @see FactQueryBuilder.tag */
    fun tag(key: String, value: String) {
        predicates += FactFilter.Predicate.Tag(TagKey(key), TagValue(value))
    }

    /** @see FactQueryBuilder.metadata */
    fun metadata(key: String, value: String) {
        predicates += FactFilter.Predicate.Metadata(key, value)
    }

    /** @see FactQueryBuilder.timeRange */
    fun timeRange(range: TimeRange) {
        predicates += FactFilter.Predicate.TimeRange(range)
    }

    /** @see FactQueryBuilder.anyOf */
    fun anyOf(block: PredicateBuilder.() -> Unit) {
        predicates += PredicateBuilder().apply(block).buildAnyOf()
    }

    /** @see FactQueryBuilder.allOf */
    fun allOf(block: PredicateBuilder.() -> Unit) {
        predicates += PredicateBuilder().apply(block).buildAllOf()
    }

    /** @see FactQueryBuilder.last */
    fun last(n: Int, block: PredicateBuilder.() -> Unit) {
        predicates += PredicateBuilder().apply(block).buildLast(n)
    }

    /** @see FactQueryBuilder.first */
    fun first(n: Int, block: PredicateBuilder.() -> Unit) {
        predicates += PredicateBuilder().apply(block).buildFirst(n)
    }

    internal fun buildAnyOf(): FactFilter.Predicate {
        require(predicates.isNotEmpty()) { "anyOf must contain at least one predicate" }
        return if (predicates.size == 1) predicates[0]
               else FactFilter.Predicate.AnyOf(predicates.toList())
    }

    internal fun buildAllOf(): FactFilter.Predicate {
        require(predicates.isNotEmpty()) { "allOf must contain at least one predicate" }
        require(predicates.none { it is FactFilter.Predicate.Last || it is FactFilter.Predicate.First }) {
            "Last/First may not be a direct child of allOf — place them inside anyOf instead"
        }
        return if (predicates.size == 1) predicates[0]
               else FactFilter.Predicate.AllOf(predicates.toList())
    }

    internal fun buildLast(n: Int): FactFilter.Predicate {
        require(n >= 1) { "n must be at least 1, got $n" }
        require(predicates.isNotEmpty()) { "last must contain at least one predicate" }
        val inner = if (predicates.size == 1) predicates[0]
                    else FactFilter.Predicate.AllOf(predicates.toList())
        require(inner !is FactFilter.Predicate.Last && inner !is FactFilter.Predicate.First) {
            "Last/First must not directly wrap another Last/First"
        }
        return FactFilter.Predicate.Last(n, inner)
    }

    internal fun buildFirst(n: Int): FactFilter.Predicate {
        require(n >= 1) { "n must be at least 1, got $n" }
        require(predicates.isNotEmpty()) { "first must contain at least one predicate" }
        val inner = if (predicates.size == 1) predicates[0]
                    else FactFilter.Predicate.AllOf(predicates.toList())
        require(inner !is FactFilter.Predicate.Last && inner !is FactFilter.Predicate.First) {
            "Last/First must not directly wrap another Last/First"
        }
        return FactFilter.Predicate.First(n, inner)
    }
}

/**
 * Builds a [FactQuery] for [storeName] using the [FactQueryBuilder] DSL.
 *
 * ```kotlin
 * // All facts for a subject, newest first
 * val q = factQuery(storeName) {
 *     subject("order-123")
 *     direction = ReadDirection.Backward
 *     limit = Limit.of(50)
 * }
 *
 * // DCB pattern: OR of AND clauses
 * val q = factQuery(storeName) {
 *     anyOf {
 *         allOf { subject("order-123"); type("OrderStarted") }
 *         allOf { type("PaymentReceived"); tag("orderId", "123") }
 *     }
 * }
 *
 * // State-machine current-state reconstruction
 * val q = factQuery(storeName) {
 *     subject("machine-1")
 *     anyOf {
 *         type("Initiated")
 *         type("ShutDown")
 *         last(1) { type("Activated") }
 *         last(1) { type("DeActivated") }
 *     }
 * }
 * ```
 */
inline fun factQuery(storeName: StoreName, block: FactQueryBuilder.() -> Unit): FactQuery =
    FactQueryBuilder(storeName).apply(block).build()
