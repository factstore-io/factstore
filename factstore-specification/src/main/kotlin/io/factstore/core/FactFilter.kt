package io.factstore.core

/**
 * Describes which facts to match in a [FactQuery].
 *
 * The hierarchy has two distinct levels. [All] is the outer level and matches
 * every fact in the store without applying any predicate. [Predicate] is the
 * nestable inner level and can be freely composed using [Predicate.AnyOf] and
 * [Predicate.AllOf]. [All] cannot appear inside a composite, eliminating
 * "match-everything-nested-inside-an-AND" at compile time.
 *
 * Construct instances via the [factQuery] DSL rather than the data classes
 * directly — the DSL validates structural constraints and normalises the tree.
 *
 * @author Domenic Cassisi
 */
sealed interface FactFilter {

    /** Matches every fact in the store — no predicate applied. */
    data object All : FactFilter

    /**
     * A composable predicate that evaluates against individual facts.
     *
     * All nodes are nestable. [AnyOf] and [AllOf] accept only other [Predicate]
     * instances, so [All] can never appear inside a composite.
     */
    sealed interface Predicate : FactFilter {

        // ── Leaf predicates ───────────────────────────────────────────────

        /** Matches facts whose [Fact.subject] equals [value]. */
        data class Subject(val value: io.factstore.core.Subject) : Predicate

        /** Matches facts whose [Fact.subject] value starts with [prefix]. */
        data class SubjectPrefix(val prefix: String) : Predicate

        /** Matches facts whose [Fact.type] equals [value]. */
        data class Type(val value: FactType) : Predicate

        /** Matches facts that carry the tag [key]=[value]. */
        data class Tag(val key: TagKey, val value: TagValue) : Predicate

        /** Matches facts whose [Fact.metadata] contains the entry [key]=[value]. */
        data class Metadata(val key: String, val value: String) : Predicate

        /** Matches facts whose [Fact.appendedAt] falls within [range]. */
        data class TimeRange(val range: io.factstore.core.TimeRange) : Predicate

        // ── Composite predicates ──────────────────────────────────────────

        /**
         * Logical OR: a fact matches if it satisfies at least one child predicate.
         *
         * These invariants are enforced at construction time — regardless of whether
         * the tree is built via the [factQuery] DSL or directly (e.g. by a gRPC/HTTP
         * request converter) — so no caller can construct a degenerate tree.
         *
         * @throws IllegalArgumentException if [predicates] is empty
         */
        data class AnyOf(val predicates: List<Predicate>) : Predicate {
            init {
                require(predicates.isNotEmpty()) { "AnyOf must contain at least one predicate" }
            }
        }

        /**
         * Logical AND: a fact matches only if it satisfies every child predicate.
         *
         * @throws IllegalArgumentException if [predicates] is empty, or if any child
         *         is a direct [First]/[Last] — a bounded selector cannot be meaningfully
         *         intersected with other predicates at the same level; place it inside
         *         [AnyOf] instead.
         */
        data class AllOf(val predicates: List<Predicate>) : Predicate {
            init {
                require(predicates.isNotEmpty()) { "AllOf must contain at least one predicate" }
                require(predicates.none { it is First || it is Last }) {
                    "First/Last may not be a direct child of AllOf — place it inside AnyOf instead"
                }
            }
        }

        // ── Bounded selectors ─────────────────────────────────────────────

        /**
         * The [n] oldest facts matching [predicate], selected before merging.
         *
         * This is a per-branch bound operating at the selection stage of the
         * three-stage pipeline (selection → merge → delivery). It is orthogonal
         * to [FactQuery.limit] and [FactQuery.direction], which apply globally
         * after the merge.
         *
         * @throws IllegalArgumentException if [n] is not positive, or if [predicate]
         *         is itself a [First]/[Last] — nesting bounded selectors is ambiguous.
         */
        data class First(val n: Int, val predicate: Predicate) : Predicate {
            init {
                require(n >= 1) { "n must be at least 1, got $n" }
                require(predicate !is First && predicate !is Last) {
                    "First/Last must not directly wrap another First/Last"
                }
            }
        }

        /**
         * The [n] most recent facts matching [predicate], selected before merging.
         *
         * @see First
         */
        data class Last(val n: Int, val predicate: Predicate) : Predicate {
            init {
                require(n >= 1) { "n must be at least 1, got $n" }
                require(predicate !is First && predicate !is Last) {
                    "First/Last must not directly wrap another First/Last"
                }
            }
        }
    }
}

/**
 * Returns a normalised copy of this filter in which every [FactFilter.Predicate.Last]
 * and [FactFilter.Predicate.First] node has had ancestor [FactFilter.Predicate.AllOf]
 * leaf constraints injected directly into its inner predicate.
 *
 * After normalisation every bounded selector is self-contained: the query executor
 * never needs to traverse ancestor nodes to determine the full constraint set for an
 * index scan. This is called automatically by the [factQuery] DSL builder.
 *
 * Example — before normalisation:
 * ```
 * AllOf(Subject("machine-1"), AnyOf(Last(1, Type("Activated"))))
 * ```
 * After normalisation:
 * ```
 * AllOf(Subject("machine-1"), AnyOf(Last(1, AllOf(Subject("machine-1"), Type("Activated")))))
 * ```
 */
fun FactFilter.withNormalizedBoundedSelectors(): FactFilter = when (this) {
    is FactFilter.All -> this
    is FactFilter.Predicate -> normalize(emptyList())
}

/** Maximum nesting depth allowed in a [FactFilter.Predicate] tree. See [validateComplexity]. */
const val MAX_FACT_FILTER_DEPTH = 5

/** Maximum number of children allowed under a single [FactFilter.Predicate.AnyOf]/[FactFilter.Predicate.AllOf]. */
const val MAX_FACT_FILTER_BRANCHES = 50

/**
 * Validates that this filter tree does not exceed [MAX_FACT_FILTER_DEPTH] nesting
 * levels or [MAX_FACT_FILTER_BRANCHES] children per [FactFilter.Predicate.AnyOf]/
 * [FactFilter.Predicate.AllOf].
 *
 * The per-node structural invariants (non-empty composites, positive `n`, no nested
 * bounded selectors) are already enforced unconditionally by the [FactFilter.Predicate]
 * constructors themselves. This function covers the two *whole-tree* properties that
 * cannot be checked node-by-node, and exists specifically to bound trees that arrive
 * from an untrusted source (gRPC/HTTP) rather than the [factQuery] DSL — a client
 * could otherwise submit an arbitrarily deep or wide tree and drive pathological
 * amounts of index-scan fan-out.
 *
 * @throws IllegalArgumentException if either limit is exceeded
 */
fun FactFilter.validateComplexity() {
    if (this is FactFilter.Predicate) validateComplexity(depth = 1)
}

private fun FactFilter.Predicate.validateComplexity(depth: Int) {
    require(depth <= MAX_FACT_FILTER_DEPTH) {
        "FactFilter nesting depth exceeds the maximum of $MAX_FACT_FILTER_DEPTH"
    }
    when (this) {
        is FactFilter.Predicate.Subject,
        is FactFilter.Predicate.SubjectPrefix,
        is FactFilter.Predicate.Type,
        is FactFilter.Predicate.Tag,
        is FactFilter.Predicate.Metadata,
        is FactFilter.Predicate.TimeRange -> Unit

        is FactFilter.Predicate.AnyOf -> {
            require(predicates.size <= MAX_FACT_FILTER_BRANCHES) {
                "AnyOf branch count (${predicates.size}) exceeds the maximum of $MAX_FACT_FILTER_BRANCHES"
            }
            predicates.forEach { it.validateComplexity(depth + 1) }
        }

        is FactFilter.Predicate.AllOf -> {
            require(predicates.size <= MAX_FACT_FILTER_BRANCHES) {
                "AllOf branch count (${predicates.size}) exceeds the maximum of $MAX_FACT_FILTER_BRANCHES"
            }
            predicates.forEach { it.validateComplexity(depth + 1) }
        }

        is FactFilter.Predicate.First -> predicate.validateComplexity(depth + 1)
        is FactFilter.Predicate.Last -> predicate.validateComplexity(depth + 1)
    }
}

/**
 * Returns `true` if this predicate matches the given [fact].
 *
 * For [FactFilter.Predicate.First] and [FactFilter.Predicate.Last] the evaluation
 * delegates to the inner predicate — the bounding semantics (selecting the first/last
 * *n* from a collection) are handled at the query-executor level, not at the
 * single-fact level.
 */
fun FactFilter.Predicate.matches(fact: Fact): Boolean = when (this) {
    is FactFilter.Predicate.Subject -> fact.subject == value
    is FactFilter.Predicate.SubjectPrefix -> fact.subject.value.startsWith(prefix)
    is FactFilter.Predicate.Type -> fact.type == value
    is FactFilter.Predicate.Tag -> fact.tags[key] == value
    is FactFilter.Predicate.Metadata -> fact.metadata[key] == value
    is FactFilter.Predicate.TimeRange -> fact.appendedAt in range
    is FactFilter.Predicate.AnyOf -> predicates.any { it.matches(fact) }
    is FactFilter.Predicate.AllOf -> predicates.all { it.matches(fact) }
    is FactFilter.Predicate.First -> predicate.matches(fact)
    is FactFilter.Predicate.Last -> predicate.matches(fact)
}

// ── Internal tree normalisation ───────────────────────────────────────────────

private fun FactFilter.Predicate.normalize(
    ancestorLeafConstraints: List<FactFilter.Predicate>,
): FactFilter.Predicate = when (this) {
    // Leaf predicates are returned unchanged — they carry no nested structure.
    is FactFilter.Predicate.Subject,
    is FactFilter.Predicate.SubjectPrefix,
    is FactFilter.Predicate.Type,
    is FactFilter.Predicate.Tag,
    is FactFilter.Predicate.Metadata,
    is FactFilter.Predicate.TimeRange -> this

    is FactFilter.Predicate.AllOf -> {
        // Collect the leaf predicates at this AllOf level and add them to the
        // set of constraints that any nested Last/First should inherit.
        val leafsHere = predicates.filter { it.isLeaf() }
        val enriched = ancestorLeafConstraints + leafsHere
        FactFilter.Predicate.AllOf(predicates.map { it.normalize(enriched) })
    }

    is FactFilter.Predicate.AnyOf ->
        // Pass ancestors down so Last/First children can receive them.
        FactFilter.Predicate.AnyOf(predicates.map { it.normalize(ancestorLeafConstraints) })

    is FactFilter.Predicate.Last -> {
        val injectedInner =
            if (ancestorLeafConstraints.isEmpty()) predicate
            else FactFilter.Predicate.AllOf(ancestorLeafConstraints + predicate)
        FactFilter.Predicate.Last(n, injectedInner.normalize(emptyList()))
    }

    is FactFilter.Predicate.First -> {
        val injectedInner =
            if (ancestorLeafConstraints.isEmpty()) predicate
            else FactFilter.Predicate.AllOf(ancestorLeafConstraints + predicate)
        FactFilter.Predicate.First(n, injectedInner.normalize(emptyList()))
    }
}

private fun FactFilter.Predicate.isLeaf(): Boolean = when (this) {
    is FactFilter.Predicate.Subject,
    is FactFilter.Predicate.SubjectPrefix,
    is FactFilter.Predicate.Type,
    is FactFilter.Predicate.Tag,
    is FactFilter.Predicate.Metadata,
    is FactFilter.Predicate.TimeRange -> true
    is FactFilter.Predicate.AnyOf,
    is FactFilter.Predicate.AllOf,
    is FactFilter.Predicate.First,
    is FactFilter.Predicate.Last -> false
}
