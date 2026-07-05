# Unified Fact Query API — Design Proposal

**Status:** Draft  
**Author:** \[Lead Software Architect\]  
**Reviewers:** \[Head of Product\], \[Engineering Team\]  
**Last Updated:** 2026-06-24

---

## Executive Summary

FactStore's current query capabilities are fragmented: developers must choose between separate, purpose-built endpoints to find facts by subject, by tag, or by time range — but cannot combine these criteria in a single operation. When a query spans multiple dimensions (for example, all events of a specific type within a time window), callers are forced to over-fetch data and discard the unwanted portion on the client side. This is wasteful, error-prone, and does not scale.

This proposal introduces a **Unified Fact Query API**: a single, composable query operation that replaces the existing collection of find endpoints. Filters — by subject, event type, tags, and time range — can be freely combined using logical AND and OR operators, expressed through an idiomatic Kotlin DSL. Results are delivered as a **server-side stream** rather than a materialised list, ensuring that queries over large datasets consume constant server memory regardless of result size.

A key addition beyond simple filtering is the **`First`/`Last` predicate**: a per-branch bounded selector that retrieves the N oldest or N most recent facts matching a given sub-predicate. This unlocks efficient current-state reconstruction for event-sourced systems — for example, loading only the most recent activation and deactivation events for a state machine without retrieving its full history. This class of query is directly applicable to functional decision-making, Dynamic Consistency Boundary (DCB) patterns, and any domain where only the latest facts of certain types determine current behaviour.

The investment delivers four concrete outcomes. First, developer productivity increases: a single expressive query replaces multiple awkward workarounds, and projection rebuilds, audit trails, and UI queries all use the same mental model. Second, operational safety improves: the streaming transport eliminates a class of out-of-memory incidents that the current list-based API is structurally susceptible to at production data volumes. Third, the API gains longevity: the composable filter model does not need to grow as use cases evolve. Fourth, a clear roadmap of future predicates — metadata filtering, causation tracking, subject prefix matching, and payload-aware queries — positions FactStore as a competitive, extensible foundation for advanced event-driven architectures.

---

## 1. Background and Motivation

### 1.1 The FactStore query model today

FactStore currently exposes the following read operations for multi-fact retrieval:

| Operation | Filter criteria |
|---|---|
| `findBySubject` | subject only |
| `findByTags` | tags only (AND semantics) |
| `findByTagQuery` | tags with OR-of-AND semantics |
| `findInTimeRange` | time range only |

Each operation is a separate endpoint with its own request and response type, returning a fully materialised `List<Fact>`.

### 1.2 Limitations

**No cross-dimension filtering.** There is no way to retrieve facts that match a specific subject *and* a specific event type, or events matching a tag combination within a bounded time window. Callers work around this by over-fetching on one dimension and filtering in application code — an approach that breaks down at scale.

**Unbounded memory consumption.** Returning `List<Fact>` requires the server to hold the entire result set in memory before sending a response. A query over a large store or a subject with a long history can return hundreds of thousands of facts, putting the server at risk of out-of-memory failure. There is no structural safeguard — only optional, unenforced limits passed by callers.

**Combinatorial API growth.** Every new cross-dimension query need produces pressure for a new dedicated endpoint. With three current filter dimensions (subject, tags, time range) and a fourth (event type) partially accessible through `TagQuery`, the number of possible combinations is already unmanageable as distinct endpoints.

**Inconsistency with streaming operations.** The `ReplayFacts` and `SubscribeFacts` operations already deliver results as a stream of batches (`Flow<List<Fact>>`). The query operations use a different, less scalable transport model, creating two distinct paradigms for "read multiple facts from the store."

**No efficient current-state queries.** Reconstructing the current state of an event-sourced entity requires loading its entire event history, even when only the last occurrence of certain event types is needed. There is no mechanism to bound the result per event type within a single query.

---

## 2. Proposed Design

### 2.1 A two-level filter type hierarchy

All filter criteria are modelled as an immutable, serialisable **sealed type hierarchy** that the storage layer walks to determine the best execution strategy.

The hierarchy has two distinct levels. The outer level distinguishes between "match everything" and "apply a predicate." The inner level contains the composable predicate tree. This separation is enforced by the type system: `FactFilter.All` cannot appear inside a composite predicate, eliminating a class of degenerate inputs at compile time.

```kotlin
sealed interface FactFilter {
    /** Matches every fact in the store — no predicate applied. */
    data object All : FactFilter

    /**
     * A composable predicate that evaluates against individual facts.
     * All nodes in this hierarchy are nestable; [AnyOf] and [AllOf] accept
     * only other [Predicate] instances, so [All] can never appear inside
     * a composite.
     */
    sealed interface Predicate : FactFilter {

        // ── Leaf predicates ──────────────────────────────────────────────

        data class Subject(val value: io.factstore.core.Subject) : Predicate
        data class Type(val value: FactType) : Predicate
        data class Tag(val key: TagKey, val value: TagValue) : Predicate
        data class TimeRange(val range: io.factstore.core.TimeRange) : Predicate

        // ── Composite predicates ─────────────────────────────────────────

        /** Logical OR: a fact matches if it satisfies at least one child. */
        data class AnyOf(val predicates: List<Predicate>) : Predicate

        /** Logical AND: a fact matches only if it satisfies all children. */
        data class AllOf(val predicates: List<Predicate>) : Predicate

        // ── Bounded selectors ────────────────────────────────────────────

        /** The [n] oldest facts matching [predicate]. */
        data class First(val n: Int, val predicate: Predicate) : Predicate

        /** The [n] most recent facts matching [predicate]. */
        data class Last(val n: Int, val predicate: Predicate) : Predicate
    }
}
```

### 2.2 The query object

Pagination, ordering, and resumption are query-level concerns, separated cleanly from the filter:

```kotlin
data class FactQuery(
    val storeName: StoreName,
    val filter: FactFilter = FactFilter.All,
    val limit: Limit = Limit.None,
    val direction: ReadDirection = ReadDirection.Forward,
    val cursor: FactId? = null,
)
```

### 2.3 The three-stage query pipeline

Understanding the distinction between `First`/`Last`, `direction`, and `limit` is important because they are similar concepts operating at different stages of the pipeline:

```
1. SELECTION         →    2. MERGE           →    3. DELIVERY
   First / Last            versionstamp             direction
   per predicate branch    order across             + limit
                           all branches
```

| Concern | Controls | Scope |
|---|---|---|
| `First(n)` / `Last(n)` | Which facts to select from a branch | Per predicate branch, before merging |
| `direction` | Order in which the merged result is delivered | Global, after merging |
| `limit` | Maximum number of facts to deliver | Global delivery cap |

These are orthogonal. `Last(1) { type("Activated") }` selects the most recent `Activated` fact from the index. That fact then joins the merged stream and is delivered in whatever global order `direction` specifies. A `limit` at the query level caps the total delivered facts across all branches — it cannot bound individual branches.

**Key implication:** for a simple single-branch query, `limit = 5, direction = Backward` and `last(5) { ... }` produce the same result in the same order. The distinction matters only when multiple predicate branches are composed in `AnyOf` and per-branch bounds are needed.

### 2.4 Kotlin DSL

A type-safe builder constructs `FactQuery` instances with minimal boilerplate. `@DslMarker` prevents `limit`, `direction`, and `cursor` from being called inside filter blocks, where they would be meaningless.

```kotlin
// All facts for a subject, newest first
val q = factQuery(storeName) {
    subject("order-123")
    direction = ReadDirection.Backward
    limit = Limit.of(50)
}

// Subject + event type (implicit AND at root)
val q = factQuery(storeName) {
    subject("order-123")
    type("OrderPlaced")
}

// Multi-subject OR
val q = factQuery(storeName) {
    anyOf {
        subject("order-123")
        subject("order-456")
    }
}

// Tags within a time range
val q = factQuery(storeName) {
    tag("region", "eu")
    timeRange { from = Instant.now().minus(1, ChronoUnit.DAYS) }
}

// DCB pattern: OR of AND clauses
val q = factQuery(storeName) {
    anyOf {
        allOf {
            subject("order-123")
            type("OrderStarted")
        }
        allOf {
            type("PaymentReceived")
            tag("orderId", "123")
        }
    }
}

// State machine current-state reconstruction:
// all Initiated and ShutDown facts, plus only the most recent
// Activated and DeActivated facts — without loading full history
val q = factQuery(storeName) {
    subject("machine-1")
    anyOf {
        type("Initiated")
        type("ShutDown")
        last(1) { type("Activated") }
        last(1) { type("DeActivated") }
    }
    direction = ReadDirection.Forward
}

// Paginated — second page
val q = factQuery(storeName) {
    type("OrderPlaced")
    limit = Limit.of(20)
    cursor = lastSeenFactId
}
```

### 2.5 `First` and `Last` — semantics and constraints

**`First(n, predicate)`** scans the matching index from the oldest end, returning the `n` earliest facts that satisfy `predicate`.

**`Last(n, predicate)`** scans the matching index from the newest end, returning the `n` most recent facts that satisfy `predicate`.

In both cases, the selected facts join the merged result stream and are delivered in the global `direction` order. Selection direction and delivery direction are independent.

**Constraints enforced at build time:**
- `n` must be positive (`n ≥ 1`)
- `First`/`Last` must not directly wrap another `First`/`Last` — nesting bounded selectors produces semantically ambiguous results
- `First`/`Last` must not appear as a direct child of `AllOf` — `AllOf` is an intersection; a bounded selector produces a specific fact set that cannot meaningfully be intersected with other predicates at the same level

**Ancestor constraints propagate.** When `Last(1) { type("Activated") }` appears inside `anyOf { subject("machine-1"); ... }`, the root-level `subject` constraint applies to the bounded scan. The implementation must apply all ancestor `AllOf` constraints when executing a `First`/`Last` index scan.

This is implemented by normalization (`FactFilter.withNormalizedBoundedSelectors`), which rewrites the tree so every `First`/`Last` node's inner predicate has ancestor `AllOf` leaf constraints injected directly into it — self-contained, with no need for the executor to track ancestors during traversal. Each storage-layer `query()`/`IfNoneMatch` entry point calls this once, on every incoming `FactFilter`, rather than relying on callers to have done so: gRPC and HTTP requests build `FactFilter` trees directly from the wire and never touch the `factQuery` DSL, which is the only place that previously performed this step. Normalizing only at the DSL layer left every non-DSL caller — i.e. essentially all network traffic — silently vulnerable to ancestor constraints being dropped.

### 2.6 Streaming result

Query results are delivered as a stream of fact batches, consistent with the existing replay and subscribe operations:

```kotlin
sealed interface FactQueryResult {
    @JvmInline value class FactStream(val stream: Flow<List<Fact>>) : FactQueryResult
    @JvmInline value class StoreNotFound(val storeName: StoreName) : FactQueryResult
    @JvmInline value class CursorNotFound(val factId: FactId) : FactQueryResult
}
```

The domain interface reduces to a single method alongside the existing point-lookup operations:

```kotlin
interface FactFinder {
    suspend fun findById(request: FindByIdRequest): FindByIdResult
    suspend fun existsById(request: ExistsByIdRequest): ExistsByIdResult
    suspend fun query(query: FactQuery): FactQueryResult
}
```

---

## 3. Why Streaming

The choice to return `Flow<List<Fact>>` rather than `List<Fact>` is not merely stylistic — it is structurally necessary for production safety and long-term scalability.

**Memory safety is structural, not contractual.** A list-based response requires the server to buffer the entire result set before sending a single byte. With streaming, server memory consumption is bounded by batch size, not result cardinality. No configuration, no discipline, and no limit enforcement is needed — the constraint is architectural.

> **Current limitation.** This guarantee fully holds for `FactFilter.All` (`fullStoreStream`) and for `First`/`Last` bounded selectors, which now push their `n` bound and scan direction into the underlying FDB range read. It does **not** yet hold for an arbitrary `FactFilter.Predicate` query: `FdbFactQuerier` collects every matching `FactPosition` for the whole filter tree into memory before merge-sorting and applying `limit`, and — except where `First`/`Last` narrows a branch — does not push the query's `limit` down into the FDB range read for a plain predicate query (e.g. `type("X"); limit(10)` still scans the entire `type("X")` index). `FactPosition`s are much smaller than full `Fact`s, so this is materially better than materializing facts, but it is not the batch-bounded, limit-pushed-down guarantee this section otherwise describes. Closing this gap for the general (potentially multi-branch `AnyOf`) case requires a true incremental k-way merge across branches and is recommended follow-on work.



**Pagination is a UI concern, not a protocol concern.** Clients that need paginated display — such as the FactStore Explorer — implement keyset pagination using the `cursor` and `limit` parameters, without the API needing to carry cursor metadata in responses. The stream completing is the authoritative signal that no more results exist.

**Resumability is built-in.** Long-running consumers (projection rebuilds, exports) can checkpoint the last processed `FactId` and resume with a new query from that cursor if interrupted. The replay API already establishes this pattern.

**Consistency with the existing model.** `ReplayFacts` and `SubscribeFacts` already deliver `Flow<List<Fact>>`. A unified streaming transport for all multi-fact operations eliminates a two-paradigm API surface and allows infrastructure improvements (batching, backpressure, observability) to benefit all operations equally.

---

## 4. gRPC and HTTP Transport

### 4.1 gRPC

The new query operation is a **server-streaming RPC**, reusing the existing `StreamFactsResponse` envelope. The proto mirrors the two-level Kotlin type hierarchy exactly:

```protobuf
rpc QueryFacts(QueryFactsRequest) returns (stream StreamFactsResponse);

message QueryFactsRequest {
  string store_name       = 1;
  FactFilter filter       = 2;
  optional int32 limit    = 3;
  ReadDirection direction = 4;
  optional string cursor  = 5;  // fact ID; absent = no cursor
}

// Outer level: All or a predicate tree
message FactFilter {
  oneof kind {
    bool             all       = 1;
    PredicateFilter  predicate = 2;
  }
}

// Inner level: composable predicate (All cannot appear here)
message PredicateFilter {
  oneof kind {
    string          subject    = 1;
    string          type       = 2;
    TagFilter       tag        = 3;
    TimeRangeFilter time_range = 4;
    CompositeFilter any_of     = 5;
    CompositeFilter all_of     = 6;
    BoundedFilter   first      = 7;
    BoundedFilter   last       = 8;
  }
}

message CompositeFilter { repeated PredicateFilter predicates = 1; }
message BoundedFilter   { int32 n = 1; PredicateFilter predicate = 2; }
message TagFilter       { string key = 1; string value = 2; }
message TimeRangeFilter {
  optional google.protobuf.Timestamp from = 1;  // inclusive
  optional google.protobuf.Timestamp to   = 2;  // exclusive
}
```

Pre-stream errors (`StoreNotFound`, `CursorNotFound`) are delivered as the first and only message in the stream, consistent with the established pattern for replay and subscribe.

### 4.2 HTTP / SSE

The HTTP endpoint produces a **Server-Sent Events stream**, matching the existing `/facts/replay` and `/facts/subscribe` endpoints. Complex filter trees are expressed as a JSON body on a `POST` request; simple single-dimension queries may additionally support query parameters for ergonomics.

```
POST /v1/stores/{storeName}/facts/query
Content-Type: application/json
Accept: text/event-stream
```

---

## 5. Production Readiness Considerations

### 5.1 Validation

| Layer | What is validated |
|---|---|
| `FactFilter.Predicate` construction | `AnyOf` and `AllOf` must be non-empty; `First`/`Last` `n` must be positive; `First`/`Last` must not wrap another `First`/`Last`; `First`/`Last` must not appear as direct child of `AllOf` |
| `FactQuery` / `AppendCondition.IfNoneMatch` construction | Filter tree nesting depth ≤ 5; `AnyOf`/`AllOf` branch count ≤ 50 (`FactFilter.validateComplexity`) |
| DSL / build time | `TimeRange.from` must precede `TimeRange.to`; `Limit` must be positive |
| `FactFinder` boundary | Store exists; cursor fact exists |

The structural invariants (non-empty composites, positive `n`, no nested/direct-child bounded selectors) are enforced by the `FactFilter.Predicate` data classes themselves, not only by the `factQuery` DSL builder. This matters because gRPC and HTTP requests construct `FactFilter` trees directly from the wire, bypassing the DSL entirely — enforcing invariants only in the builder left that boundary completely unvalidated. The nesting-depth and branch-count limits are enforced the same way, at `FactQuery`/`IfNoneMatch` construction, so they apply uniformly regardless of the caller.

Structurally impossible queries (e.g., `AllOf` containing two `Subject` leaves — always false since a fact has exactly one subject) are not validation errors. They produce an empty stream. In the current implementation this falls out of the normal execution path — the storage layer picks one `Subject` as the index-scan key and evaluates the other as an in-application residual filter that can never match — rather than a dedicated pre-pass that detects and short-circuits contradictions before touching any index. A true pre-pass remains a possible future optimization but is not required for correctness.

### 5.2 Cursor semantics

The cursor identifies a fact by ID and represents a position in the global versionstamp-ordered log. It is always **exclusive**: the cursor fact itself is not included in results. Direction determines traversal:

- `Forward` + cursor → facts with versionstamp strictly after the cursor
- `Backward` + cursor → facts with versionstamp strictly before the cursor

The cursor is a log position, not a filter position. A cursor fact does not need to match the query's filter. `CursorNotFound` is returned if the referenced fact does not exist in the store. A cursor pointing to the final fact in the store returns an empty stream, not an error.

### 5.3 `First`/`Last` interaction with the cursor

The cursor scopes the entire query to a log position. `First`/`Last` selects from the end of the matching sequence within that scope. Together:

- `last(1)` + Forward cursor → the most recent matching fact **after** the cursor position. Requires scanning backward from the current head and stopping at the cursor boundary.
- `first(1)` + Forward cursor → the oldest matching fact after the cursor. Natural: scan forward from the cursor.
- `last(1)` + Backward cursor → the most recent matching fact before the cursor. Natural: scan backward from the cursor.

The storage layer must apply the cursor boundary when executing bounded scans, not only when scanning the global log.

### 5.4 `TimeRange` interaction with the cursor

When both a `TimeRange` predicate and a cursor are present, the effective query scope is the intersection of both constraints. If the cursor position falls outside the time range (e.g., the cursor versionstamp corresponds to a time after `TimeRange.to` in a Forward query), no facts will be returned. This is correct behaviour — an empty stream, not an error.

### 5.5 Result ordering

All results are delivered in strict **versionstamp order** regardless of how many index ranges the filter requires the storage layer to scan. For queries spanning multiple branches (multiple subjects, multiple types, multiple `AnyOf` children), the storage layer merge-sorts all contributions by versionstamp before emitting. This is a behavioural contract, not an implementation detail — it is what makes cursor-based pagination correct and predictable across pages.

### 5.6 Cross-page consistency

Each page (each query execution) operates on a snapshot at the time of execution. There is no snapshot guarantee across pages. Facts appended between page executions that fall after the cursor will appear on the next page. This is the correct behaviour for an append-only log and is consistent with replay and subscribe semantics.

### 5.7 Batch semantics

Batches emitted by the stream are always non-empty. An empty `List<Fact>` within a `Flow` is a bug in the implementation, not a valid signal. The stream completing is the only signal that no more results are available.

### 5.8 Upper bound on `n` in `First`/`Last`

`Last(n)` with a very large `n` drives a large internal bounded scan before merging. Unlike the global `limit` (which caps the output stream), a large `n` can cause significant index work. A server-side maximum (e.g., `n ≤ 10,000`) should be enforced and documented. Callers needing more than this threshold should use an unbounded predicate with a global `limit` instead.

As of this iteration, `n` (and the bound direction) is pushed down as a native FDB row limit and reverse-scan flag whenever the bounded selector's inner predicate resolves to a single index scan with no in-application residual filter — so `last(1) { type("Activated") }` is a genuine bounded reverse scan, not a full scan of the `Activated` index followed by an in-application sort and truncation. This makes a very large `n` primarily a *result-set-size* concern rather than a *full-scan* one, but the maximum-`n` cap described above is still unimplemented and still recommended.

### 5.9 Query complexity limits

The filter tree is recursive and could be arbitrarily wide or deep without bounds. Two server-side limits are enforced:

- **Maximum nesting depth ≤ 5.** Prevents pathological trees and reduces parsing overhead. Sufficient for all known use cases including DCB patterns, which require at most two levels (an `AnyOf` wrapping multiple `AllOf` clauses).
- **Maximum branch count ≤ 50 per `AnyOf` or `AllOf`.** A single `AnyOf` with hundreds of branches drives hundreds of parallel index scans. A practical cap prevents accidental resource exhaustion.

### 5.10 Store lifecycle during active queries

If a store is deleted while a streaming query is running, the stream surfaces this as a gRPC status error (terminal failure), consistent with the mid-stream error convention established by replay and subscribe. The storage layer must detect store absence at each transaction boundary within a long-running query and fail cleanly rather than producing partial results silently.

### 5.11 Query observability

For debugging slow or expensive queries in production, the query executor should emit structured logs or traces covering:

- Which indexes were scanned to satisfy the filter tree
- Facts scanned vs. facts returned (selectivity ratio per branch)
- Execution duration per stage (index scans, merge, delivery)

This information belongs in operational telemetry, not in the query response itself.

### 5.12 Serialisability of `FactQuery`

The immutable data class structure makes `FactQuery` naturally serialisable. Two concrete applications:

- **Saved queries in the Explorer** — users build a query in the UI and save it for reuse
- **Scheduled/recurring jobs** — background jobs that run the same filter on a schedule

Both require verifying that `FactFilter` round-trips correctly through JSON and proto, including recursive `AnyOf`/`AllOf` and `First`/`Last` nodes.

### 5.13 Proto schema evolution

The recursive `PredicateFilter` message is a new proto type. Adding new variants to the `oneof kind` is backward compatible — existing clients receive an unrecognised variant and should handle this gracefully rather than crashing. Removing or renaming existing variants is a breaking change. The `First`/`Last` fields (7 and 8) must never be repurposed.

### 5.14 Clock skew in `TimeRange` queries

`TimeRange` filters on `appended_at`, which is the wall-clock timestamp assigned at append time. In a multi-node deployment, clock skew means facts appended "at the same time" may have non-monotonic timestamps even though their versionstamps are globally ordered. A `TimeRange` query may miss facts timestamped slightly outside the requested window due to skew. The versionstamp is the authoritative ordering; `appended_at` is informational and not guaranteed monotonic across nodes. This should be explicitly documented.

---

## 6. What This Replaces

### 6.1 Query operations

The existing `findBySubject`, `findByTags`, `findByTagQuery`, and `findInTimeRange` operations are superseded by `query` and are now marked `@Deprecated`, allowing existing callers to migrate at their own pace. The `findById` and `existsById` point-lookup operations are unaffected.

Note: as of this iteration these are deprecated *signals*, not yet deprecated *wrappers* — each still has its own independent storage-layer implementation rather than delegating to `query()`. Consolidating them onto the `query()` execution path (and, more broadly, unifying the three separate `FactFilter` tree-walkers that currently exist across `FdbFactQuerier`, `MemoryFactStore`, and `FdbFactAppender`'s `IfNoneMatch` evaluation) is recommended follow-on work: today, a fix to predicate-evaluation semantics has to be found and applied independently in each of those three places.

The existing `TagQuery` structure (used in `findByTagQuery`) is superseded for query purposes. `FactFilter.Predicate` is a strict superset: any `TagQuery` expression is representable as a `FactFilter` tree, with the addition of subject, time range, and `First`/`Last` as first-class dimensions.

### 6.2 `AppendCondition` evolution

The `TagQueryBasedCondition` in `AppendCondition` currently uses `TagQuery` — OR-of-AND tag/type combinations. Now that `FactFilter.Predicate` is more expressive, the append condition should adopt the same predicate model in a future iteration:

```kotlin
// Current: tags and types only
condition = AppendCondition.IfExists(tagQuery = TagQuery(...))

// Future: full predicate expressiveness, including subject
condition = AppendCondition.IfExists(filter = allOf {
    subject("machine-1")
    type("Initiated")
})
```

This eliminates two separate filter languages for what is conceptually the same operation: "do any facts matching this pattern exist?" Aligning them reduces cognitive overhead and opens DCB conditions to subject-scoped checks, which are currently impossible.

### 6.3 Filtered subscriptions and replay

`SubscribeFacts` and `ReplayFacts` currently stream all facts from a start position with no filter capability. Adding an optional `FactFilter` to both is a natural extension of this work — callers subscribe to or replay only the facts relevant to their projection, avoiding client-side discard of irrelevant events. This is deferred to a follow-on iteration but should be designed with the same `FactFilter` type from the outset.

---

## 7. Open Questions

| Question | Owner | Notes |
|---|---|---|
| ~~Should the builder normalise the filter tree?~~ | Architecture | **Resolved:** normalization (bounded-selector ancestor-constraint injection) happens at each storage-layer entry point (`FdbFactQuerier.query`, `MemoryFactStore.query`, `IfNoneMatch` evaluation), not the DSL builder — the DSL still normalises too (cheap, idempotent), but the storage layer no longer depends on callers having done so. This was necessary rather than optional: gRPC/HTTP requests build `FactFilter` trees directly and never go through the DSL, so builder-only normalisation left ancestor constraints silently dropped for those callers. |
| Should the HTTP API support query parameters for simple cases? | Architecture | Ergonomics benefit for browser/curl use. JSON body covers all cases; query params would be a shortcut layer only. |
| What is the maximum supported `n` in `First`/`Last`? | Architecture | Suggested cap: 10,000, still unenforced. Partially mitigated: `n` is now pushed down as a native FDB row limit for the common (no in-app residual filter) case, so a large `n` is a result-set-size cost rather than a full-scan cost — but the cap itself is not yet implemented. |
| Should filtered subscriptions and replay be in-scope for this iteration? | Product | High value; adds implementation surface. Likely a fast follow given shared infrastructure. |

---

## 8. Summary

| Concern | Current state | After this change |
|---|---|---|
| Cross-dimension filtering | Not supported | Any combination of subject, type, tags, time range |
| Server memory safety | Unbounded — structural risk | Bounded by batch size — structural guarantee |
| API surface | One endpoint per filter combination | One composable endpoint |
| Consistency with replay/subscribe | Different transport model | Unified streaming model |
| DCB query support | Partial (tags + type only, no subject) | Full (subject + type + tags + time range) |
| Pagination/resumption | Not supported for queries | Cursor-based, direction-aware |
| Current-state reconstruction | Requires loading full history | `First`/`Last` per branch — efficient bounded selection |
| `AppendCondition` expressiveness | Tags and types only | Subject-scoped DCB conditions (follow-on) |
| Filtered subscriptions/replay | Not supported | Planned follow-on using the same `FactFilter` |
| Existing callers | Unaffected immediately | Deprecated wrappers preserve compatibility |

---

## 9. Future Roadmap

The following predicate types and features were originally scoped as follow-on work. **9.1 (Metadata predicates) and 9.3 (Subject prefix matching) were pulled into this iteration and have shipped** — both are implemented as `FactFilter.Predicate.Metadata` and `FactFilter.Predicate.SubjectPrefix`, indexed the same way as tags and subjects respectively. They are left here (with their original rationale) rather than deleted, since the "why" is still useful context; the rest of this section remains genuine future work, recorded to inform storage layer and API design decisions that should not inadvertently close these options off.

### 9.1 Metadata predicates — shipped

The `metadata` map already exists on every `Fact` and is now indexed and queryable the same way tags are:

```kotlin
metadata("correlationId", "order-flow-abc123")
metadata("causationId", "cmd-456")
```

Tags describe the business domain of a fact; metadata describes its operational context (trace IDs, request IDs, actor IDs). The distinction is semantic — the implementation is identical.

### 9.2 Causation and correlation as first-class predicates

An elevation of metadata predicates to named, first-class concepts. If FactStore tracks causal chains explicitly:

```kotlin
causedBy("fact-id-xyz")        // all facts directly caused by this fact
correlatedWith("flow-abc123")  // all facts in the same business transaction
```

This enables "give me the entire causal subtree of this command" — directly applicable to distributed tracing, debugging, and audit trails. Requires either indexed reserved metadata keys or dedicated `causationId`/`correlationId` fields on `Fact`.

**Effort:** Medium. **Value:** High. **Suggested timing:** Medium-term.

### 9.3 Subject prefix matching — shipped

Hierarchically namespaced subjects (`machine/plant-A/unit-3/machine-42`) are common in event-sourced systems. Prefix matching unlocks range-scoped queries without enumerating every subject:

```kotlin
subjectPrefix("machine/plant-A/")
```

Implemented as a raw byte-prefix range over the subject index (`SubjectIndexSubspace.prefixRange`): the prefix string is tuple-packed, its trailing null terminator is stripped, and `ByteArrayUtil.strinc` supplies the exclusive upper bound. This is **not** the same as calling `Subspace.range()`/`Tuple.range()` on a partial string — that matches tuples having the string as an exact, complete element, not a substring prefix, because tuple-encoded strings are null-terminated. A subject index scan built the naive way (`subspace.range(Tuple.from(storeId, prefix))`) silently returns nothing for any subject longer than the prefix itself.

### 9.4 Relative time predicates

`TimeRange` requires absolute timestamps, forcing callers to compute time arithmetic. A relative variant resolves at query execution time:

```kotlin
withinLast(Duration.ofDays(7))
withinLast(Duration.ofHours(1))
```

Thin convenience over `TimeRange`. Particularly useful for monitoring, alerting, and recurring queries.

**Effort:** Very low. **Value:** Medium. **Suggested timing:** Soon after initial release.

### 9.5 Last-per-group (deduplication per group)

An extension of `First`/`Last` where the grouping key is a fact attribute such as type, subject, or a tag value:

```kotlin
lastPerType()                  // last fact of each distinct type
lastPerSubject()               // last fact for each distinct subject
firstPerTag("customerId")      // first fact per customer
```

Enables efficient current-state queries without a projection layer: "what is the current status of every machine in the fleet?" Requires either a specialised index or a streaming group-by in the query executor.

**Effort:** High. **Value:** Very high. **Suggested timing:** Medium-term.

### 9.6 Transaction / batch awareness

Facts appended in a single `AppendFacts` call share a commit versionstamp. A batch predicate retrieves all facts committed atomically together:

```kotlin
sameTransaction(factId = "some-fact-id")
```

Useful for audit trails: "give me the full effect of this command" — all facts written in the same atomic write.

**Effort:** Medium. **Value:** Medium. **Suggested timing:** Medium-term.

### 9.7 Sequence / version range predicates

Aggregates have an implicit sequence number — the Nth event for a given subject. A version range predicate enables efficient partial aggregate loading for subjects with long histories:

```kotlin
versionRange(subject = "order-123", from = 50, to = 100)
```

Requires a subject-scoped sequence counter in the data model.

**Effort:** Medium. **Value:** Medium. **Suggested timing:** Medium-term.

### 9.8 Existence / absence predicates

Correlated queries across subjects: "find all subjects that have an `Initiated` fact but no `ShutDown` fact." These are aggregate-level predicates that cannot be answered by scanning individual facts.

```kotlin
subjectsWithout(type = "ShutDown")
subjectsWhoseLast(type = "Activated")
```

Requires aggregation indexes or a maintained read model. Arguably better served by a dedicated projection feature than by a raw query predicate.

**Effort:** Very high. **Value:** High. **Suggested timing:** Long-term / projection feature.

### 9.9 Payload predicates

For inspectable payload formats (JSON, Avro, Protobuf with known schema), filter facts by field values within the payload:

```kotlin
payload {
    field("status") equalTo "ACTIVE"
    field("amount") greaterThan 1000
    field("currency") in setOf("EUR", "GBP")
}
```

Requires schema registry integration and a secondary index strategy per indexed field. The key design decision is whether payload field indexes are declared at store configuration time (efficient, less flexible) or at query time (flexible, requires full scans for unindexed fields).

**Effort:** Very high. **Value:** High (for the right use cases). **Suggested timing:** Long-term.

### 9.10 Type hierarchy / semantic grouping

Event types often form natural families. A type group predicate avoids enumerating every member in every query:

```kotlin
typeGroup("order-lifecycle")   // resolved via a registered type registry
typePrefix("Order")            // convention-based prefix matching
```

Requires either a naming convention (prefix-based, low cost) or an explicit type registry (higher cost, more powerful).

**Effort:** Medium–High. **Value:** Medium. **Suggested timing:** Long-term.

---

### Roadmap summary

| Feature | Value | Effort | When |
|---|---|---|---|
| Metadata predicates | High | Low | Soon |
| Subject prefix matching | High | Low | Soon |
| Relative time predicates | Medium | Very low | Soon |
| Causation / correlation | High | Medium | Medium-term |
| Last-per-group | Very high | High | Medium-term |
| Transaction / batch awareness | Medium | Medium | Medium-term |
| Sequence / version range | Medium | Medium | Medium-term |
| Filtered subscriptions + replay | High | Medium | Fast follow |
| `AppendCondition` + `FactFilter` | High | Medium | Fast follow |
| Existence / absence predicates | High | Very high | Long-term |
| Payload predicates | High | Very high | Long-term |
| Type hierarchy / grouping | Medium | High | Long-term |
