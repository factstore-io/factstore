# 003 — Error handling

**Date:** 2026-09-17
**Status:** Accepted

## Summary

- A failure is classified by **when** it happens, not by what was thrown: anything that fails while
  client input is being parsed is the client's fault; anything that fails afterwards is ours.
- The specification keeps signalling invalid values with `require` (`IllegalArgumentException`).
  The server turns failures during parsing into `InvalidInputException`.
- Each transport translates failures in **one place**, and every HTTP error is an `ApiError`.
- Domain outcomes — store not found, condition violated — remain sealed results.

## Categories

| Category | Example | HTTP | gRPC |
|---|---|---|---|
| Domain outcome | store not found, condition violated | 404 / 409, from the sealed result | `oneof` outcome |
| Invalid input | 6 tags, malformed UUID or JSON | 400 `InvalidInput` | `INVALID_ARGUMENT` |
| Unexpected | a backend failure or a bug | 500 `InternalError`, logged with an ID | `UNKNOWN` |

## Decisions

| What | Decision | Why |
|---|---|---|
| Specification | Plain `require` | Kotlin's convention for invalid arguments; the specification cannot know whether a value came from a client |
| Parsing | Each transport's converters (`toDomainRequest()`) parse inside `parseInput { }`, which catches `IllegalArgumentException` and `DateTimeParseException` only; endpoints then execute and render | Invalid input comes from many throwers (`require`, `UUID.fromString`, `Instant.parse`); the phase covers all of them, and because execution starts only after a converter returns, a bug in a backend still ends up as a 500 |
| Parameters | Values that need interpreting arrive as `String` and are converted while parsing | Jakarta REST answers a failed query or path parameter conversion with 404 |
| HTTP | One `ErrorMappers` class: invalid input and malformed JSON → 400 (naming the offending field, never an internal class), `WebApplicationException` → its own status and headers, anything else → 500 | Every error has a JSON body; framework statuses such as 404 and 415 keep their meaning |
| gRPC | Converters parse inside `parseRequest { }`, which throws `StatusException(INVALID_ARGUMENT)` | Coroutine services bypass Quarkus's default exception handler and report every exception as `UNKNOWN` without a description |
| Validation rules | Only in the specification; request classes carry no bean validation, and document limits with `@Schema` using the specification's constants | One source of truth, while the generated OpenAPI still shows the limits |
| Error codes | `ApiError.reason` is the contract; messages are for humans | Clients branch on codes, never on text |
| Clients | The Kotlin client maps `INVALID_ARGUMENT` to its own exception; the CLI prints its message | Callers can tell a bad request from a server failure |

## Alternatives considered

| Alternative | Why not |
|---|---|
| A typed exception in the specification | Malformed UUIDs and timestamps would still need translating at the edge — two mechanisms instead of one |
| `Either` or `Result` in the specification | Changes every signature and adds a dependency to a public module |
| An `AppendResult` variant for invalid input | Cannot express a failure raised while a value is constructed |
| Mapping every `IllegalArgumentException` to 400 | Reports bugs in the backends as the client's fault |
| RFC 9457 problem details | Worth considering later; `ApiError` is kept for now |

## Consequences

- Every converter must parse inside `parseInput` or `parseRequest`. A test per endpoint and
  transport sends invalid input to catch one that does not.
- Invalid input never reaches a store.
- A bug in parsing code would be reported as a 400. Parsing code is small and unit-tested.
- The unused `FactStoreException` is removed.

## Deferred

- Reporting backend unavailability as 503 or `UNAVAILABLE`, which needs a backend-independent signal.
- Failures in the middle of a stream.
- Field-level error details.
