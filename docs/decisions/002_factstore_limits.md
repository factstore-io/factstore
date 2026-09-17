# 002 — FactStore limits

**Date:** 2026-09-10
**Status:** Accepted

## Summary

- FactStore limits what a client may append. The limits are defined **once, in the specification**,
  and are identical for every storage backend.
- They are chosen from **event-sourcing practice** — small facts, few tags — not from what a
  particular backend can store.
- They are enforced **when a request object is constructed**, so an invalid request cannot exist
  and no backend can forget a check.
- Limits may be **relaxed later, never restricted**: stored facts are re-validated when they are
  read, so tightening a rule would make existing facts unreadable.

## Limits

| What | Limit | Why |
|---|---|---|
| Text values of a fact (`Subject`, `FactType`, `TagKey`, `TagValue`, `MetadataKey`, `MetadataValue`) | ASCII letters, digits and `. _ : / -`; starts and ends with a letter or digit | One rule rules out Unicode collisions, control characters and invisible duplicates such as `"order/1 "` |
| `Subject`, `FactType`, `TagValue`, `MetadataValue` | 256 characters | Holds real identifiers — a UUID is 36, `tenant/<uuid>/order/<uuid>` is 86 |
| `TagKey`, `MetadataKey` | 128 characters | Keys name a dimension (`course`, `correlationId`) and are short by nature |
| `TagValue`, `MetadataValue` | may be empty | An empty value marks presence only, such as `archived` |
| `StoreName` | 256 characters; letters, digits, `_` and `-`, starting with a letter and ending with a letter or digit | Appears in URLs and the CLI, so it keeps its stricter pattern |
| Payload | 64 KiB (65,536 bytes), may be empty | Events are small; larger data belongs in a blob store, with a reference in the fact |
| Tags per fact | 5 | An aggregate stream needs one tag and DCB a few more; a fact needing more usually wants splitting into one fact per entity |
| Metadata entries per fact | 20 | Not indexed, so an entry costs only its bytes, and middleware adds entries too |
| Tags required by a query (`TagTypeItem`, `TagOnlyQueryItem`, `FindByTagsRequest`) | 1 to 5 | A query needing more tags than a fact can carry never matches; one without tags is not a tag query |
| Whole fact | no separate limit; at most 75,712 bytes through its parts | Every part is already limited; measured at 75,806 bytes encoded for FoundationDB, about 24 kB under its 100 kB value limit |
| Facts per append | 1 to 512 | An append records the facts of one decision, usually a handful; 512 leaves ample room while keeping every append a bounded unit of work |
| Total size of an append | 1 MiB (1,048,576 bytes): the payloads and text values of all its facts | Room for 16 maximum-size payloads, or 512 facts of about 2 KiB each; counted from what the client sends, so a client can compute it |

The size of an append is counted in UTF-8 bytes. The per-value lengths are counted in characters,
which equals bytes only because the text values are ASCII; widening the character set would require
counting those in bytes as well.

## How the limits are applied

- **Validation in `init`.** Each type checks what it can see: the text values check themselves,
  `FactInput` checks its tag and metadata counts, `AppendRequest` checks its number of facts and their
  total size, and the query types check their tags. A violation
  is an `IllegalArgumentException`, like any other invalid value in the specification.
- **An exception, not an `AppendResult`.** Results describe outcomes that depend on the store's
  state, such as a violated condition. A limit violation is known before the call and identical on
  every retry: the request was never valid.
- **Trimming at the server edge.** Surrounding whitespace is removed in one place,
  `server/input/ClientInput.kt`, before values are constructed. The value types and their `toX()`
  helpers stay strict, so a stored fact is exactly what was appended.
- **Metadata is not indexed.** The metadata index was written but never read, and has been removed.
  Tags select facts; metadata describes them.

## Alternatives considered

| Alternative | Why not |
|---|---|
| Limits per backend | Clients could not be written portably; the same append could succeed on one deployment and fail on another |
| Configurable limits | Rules out validation at construction, since a request cannot know its deployment's settings; raising fixed limits later stays compatible |
| Limits per store | Largest API surface, and every client would have to look up limits per store |
| Measure the serialized size | Unpredictable for clients, different per backend, and it ties the API to an encoding |
| Count only payload bytes toward an append's size | Tags and metadata could add megabytes outside the limit |
| Add a fixed per-fact overhead to the size | Clients would have to know a constant for bytes they never send |
| A separate limit for the whole fact | Adds a rule without protecting anything: every part of a fact is already limited |
| Allow larger facts, split across storage keys | Every backend would need splitting and reassembly in its write, read and streaming paths, for data that belongs in a blob store |
| `AppendResult.LimitExceeded` | Every backend would have to perform the check; a limit violation is an invalid request, not an outcome |
| Restrict names, allow any text in values (as Prometheus, HTTP and Kafka do) | Likely the better long-term rule, and deferred rather than rejected: widening later is safe, narrowing is not |
| Normalize whitespace inside the value types | The store would persist something other than what the client sent |
| More tags per fact (dcb-layer, a DCB implementation on FoundationDB, allows 10) | Not needed by today's models; raising the limit later is compatible |

## Consequences

- Non-Latin text such as `city=München` is rejected until the character set is widened.
- Payloads over 64 KiB are rejected; the documented answer is to store the data elsewhere and
  append a reference.
- A gRPC `FindFactsByTags` call with no tags is now an error.
- Until violations are mapped to `400 Bad Request`, they surface as HTTP 500 and generic gRPC
  errors.
- After 1.0, these limits are part of the storage contract.

## Open

- **Append condition complexity:** nesting is left unlimited for now, since `AppendCondition.All`
  may be removed; limits on query items and types per query item are undecided.
