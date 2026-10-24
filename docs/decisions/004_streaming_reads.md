# 004 — Streaming reads and subscriptions

**Date:** 2026-10-24, recording decisions made between 2026-09-24 and 2026-10-11
**Status:** Accepted

## Summary

- Every read is a **stream**. A bounded read is pinned to the store's head when it is opened; a
  subscription follows the tail and never ends.
- HTTP carries both as **NDJSON**, one single-key object per line. A bounded stream is complete
  only when it closes with an `end` line.
- A stream **continues after a fact ID**, the same cursor for bounded reads and subscriptions.
- The HTTP read surface is **two endpoints**, `GET /facts` for one filter and `POST /facts:query`
  for several, plus `GET /facts:subscribe`.
- A subscription is **one operation** and takes no filters for now.

## Bounded streams

| What | Decision | Why |
|---|---|---|
| Extent | A stream resolves the store and its head in one consistent read when it is opened, and emits the matching facts up to that head | Bounded, repeatable and independent of the consumer's pace; a stream that followed the tail would never terminate reliably under load |
| Laziness | Cold: nothing is read until the stream is collected, and every collection emits the same facts | A stream can be handed around and retried like a value. The cursor tree of a multi-tag read is built inside the flow, since one built at call time was used up by the first collection |
| Direction | Required in the specification and in gRPC; defaults to `forward` at the HTTP edge | The specification has no natural default for a backward read, while a URL without `direction` is the common case |
| Limits | At most 10 filters per query, 10 subjects and 10 types per filter, and 5 tags | A query expresses one decision's interest, which covers a handful of entities; reading wider is a projection or an export, which streams the store instead. Chosen from modelling practice and widened later if needed, never narrowed |

## Continuing a stream

| What | Decision | Why |
|---|---|---|
| Cursor | `continueAfter` takes a **fact ID**, exclusive and in reading order | The consumer already holds it, it is readable in a support conversation, and a stale one is detectable as `ContinuationNotFound` |
| `head` in the result | Not returned | The last processed fact answered every use case found |
| An ordered position | Not exposed | It would be the only way to compare checkpoints, and still not answer "how many facts behind": FoundationDB versionstamps are commit versions, not counts, and dense sequence numbers would cost a read on every append. Lag belongs in a separate operation |
| Replay | Removed; `continueAfter = null` replays from the start, `continueAfter = X` resumes | One mechanism instead of two, and unlike replay it also works on filtered streams |

## HTTP surface

| What | Decision | Why |
|---|---|---|
| Format | NDJSON: one `{"fact":…}` per line, closed by `{"end":{"count":n}}` or `{"error":{…}}` | A long history can be processed while it arrives. A stream without a closing line is incomplete, whatever the transport reported |
| Endpoints | `GET /facts` with repeatable `subject`, `type` and `tag` parameters for **one filter**; `POST /facts:query` for a query of several | A query string is flat and cannot express OR across filters. The resource picks the cheapest specification operation for what was asked, so the URL shape does not dictate the read |
| Sub-resources | `/subjects/{subject}/facts` and `/types/{type}/facts` removed | A third way to say the same thing, and subjects and types may contain slashes, which in a path segment means `%2F` and the mercy of proxy normalisation |
| Operations | Separated from their resource by a colon, as in `/facts:query` | An operation can never collide with an identifier, and unlike a custom HTTP method it still appears in OpenAPI |

gRPC needs no terminator: its trailers carry the final status.

## Subscriptions

| What | Decision | Why |
|---|---|---|
| Shape | One operation emitting `Flow<Fact>`, resuming on `continueAfter`, starting at `beginning`, `end` or after a fact | The same vocabulary as bounded streams, without mirroring all five stream operations: a subscription is long-lived, so every filtered path it adds has to stay correct under a head watch |
| Filters | None; a subscription delivers every fact of the store | Filtered subscriptions were built and removed again: they read the tail and discarded what did not match, so `from=beginning` on a large store decoded every fact to find a few. When filters return they go on the same operation and endpoint as one filter; a POST form for a query of several would be purely additive |
| HTTP transport | NDJSON on `GET /facts:subscribe`, with a `heartbeat` line every 20 s while idle and no `end` line | SSE's reconnect and `Last-Event-ID` are free only in a browser, while `continueAfter` resumes any client in the vocabulary it already knows. Every HTTP read now has one shape |
| gRPC transport | Batches are sent with what is ready; a batch is never waited for | Draining history fills batches, while following a tail sends each fact as it arrives instead of holding it back |

## Alternatives considered

| Alternative | Why not |
|---|---|
| Following the tail in bounded reads | Never terminates reliably under load, and a slow consumer would see a different result than a fast one |
| An opaque cursor token | Not readable in support, and the consumer would have to store something besides the fact it processed |
| Server-Sent Events | Their benefits exist only in a browser, they need their own terminator for bounded reads, and they gave HTTP two stream shapes |
| A custom HTTP method such as `QUERY` | Poorly supported by proxies and clients, and invisible in OpenAPI |
| One subscription operation per stream operation | Five long-lived paths to keep correct under a head watch, for a feature without a use case yet |
| Reading by time range | Ordered by the server's clock, which can disagree with the store's order across instances, and it invites timestamps as cursors. If it returns, it is "start streaming from about time T", in store order |

## Consequences

- A client must treat a bounded HTTP stream without an `end` line as failed.
- The explorer writes its own reconnect loop for subscriptions, resuming on `continueAfter`.
- Some proxies treat `text/event-stream` more kindly than `application/x-ndjson`; the heartbeat
  keeps an idle subscription under their timeouts.
- Jakarta REST ignores unknown query parameters, so an old client passing the removed `from=` to
  `GET /facts`, or a filter to `GET /facts:subscribe`, silently gets every fact.

