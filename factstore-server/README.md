# FactStore Server

The FactStore Server is the HTTP-accessible runtime for FactStore.
It exposes the FactStore core via a RESTful API and manages the underlying storage backend.

At the moment, the server supports FoundationDB as its storage engine.

> ⚠️ **Status:** The HTTP API is currently **experimental** and may change without notice.

---

## Overview

FactStore Server provides:

- Append-only fact persistence
- Ordered fact streaming
- Time-range and subject-based queries

The server is implemented using Quarkus and is designed to be:
- Lightweight
- Cloud-native friendly
- Configuration-driven
- Infrastructure-ready

---

## Storage Backend

Currently supported:
- **FoundationDB**

The server initializes a single global FoundationDB Database instance at startup and wires it into the FactStore core components.

> FactStore Server does not create a FoundationDB cluster for you! 

Future versions may support additional backends.

---

## Configuration

Configuration is provided via `application.yaml` (or environment variables).

```yaml
factstore:
  foundationdb:
    cluster-file: /etc/foundationdb/fdb.cluster
    api-version: 730
```

---

## HTTP API (Experimental)

The HTTP API exposes:
- Fact append operations
- Fact retrieval
- Fact streaming

⚠️ The API contract is currently experimental and may change in future versions.

It should not yet be considered stable for long-term external integrations.

All endpoints are namespaced under:
```
/v1/stores/{storeName}
```

In the examples below, we use
```
storeName = default
baseUrl       = http://localhost:8080
```

### 1. Append Facts

Append one or more facts to a store 

```bash
curl -X POST http://localhost:8080/api/v1/stores/default/facts \
  -H "Content-Type: application/json" \
  -d '{
    "facts": [
      {
        "type": "UserRegistered",
        "subject": "user:123",
        "payload": {
          "data": "SGVsbG8gd29ybGQ="
        },
        "tags": { "role": "user" }
      }
    ]
  }'
```

### 2. Retrieve Fact by ID

Retrieve a single fact by its ID.

```bash
curl http://localhost:8080/api/v1/stores/default/facts/2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11
```

Response (example):

```json
{
  "id": "2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11",
  "type": "UserRegistered",
  "subject": "user:123",
  "payload": {
    "data": "SGVsbG8gd29ybGQ="
  },
  "tags": { "role": "user" },
  "appendedAt": "2026-02-14T12:34:56Z"
}
```

### 3. Stream Facts by Subject

Stream all facts of a specific subject.

```bash
curl "http://localhost:8080/api/v1/stores/default/subjects/user:123/facts"
```

Reads of several facts respond with NDJSON (`application/x-ndjson`): one JSON object per line,
each with exactly one property.

```json
{"fact":{"id":"2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11","type":"UserRegistered",…}}
{"fact":{"id":"8a1c0e52-3f4b-4d6e-9a7c-2b5d8e1f3a90","type":"UserLocked",…}}
{"end":{"count":2}}
```

- A `fact` line carries a fact.
- The stream closes with an `end` line, holding the number of facts sent, or with an `error`
  line (an `ApiError`) if it failed part way.
- **A stream that stops without an `end` or `error` line is incomplete**, however the connection ended.
- A missing store or invalid input is answered before the stream starts, with an `ApiError`.

`direction` is `forward` (oldest first, the default) or `backward` (newest first); `limit` is optional.

The facts are those stored when the request is made: facts appended while the stream is
being read are not included.

#### Continuing a stream

`continueAfter` names the last fact a consumer processed, and the stream continues after it. This
is how a projection resumes after a restart, and how a client reads a long result page by page:

```bash
# The first page, and then the next one, continuing after the last fact of the first
curl "http://localhost:8080/api/v1/stores/default/subjects/user:123/facts?limit=100"
curl "http://localhost:8080/api/v1/stores/default/subjects/user:123/facts?limit=100&continueAfter=2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11"
```

- It is **exclusive**: the named fact is not streamed again.
- It follows the **reading order**: forward it continues with the facts appended after it, backward
  with those appended before it.
- **Any fact of the store** may be named, even one the stream does not emit — it marks a position,
  which is what lets a filtered stream resume from a checkpoint.
- A fact the store does not hold is answered with `404` and an `ApiError`, before the stream starts.

It works on every read: all facts, by subject, by type, by tags, and in the body of a query.

### 3b. Stream Facts by Type

Stream all facts of one type. The type is matched exactly, so `com.acme.OrderPlaced` is not
matched by `com.acme`.

```bash
curl "http://localhost:8080/api/v1/stores/default/types/UserRegistered/facts"
```

The response is an NDJSON fact stream, as for a subject.

### 3c. Stream Facts by Query

Stream the facts matching a query. A fact matches when it matches **any** of the filters; within a
filter, the properties that are set must **all** hold, and a property with several values matches
any of them. A fact matching several filters is streamed once.

```bash
curl -X POST "http://localhost:8080/api/v1/stores/default/facts:query" \
  -H 'Content-Type: application/json' \
  -d '{
        "filters": [
          { "subjects": ["user:123"] },
          { "types": ["UserRegistered", "UserLocked"], "tags": { "role": "user" } }
        ],
        "direction": "backward",
        "limit": 100,
        "continueAfter": "2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11"
      }'
```

The whole request lives in the body, including `direction`, `limit` and `continueAfter`. The colon separates the
operation from the resource it acts on, so it cannot be mistaken for a sub-resource. The response
is an NDJSON fact stream, as for a subject.

### 4. Stream Facts of a Store

Stream all facts of a store, or those carrying the given tags, or those appended in a time range.

```bash
curl "http://localhost:8080/api/v1/stores/default/facts?direction=backward&limit=10"
curl "http://localhost:8080/api/v1/stores/default/facts?tag=role%3Duser&tag=region%3Deu"
curl "http://localhost:8080/api/v1/stores/default/facts?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z"
```

The response is an NDJSON fact stream, as for a subject. A fact must carry **all** of the given
tags, so repeating `tag` narrows the result. In a time range, `from` is inclusive and `to`
exclusive; either may be omitted. Tags and a time range cannot be combined yet.

### 5. Subscribe to Facts (Server-Sent Events)

Subscribe to a store and stream facts continuously using Server-Sent Events (SSE).
A subscription drains the existing facts and then **stays open**, emitting new facts
as they are appended (a "catch-up subscription"). It never completes on its own —
the client disconnects when it is done.

```bash
curl http://localhost:8080/api/v1/stores/default/facts/subscribe
```

Start position (optional):

| Query param         | Behaviour                                             |
| ------------------- | ----------------------------------------------------- |
| _(none)_            | from the beginning of the store (default)             |
| `from=beginning`    | from the beginning of the store                       |
| `from=end`          | only facts appended **after** the subscription opens  |
| `after=<factId>`    | resume immediately after a known fact ID              |

```bash
# Resume after a specific fact, then keep following the live tail
curl "http://localhost:8080/api/v1/stores/default/facts/subscribe?after=2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11"
```

Example SSE Output (the connection stays open and keeps emitting):

```
data: {"factId":"...","type":"UserRegistered",...}

data: {"factId":"...","type":"UserEmailUpdated",...}
```

### 6. Replay Facts (Server-Sent Events)

Replay drains the existing facts **up to the head pinned at the moment the request
is received, then completes** — the SSE connection closes once the client has caught
up. Facts appended while the replay is running are excluded (a later replay will see
them). Use it for exports, projection rebuilds, and incremental batch jobs that need
a terminating read rather than a live tail.

```bash
# Replay everything currently in the store, then the stream ends
curl http://localhost:8080/api/v1/stores/default/facts/replay
```

Start position (optional):

| Query param      | Behaviour                                                        |
| ---------------- | ---------------------------------------------------------------- |
| _(none)_         | from the beginning of the store (default)                        |
| `after=<factId>` | only facts after a checkpoint, up to the pinned head (the delta) |

There is deliberately **no `from=end`** for replay: replaying from the end would
always be empty.

```bash
# Incremental replay: everything since the last processed fact, then exit
curl "http://localhost:8080/api/v1/stores/default/facts/replay?after=2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11"
```

**Resumable batch pattern:** persist the id of the last fact you processed, then on
the next run call `replay?after=<that id>`. Because every event carries its fact id,
a crashed run resumes exactly where it left off — no cursor bookkeeping required.

#### Streaming Semantics (subscribe & replay)

- Facts are emitted in storage order, one serialized Fact per SSE event.
- Both endpoints use `text/event-stream` (SSE).
- The `after` parameter resumes from a known fact ID.
- **Subscribe never completes**; it follows the live tail until the client disconnects.
- **Replay completes** once it reaches the head pinned when the request was received;
  facts appended during the replay are excluded.
- Pre-stream errors (store not found, unknown `after` cursor) are reported as an HTTP
  error response before the SSE stream begins.

---

## gRPC API (Experimental)

FactStore also exposes a gRPC API that covers the same operations as the HTTP API.
gRPC uses binary-encoded Protocol Buffers over HTTP/2, which makes it well-suited for service-to-service communication and high-throughput workloads.

> ⚠️ **Status:** The gRPC API is currently **experimental** and may change without notice.

### Connection

The gRPC server runs on the **same port as the HTTP server** (default `8080`), multiplexed over HTTP/2.

```
Host: localhost
Port: 8080
Protocol: plaintext HTTP/2 (h2c)
```

The full service definitions live in:

```
factstore-server/src/main/proto/factstore.proto
```

Pass the proto file directly to tools that need it (e.g. grpcurl in environments where reflection is disabled):

```bash
grpcurl -plaintext \
  -proto factstore-proto/src/main/proto/factstore/v1/factstore.proto \
  -d '{"name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.StoreService/CreateStore
```

---

### Services

| Service | Description |
|---|---|
| `InfoService` | Server metadata (version, storage backend) |
| `StoreService` | Create, inspect, and delete named stores |
| `FactService` | Append, query, and stream facts within a store |

---

### InfoService

#### GetServerInfo

Returns the running server version and active storage backend.

```bash
grpcurl -plaintext localhost:8080 io.factstore.server.grpc.InfoService/GetServerInfo
```

---

### StoreService

#### CreateStore

```bash
grpcurl -plaintext \
  -d '{"name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.StoreService/CreateStore
```

Outcomes: `created` (with UUID) · `name_already_exists`

#### GetStore

```bash
grpcurl -plaintext \
  -d '{"name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.StoreService/GetStore
```

Outcomes: `found` (with store info) · `not_found`

#### ListStores

```bash
grpcurl -plaintext localhost:8080 io.factstore.server.grpc.StoreService/ListStores
```

#### StoreExists

```bash
grpcurl -plaintext \
  -d '{"name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.StoreService/StoreExists
```

Outcomes: `present` · `absent`

#### DeleteStore

```bash
grpcurl -plaintext \
  -d '{"name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.StoreService/DeleteStore
```

Outcomes: `deleted` · `not_found`

---

### FactService

#### AppendFacts

`payload.data` must be base64-encoded bytes.

```bash
grpcurl -plaintext \
  -d '{
    "store_name": "orders",
    "facts": [{
      "type": "OrderCreated",
      "subject": "order-42",
      "payload": { "data": "eyJvcmRlcklkIjogNDJ9" },
      "tags": { "region": "eu" }
    }]
  }' \
  localhost:8080 io.factstore.server.grpc.FactService/AppendFacts
```

Outcomes: `appended` · `already_applied` · `condition_violated` · `duplicate_fact_ids` (with IDs) · `store_not_found`

An optional `idempotency_key` (UUID string) makes the operation safe to retry. An optional `condition` enables conditional appends — see the proto file for the available condition types.

#### GetFact

```bash
grpcurl -plaintext \
  -d '{"store_name": "orders", "fact_id": "<uuid>"}' \
  localhost:8080 io.factstore.server.grpc.FactService/GetFact
```

Outcomes: `found` (with fact) · `not_found` · `store_not_found`

#### FactExists

```bash
grpcurl -plaintext \
  -d '{"store_name": "orders", "fact_id": "<uuid>"}' \
  localhost:8080 io.factstore.server.grpc.FactService/FactExists
```

Outcomes: `present` · `absent` · `store_not_found`

#### StreamFactsByQuery

```bash
grpcurl -plaintext \
  -d '{
    "store_name": "orders",
    "direction": "READ_DIRECTION_FORWARD",
    "query": { "filters": [
      { "subjects": ["order-42"] },
      { "types": ["OrderPlaced"], "tags": { "region": "eu" } }
    ] }
  }' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFactsByQuery
```

A fact matches when it matches any filter, and is streamed once even if several match. A query
holds at most 10 filters, and a filter at most 10 subjects, 10 types and 5 tags.

#### StreamFacts, StreamFactsBySubject, StreamFactsByType and StreamFactsByTags

Stream the facts of a store, of one subject, of one type, or those carrying a set of tags, as they
are stored when the call is made, then complete.

```bash
grpcurl -plaintext \
  -d '{"store_name": "orders", "direction": "READ_DIRECTION_BACKWARD", "limit": 50}' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFacts

grpcurl -plaintext \
  -d '{"store_name": "orders", "subject": "order-42", "direction": "READ_DIRECTION_FORWARD"}' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFactsBySubject

grpcurl -plaintext \
  -d '{"store_name": "orders", "type": "OrderPlaced", "direction": "READ_DIRECTION_FORWARD"}' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFactsByType
```

```bash
grpcurl -plaintext \
  -d '{"store_name": "orders", "tags": {"region": "eu"}, "direction": "READ_DIRECTION_FORWARD"}' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFactsByTags
```

`StreamFactsByType` matches the type exactly. `StreamFactsByTags` requires a fact to carry every
given tag (AND semantics), and at least one tag must be given.

Each message carries a `batch` of facts, at most 1 MiB in size. A missing store is reported as a
single `store_not_found` message instead. The stream completes with status `OK` once every fact
was sent, and with an error status if it fails part way.

`direction` is required on every read: `READ_DIRECTION_FORWARD` (oldest first) or
`READ_DIRECTION_BACKWARD` (newest first). `limit` is optional and must be positive.

`continue_after_fact_id` continues a stream after a fact that was already processed: exclusive, in
reading order, and any fact of the store may be named. A fact the store does not hold is reported
as a single `continuation_not_found` message.

#### FindFactsByTags

```bash
grpcurl -plaintext \
  -d '{"store_name": "orders", "tags": {"region": "eu"}, "direction": "READ_DIRECTION_FORWARD"}' \
  localhost:8080 io.factstore.server.grpc.FactService/FindFactsByTags
```

All specified tags must match (AND semantics). Outcomes: `found` · `store_not_found`

#### QueryFacts

Supports compound tag queries with OR-of-AND semantics, optionally filtered by fact type.

```bash
grpcurl -plaintext \
  -d '{
    "store_name": "orders",
    "query": {
      "items": [
        { "tag_only": { "tags": { "region": "eu" } } },
        { "tag_type": { "types": ["OrderCreated"], "tags": { "env": "prod" } } }
      ]
    }
  }' \
  localhost:8080 io.factstore.server.grpc.FactService/QueryFacts
```

Outcomes: `found` · `store_not_found`

#### FindFactsInTimeRange

```bash
grpcurl -plaintext \
  -d '{
    "store_name": "orders",
    "from": "2026-01-01T00:00:00Z",
    "to":   "2026-02-01T00:00:00Z",
    "direction": "READ_DIRECTION_FORWARD"
  }' \
  localhost:8080 io.factstore.server.grpc.FactService/FindFactsInTimeRange
```

Both `from` (inclusive) and `to` (exclusive) are optional. Omitting both returns all facts. Outcomes: `found` · `store_not_found`

#### SubscribeFacts

Opens a long-lived server-side stream (catch-up subscription): existing facts first,
then new facts as they are appended. The stream never completes on its own.

```bash
# Catch up on all existing facts, then receive new ones as they are appended
grpcurl -plaintext \
  -d '{"store_name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.FactService/SubscribeFacts

# Receive only facts appended after the subscription is opened
grpcurl -plaintext \
  -d '{"store_name": "orders", "from_end": {}}' \
  localhost:8080 io.factstore.server.grpc.FactService/SubscribeFacts

# Resume from a known fact ID, then keep following
grpcurl -plaintext \
  -d '{"store_name": "orders", "after_fact_id": "<uuid>"}' \
  localhost:8080 io.factstore.server.grpc.FactService/SubscribeFacts
```

#### ReplayFacts

Opens a **bounded** server-side stream: existing facts up to the head pinned when the
call is received, then the stream **completes**. There is no `from_end` (it would
always be empty).

```bash
# Replay everything currently in the store, then the stream ends
grpcurl -plaintext \
  -d '{"store_name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.FactService/ReplayFacts

# Incremental replay: only facts after a checkpoint, up to the pinned head
grpcurl -plaintext \
  -d '{"store_name": "orders", "after_fact_id": "<uuid>"}' \
  localhost:8080 io.factstore.server.grpc.FactService/ReplayFacts
```

For both RPCs, store-not-found and unknown `after_fact_id` are signalled as a
`FAILED_PRECONDITION` gRPC status, not a response message field.

---

### Response model

Every operation returns a response message with a `oneof outcome` field. Each case is a named, typed message that carries exactly the data belonging to that outcome — there are no ambiguous optional fields.

For example, `AppendFactsResponse` looks like this conceptually:

```
outcome = appended            {}
        | already_applied     {}
        | condition_violated  {}
        | duplicate_fact_ids  { fact_ids: [string] }
        | store_not_found     { store_name: string }
```

Inspect the full definitions in the proto file to see all outcome types and their fields.
