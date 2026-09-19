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

### 3. Stream Facts of a Store

Stream all facts of a store, or those matching a subject, a type or tags.

```bash
# every fact, newest first
curl "http://localhost:8080/api/v1/stores/default/facts?direction=backward&limit=10"

# one subject, one type, or facts carrying tags
curl "http://localhost:8080/api/v1/stores/default/facts?subject=user:123"
curl "http://localhost:8080/api/v1/stores/default/facts?type=UserRegistered"
curl "http://localhost:8080/api/v1/stores/default/facts?tag=role%3Duser&tag=region%3Deu"

# combined: one filter, and repeated values match any of them
curl "http://localhost:8080/api/v1/stores/default/facts?type=UserRegistered&type=UserLocked&tag=role%3Duser"
```

`subject` and `type` match **any** of their values, while **all** tags must be carried, so the
parameters together describe one filter. A query of several filters needs a body, and is
`POST /facts:query` below.

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
curl "http://localhost:8080/api/v1/stores/default/facts?subject=user:123&limit=100"
curl "http://localhost:8080/api/v1/stores/default/facts?subject=user:123&limit=100&continueAfter=2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11"
```

- It is **exclusive**: the named fact is not streamed again.
- It follows the **reading order**: forward it continues with the facts appended after it, backward
  with those appended before it.
- **Any fact of the store** may be named, even one the stream does not emit — it marks a position,
  which is what lets a filtered stream resume from a checkpoint.
- A fact the store does not hold is answered with `404` and an `ApiError`, before the stream starts.

### 4. Stream Facts by Query

A query matches facts against several filters: a fact matches when it matches **any** of them, and
a fact matching several is streamed once. Within a filter, the properties that are set must **all**
hold, and a property with several values matches any of them — the same meaning the parameters of
`GET /facts` have, which is why one filter needs no body and several do.

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

The whole request lives in the body, including `direction`, `limit` and `continueAfter`. The colon
separates the operation from the resource it acts on, so it cannot be mistaken for a sub-resource;
it is part of the path and must not be percent-encoded. The response is an NDJSON fact stream, as
above.

A query holds at most 10 filters, and a filter at most 10 subjects, 10 types and 5 tags.

### 5. Subscribe to Facts

A subscription is the fact stream that does not stop: it drains the facts the store already
holds and then **stays open**, writing the facts appended next for as long as the client reads
(a "catch-up subscription"). Use it for live projections, read models and integrations; for a
read that ends, use the streams above.

```bash
curl -N "http://localhost:8080/api/v1/stores/default/facts:subscribe?from=end"
```

A subscription delivers **every** fact of the store. It takes no filters: reading a part of a
store is what the fact streams are for, and filtering a subscription efficiently means following
the indexes live, which is not built yet. Until it is, filter on the reading side.

Where to start (`from` and `continueAfter` cannot be combined):

| Query param             | Behaviour                                                   |
| ----------------------- | ----------------------------------------------------------- |
| _(none)_                | from the beginning of the store (default)                   |
| `from=beginning`        | from the beginning of the store                             |
| `from=end`              | only facts appended **after** the subscription opens        |
| `continueAfter=<factId>`| continue after a known fact, exclusive                      |

```bash
# Resume where a stopped subscription left off, then keep following
curl -N "http://localhost:8080/api/v1/stores/default/facts:subscribe?continueAfter=2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11"
```

#### Subscription semantics

The response is NDJSON, as the fact streams are, with two differences: there is **no `end`
line**, because a subscription does not end on its own, and an idle subscription writes a
`heartbeat` line every 20 seconds so a reader and the proxies in between can tell a quiet
store from a dead connection.

```
{"fact":{"id":"…","type":"shop.order.placed",…}}
{"heartbeat":{"at":"2026-09-24T19:12:50.063Z"}}
{"fact":{"id":"…","type":"shop.payment.captured",…}}
```

- Facts are written in the store's order, one `fact` line each.
- A `heartbeat` line carries the server's clock and says nothing about the store's facts.
- An `error` line ends a subscription that failed part way.
- A missing store, an unknown `continueAfter` or invalid input is answered **before** the
  stream starts, as an `ApiError`.
- Nothing is held back: a fact is written as soon as it is read, never waiting for a batch
  to fill.

**Resuming:** a connection can always drop — a proxy, a deploy, a laptop lid. Keep the id of
the last fact you processed and reconnect with `continueAfter=<that id>`: the subscription
continues exactly after it, so no fact is seen twice and none is skipped. This is the same
`continueAfter` the fact streams use, and it is the reason a subscription needs no transport
mechanics of its own.

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

#### SubscribeFacts

Opens a long-lived server-side stream (catch-up subscription): the facts the store already
holds first, then the facts appended next. The stream never completes on its own. Facts are
delivered in `FactBatch` messages which are sent as soon as they are ready, so draining a
history sends full batches while following the tail sends each fact as it arrives.

```bash
# Catch up on all existing facts, then receive new ones as they are appended
grpcurl -plaintext \
  -d '{"store_name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.FactService/SubscribeFacts

# Receive only facts appended after the subscription is opened
grpcurl -plaintext \
  -d '{"store_name": "orders", "from_end": {}}' \
  localhost:8080 io.factstore.server.grpc.FactService/SubscribeFacts

# Resume after a known fact, then keep following
grpcurl -plaintext \
  -d '{"store_name": "orders", "continue_after_fact_id": "<uuid>"}' \
  localhost:8080 io.factstore.server.grpc.FactService/SubscribeFacts
```

Every fact of the store is emitted; the RPC takes no query. Store-not-found and an unknown
`continue_after_fact_id` arrive as the first and only response message (`store_not_found` /
`continuation_not_found`), after which the stream completes normally; a failure once facts are
flowing is a gRPC status error.

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
