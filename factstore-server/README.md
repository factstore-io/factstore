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
curl -X POST http://localhost:8080/v1/stores/default/facts \
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
curl http://localhost:8080/v1/stores/default/facts/2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11
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

### 3. Retrieve Facts by Subject

Retrieve all facts for a specific subject.

```bash
curl http://localhost:8080/v1/stores/default/subjects/user:123/facts
```

Response:

```json
[
  {
    "factId": "2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11",
    "type": "UserRegistered",
    ...
  }
]
```

### 4. Retrieve Facts in Time Range

Retrieve all facts between two timestamps.

```bash
curl "http://localhost:8080/v1/stores/default/facts?from=2026-01-01T00:00:00Z&to=2026-02-01T00:00:00Z"
```

If `to` is omitted, there is no upper bound — all facts from `from` onwards are returned.

### 5. Stream Facts (Server-Sent Events)

Stream facts continuously using Server-Sent Events (SSE).

```bash
curl http://localhost:8080/v1/stores/default/facts/stream
```

Resume Streaming After a Specific Fact

```bash
curl "http://localhost:8080/v1/stores/default/facts/stream?after=2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11"
```

Example SSE Output:

```
data: {"factId":"...","type":"UserRegistered",...}

data: {"factId":"...","type":"UserEmailUpdated",...}
```

The connection remains open and emits new facts as they are appended.

#### Streaming Semantics

- Facts are emitted in storage order.
- The `after` parameter allows resuming from a known fact ID. 
- Streaming uses `text/event-stream` (SSE). 
- Each event contains one serialized Fact.

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
  -proto factstore-server/src/main/proto/factstore.proto \
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

#### FindFactsBySubject

```bash
grpcurl -plaintext \
  -d '{"store_name": "orders", "subject": "order-42", "limit": 50, "direction": "FORWARD"}' \
  localhost:8080 io.factstore.server.grpc.FactService/FindFactsBySubject
```

Outcomes: `found` (with facts list, possibly empty) · `store_not_found`

`direction` is `FORWARD` (oldest first, default) or `BACKWARD` (newest first). `limit` of `0` means no limit.

#### FindFactsByTags

```bash
grpcurl -plaintext \
  -d '{"store_name": "orders", "tags": {"region": "eu"}}' \
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
    "to":   "2026-02-01T00:00:00Z"
  }' \
  localhost:8080 io.factstore.server.grpc.FactService/FindFactsInTimeRange
```

Both `from` (inclusive) and `to` (exclusive) are optional. Omitting both returns all facts. Outcomes: `found` · `store_not_found`

#### StreamFacts

Opens a long-lived server-side stream. grpcurl prints each fact as it arrives.

```bash
# Replay all existing facts, then receive new ones as they are appended
grpcurl -plaintext \
  -d '{"store_name": "orders"}' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFacts

# Receive only facts appended after the stream is opened
grpcurl -plaintext \
  -d '{"store_name": "orders", "from_end": {}}' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFacts

# Resume from a known fact ID
grpcurl -plaintext \
  -d '{"store_name": "orders", "after_fact_id": "<uuid>"}' \
  localhost:8080 io.factstore.server.grpc.FactService/StreamFacts
```

Store-not-found and unknown `after_fact_id` are signalled as a `NOT_FOUND` gRPC status, not a response message field.

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
