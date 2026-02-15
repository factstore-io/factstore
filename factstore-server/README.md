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
/v1/stores/{factStoreName}
```

In the examples below, we use
```
factStoreName = default
baseUrl       = http://localhost:8080
```

### 1. Append Facts

Append one or more facts to a store 

```bash
curl -X POST http://localhost:8080/v1/stores/default/facts/append \
  -H "Content-Type: application/json" \
  -d '{
    "facts": [
      {
        "subject": {
          "type": "user",
          "id": "123"
        },
        "type": "UserRegistered",
        "payload": {
          "data": "SGVsbG8gd29ybGQ="
        },
        "tags": ["user", "registration"]
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
  "factId": "2f4d6f2c-6a3e-4a77-8c6b-0c3f6c2e5e11",
  "subject": {
    "type": "user",
    "id": "123"
  },
  "type": "UserRegistered",
  "payload": {
    "data": "SGVsbG8gd29ybGQ="
  },
  "tags": ["user", "registration"],
  "appendedAt": "2026-02-14T12:34:56Z"
}
```

### 3. Retrieve Facts by Subject

Retrieve all facts for a specific subject.

```bash
curl http://localhost:8080/v1/stores/default/subjects/user/123/facts
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

If to is omitted, the current time is used.

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
