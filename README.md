<p align="center">
  <img src="https://factstore.io/img/factstore-banner.png" alt="FactStore" width="100%">
</p>

# FactStore

A lightweight, Kotlin-first event store designed for clarity, correctness, and extensibility — built on a clean specification and powered by FoundationDB. 🚀

> [!WARNING]  
> FactStore is still under development and should not be used in production!

## 🚀 Getting Started

The simplest way to explore FactStore is to run `factstore-server` as a container:

```bash
docker run --rm -p 8080:8080 ghcr.io/factstore-io/factstore-server:main
```

Open <http://localhost:8080> for the web explorer, or append your first fact from the
command line:

```bash
# 1. Create a store
curl -X POST http://localhost:8080/api/v1/stores \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders"}'

# 2. Append a fact. FactStore treats payloads as opaque bytes, so the JSON API
#    carries them base64-encoded.
PAYLOAD=$(printf '{"orderId":"12345","total":42.50}' | base64)

curl -X POST http://localhost:8080/api/v1/stores/orders/facts \
  -H 'Content-Type: application/json' \
  -d "{\"facts\": [{
        \"type\": \"ORDER_PLACED\",
        \"subject\": \"order/12345\",
        \"payload\": {\"data\": \"$PAYLOAD\"},
        \"tags\": {\"region\": \"eu\"}
      }]}"
```

That returns the id of the fact you just appended:

```json
{"factIds":["5dd94f99-e094-449b-b957-69a8354ee9a1"],"appendedAt":"2026-08-02T12:38:14Z"}
```

Now read it back — either by its subject, or by any tag it carries:

```bash
# By subject. Subjects usually contain slashes, so URL-encode them ("/" -> %2F)
curl http://localhost:8080/api/v1/stores/orders/subjects/order%2F12345/facts

# By tag, in key=value form ("=" -> %3D)
curl "http://localhost:8080/api/v1/stores/orders/facts?tag=region%3Deu"
```

Reading by *subject* is classic stream-per-entity event sourcing; reading by *tag* cuts
across subjects. Being able to do both against the same facts is the point of FactStore —
see [Why FactStore?](#-why-factstore) below.

Useful endpoints:

| Endpoint | Purpose |
|---|---|
| `/` | Web explorer UI |
| `/api/v1` | REST API |
| `/q/openapi` | OpenAPI document |
| `/q/health` | Health check |

Without further configuration, `factstore-server` uses a simple in-memory storage implementation,
which is sufficient for exploring the solution. Just note that data won't survive restarts.

### Persistent storage

To keep your data, run FactStore against a [FoundationDB](https://www.foundationdb.org/)
cluster. The included Compose file starts both, and configures the cluster for you:

```bash
docker compose -f deploy/docker-compose.yml up
```

The server is available at <http://localhost:8080> as before, this time backed by
FoundationDB, so facts survive a restart. To stop the stack and discard the data again:

```bash
docker compose -f deploy/docker-compose.yml down -v
```

If you already run a FoundationDB cluster, point the server at it directly instead:

```bash
docker run --rm -p 8080:8080 \
  -e FACTSTORE_STORAGE_TYPE=foundationdb \
  -v /path/to/fdb.cluster:/etc/foundationdb/fdb.cluster \
  ghcr.io/factstore-io/factstore-server:main
```

Please see [factstore.io](https://factstore.io) for more details.

## 📚 Modules

FactStore is a modular event-sourcing system with two main parts:

### `factstore-specification`

This subproject defines the core contracts and behavioral rules for a FactStore implementation.
It is written in Kotlin and establishes the APIs for:

- Appending events (also called facts)
- Reading streams of facts
- Subscribing to live or historical streams
- Managing event ordering and consistency guarantees

This module is intentionally implementation-agnostic, serving as the foundation for any backend.

### `factstore-foundationdb`

An implementation of the specification using FoundationDB as the storage engine.
This module provides:
- A stateless event-sourcing layer built on FoundationDB’s transactional model 
- Strong consistency and ordered event writes
- Efficient range reads and streams
- A clean separation between domain logic and storage mechanics

If you want a production-ready FactStore backed by FoundationDB, this is the module for you.

## ✨ Features

FactStore is a specification and implementation of a fact/event store with a strong focus on correctness, flexibility, and explicit semantics.

### Core concepts

- Append-only fact storage with well-defined semantics
- Supports different event sourcing strategies
- Clear separation between API/specification and storage implementations
- Stateless design (the store itself does not hold application state)

### Append & consistency model

- Idempotent append operations: Safe retries using explicit idempotency keys — a crucial property in distributed systems.
- Conditional appends: Append facts only if explicit conditions are met (e.g. expected last fact per subject).
- First-class consistency boundaries: Support both:
  - traditional aggregate / stream-based event sourcing
  - more flexible models such as dynamic consistency boundaries (DCB), where consistency is expressed per operation rather than fixed upfront

### Query & access patterns

- Subject-based reads: Read facts for a specific subject or stream.
- Tag-based queries: Append and query facts based on tags, enabling cross-stream and non-aggregate-centric access patterns.
- Streaming support: Consume facts as ordered streams.

### FoundationDB-backed implementation

- Production-oriented FoundationDB implementation: Uses FoundationDB transactions to guarantee atomicity and isolation.
- Exactly-once semantics (logical): Achieved through transactional idempotency checks.
- Backend-driven correctness: Leverages FoundationDB’s transactional model instead of reimplementing coordination logic.

### Extensibility & evolution

- Backend-agnostic API: Designed to allow additional storage implementations in the future.
- Extensible condition model: New consistency or validation rules can be added without changing the core append API.
- Designed for long-lived systems: Explicit modeling of retries, partial failures, and evolving consistency needs.


## 🧱 Project Structure
```
factstore/
├── factstore-specification/      # Core APIs and contracts
├── factstore-foundationdb/       # FoundationDB-backed implementation
├── factstore-memory/             # In-memory-backed implementation
├── factstore-testing/            # Specification tests shared across implementations
└── factstore-server/             # HTTP API server to expose FactStore
```

## 🎯 Why FactStore?

Event sourcing is a powerful architectural approach, but many existing solutions implicitly enforce a single model early on — typically stream- or aggregate-centric — making it harder to adapt when systems grow, boundaries shift, or new access patterns emerge.

_FactStore_ explores a more explicit and flexible foundation: instead of baking assumptions into the storage layer, it focuses on clear semantics, explicit consistency rules, and well-defined append behavior. Consistency boundaries are expressed per operation, not fixed globally, which enables both traditional event-sourced designs and more dynamic approaches such as dynamic consistency boundaries (DCB).

The goal of FactStore is not to replace existing event stores, but to provide a small, principled core that makes correctness concerns (idempotency, conditional writes, atomicity) explicit and allows different event-sourcing strategies to coexist and evolve over time.

## 🤝 Contributing

Issues, ideas, and contributions are welcome!
Whether you want to improve the specification, suggest a new backend, or optimize the existing implementation, feel free to open a PR.

## 📜 License

FactStore is currently licensed under the Apache License 2.0.

The author may offer alternative commercial licensing options for
future versions of FactStore. Commercial licenses would provide
the right to use FactStore in production environments without the
obligations of the open-source license.

No commercial license is required at this time.
