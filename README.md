# FactStore

A lightweight, Kotlin-first event store designed for clarity, correctness, and extensibility — built on a clean specification and powered by FoundationDB. 🚀

## 📚 Overview

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
└── factstore-server/             # HTTP API server to expose FactStore
```

## 🎯 Why FactStore?

Event sourcing is a powerful architectural approach, but many existing solutions implicitly enforce a single model early on — typically stream- or aggregate-centric — making it harder to adapt when systems grow, boundaries shift, or new access patterns emerge.

_FactStore_ explores a more explicit and flexible foundation: instead of baking assumptions into the storage layer, it focuses on clear semantics, explicit consistency rules, and well-defined append behavior. Consistency boundaries are expressed per operation, not fixed globally, which enables both traditional event-sourced designs and more dynamic approaches such as dynamic consistency boundaries (DCB).

The goal of FactStore is not to replace existing event stores, but to provide a small, principled core that makes correctness concerns (idempotency, conditional writes, atomicity) explicit and allows different event-sourcing strategies to coexist and evolve over time.

## 🚀 Getting Started

*Using the Specification*

Add the spec module as a dependency and implement the interfaces if you want a custom backend.

*Using the FoundationDB Implementation*

Add the FoundationDB-backed store to integrate a production-ready FactStore into your application.

Please see [factstore.io](https://factstore.io) for more details.

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
