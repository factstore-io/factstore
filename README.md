# FactStore

A lightweight, Kotlin-first event store designed for clarity, correctness, and extensibility — built on a clean specification and powered by FoundationDB. 🚀

## 📚 Overview

FactStore is a modular event-sourcing system with two main parts:

### `fact-store-specification`

This subproject defines the core contracts and behavioral rules for a FactStore implementation.
It is written in Kotlin and establishes the APIs for:

- Appending events (also called facts)
- Reading streams of facts
- Subscribing to live or historical streams
- Managing event ordering and consistency guarantees

This module is intentionally implementation-agnostic, serving as the foundation for any backend.

### `fact-store-foundationdb`

An implementation of the specification using FoundationDB as the storage engine.
This module provides:
- A stateless event-sourcing layer built on FoundationDB’s transactional model 
- Strong consistency and ordered event writes
- Efficient range reads and streams
- A clean separation between domain logic and storage mechanics

If you want a production-ready FactStore backed by FoundationDB, this is the module for you.

## ✨ Features

- Clear contract-driven architecture
- Append-only fact storage
- Predictable event ordering
- Streaming APIs for real-time or catch-up subscribers
- Stateless FoundationDB implementation
- Kotlin-first, but compatible with the JVM ecosystem

## 🧱 Project Structure
```
factstore/
├── fact-store-specification/      # Core APIs and contracts
└── fact-store-foundationdb/       # FoundationDB-backed implementation
```

## 🎯 Why FactStore?

Event stores can become complex or overly opinionated. FactStore aims to stay:

- Minimal — The spec defines only what’s essential
- Predictable — Behavior is explicitly defined
- Extensible — Write your own backend if you want
- Fast & Reliable — The FoundationDB implementation ensures strong guarantees

## 🚀 Getting Started

*Using the Specification*

Add the spec module as a dependency and implement the interfaces if you want a custom backend.

*Using the FoundationDB Implementation*

Add the FoundationDB-backed store to integrate a production-ready FactStore into your application.
(Include Gradle/Maven coordinates here once you publish them.)

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
