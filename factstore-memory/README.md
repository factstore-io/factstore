# FactStore In-Memory Implementation

A lightweight, in-memory implementation of the FactStore specification, designed for testing, evaluation, and prototyping.

## Overview

The in-memory FactStore provides a fully functional implementation of the FactStore API without any persistent storage. All data is stored in memory and is lost when the process terminates. This makes it ideal for:

- Unit and integration testing
- Prototyping event-sourced systems
- Development and debugging
- Educational purposes

## Features

The implementation supports all FactStore interfaces:

### FactStoreFactory
- Create new fact stores with unique names
- Prevents duplicate names
- Returns factory results with proper error handling

### FactStoreFinder
- List all created fact stores
- Find fact stores by name
- Check existence of fact stores by name

### FactAppender
- Append single facts to stores
- Batch append multiple facts atomically
- Support for idempotent append operations
- Conditional append with various append conditions:
  - No condition (default)
  - Expected last fact for a subject
  - Expected last facts for multiple subjects
  - Tag query-based conditions

### FactFinder
- Find facts by their unique ID
- Check fact existence
- Find facts within a time range
- Find facts by subject reference
- Find facts by tags with OR semantics
- Find facts by complex tag queries

### FactStreamer
- Stream facts from the beginning
- Stream facts from the end (latest)
- Stream facts from a specific position
- Infinite streaming with watch-like behavior
- Used by [FactStore#stream](../factstore-specification/src/main/kotlin/io/factstore/core/FactStreamer.kt)

## Thread Safety

The implementation uses Kotlin coroutine mutexes to ensure thread-safe access to the internal data structures. All operations are protected by a single lock to guarantee consistency.

## Usage

```kotlin
// Create a new in-memory fact store
val store: FactStore = MemoryFactStore()

// Create a store
val storeName = StoreName("my-store")
store.createStore(CreateStoreRequest(storeName))

// Append a fact; its id and appendedAt are assigned by the store
val result = store.append(
    storeName,
    FactInput(
        type = "UserCreated".toFactType(),
        subject = "user/123".toSubject(),
        payload = """{"username": "Alice"}""".toFactPayload(),
    ),
)
val factId = (result as AppendResult.Appended).factIds.single()

// Find a fact by its id
val found = store.findFactById(FindFactByIdRequest(storeName, factId))

// Stream the facts of a subject
val stream = store.streamFacts(
    StreamFactsRequest(
        storeName = storeName,
        query = FactQuery(listOf(FactQueryItem(subjects = setOf("user/123".toSubject())))),
        direction = ReadDirection.Forward,
    )
)
(stream as StreamFactsResult.FactStream).facts.collect { fact -> println("Streamed: $fact") }
```

## Implementation Details

### Internal Data Structures

- **stores**: Map of StoreName → MemoryStore, one entry per store
- **MemoryStore**: a store's metadata, its facts in append order, the idempotency keys it has seen,
  and its head (the number of facts) as a `MutableStateFlow` that subscriptions watch

### Lock Strategy

All operations acquire a single mutex lock to ensure consistency. The lock is held for the minimum necessary duration to prevent data corruption and race conditions.

## Performance Characteristics

- **Append**: O(n) where n is the number of facts appended
- **Find by ID**: O(n) where n is the number of facts in the store
- **Find by Subject**: O(n) linear scan through facts
- **Find by Tags**: O(n) linear scan through facts
- **Stream**: O(1) per fact emission
- **Memory**: O(n) where n is the total number of facts and metadata

The in-memory nature makes it very fast for small to medium datasets but not suitable for very large datasets or long-running processes where memory is a concern.

## Testing

The implementation is tested using the comprehensive test suite from `factstore-testing`:

```bash
./gradlew factstore-memory:test
```

## Limitations

1. **No Persistence**: Data is lost when the JVM terminates
2. **Single-Process Only**: Not suitable for multi-process deployments
3. **Memory-Based**: Performance degrades with large datasets
4. **No Clustering**: Designed for single-instance use
5. **No Distributed Transactions**: No support for distributed append operations

## When to Use

Use the in-memory FactStore when:
- Writing unit tests for event-sourced code
- Evaluating FactStore without infrastructure setup
- Prototyping new features or domains
- Learning FactStore concepts
- Debugging issues in isolation

For production use, consider using `factstore-foundationdb` or another persistent implementation.

