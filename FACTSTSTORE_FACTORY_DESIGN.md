## FactStoreFactory Design & Implementation

This document describes the robust and idiomatic implementation of `FactStoreFactory`, a factory for provisioning and managing logical fact stores.

### Overview

The `FactStoreFactory` provides a unified interface for creating, retrieving, deleting, and managing logical fact stores. Each fact store is uniquely identified by a user-facing name (alphanumeric, URL-safe), with an internally-assigned UUID for additional identification if needed.

### Key Features

1. **User-Facing Names**: Each fact store is identified by a unique, URL-safe name
2. **Atomic Operations**: All factory operations (create, delete, list) are atomic
3. **Concurrent Safety**: Thread-safe handling of concurrent requests
4. **Metadata Management**: Persistent storage of fact store metadata (name, UUID, creation timestamp)
5. **Cascade Deletion**: Deleting a fact store removes all associated facts
6. **Name Reuse**: After deletion, a fact store name becomes available for reuse

### Architecture

#### Specification Layer (`factstore-specification`)

**File: `FactStoreFactory.kt`**
- Interface defining the factory contract
- Methods:
  - `create(name)`: Atomically creates a new fact store
  - `get(name)`: Retrieves an existing fact store instance
  - `getMetadata(name)`: Gets metadata without loading the full store
  - `exists(name)`: Checks if a fact store exists
  - `delete(name)`: Cascade-deletes a fact store and all its facts
  - `listAll()`: Returns all fact stores managed by the factory

**File: `FactStoreFactoryException.kt`**
- `FactStoreFactoryException`: Base exception for factory errors
- `FactStoreAlreadyExistsException`: Thrown when creating a duplicate name
- `FactStoreNotFoundException`: Thrown when accessing non-existent stores
- `InvalidFactStoreNameException`: Thrown for invalid names
- `FactStoreNotEmptyException`: Reserved for future strict deletion semantics

#### Implementation Layer (`factstore-foundationdb`)

**File: `FdbFactStoreFactory.kt`**
- FoundationDB implementation of the factory interface
- Metadata storage structure:
  ```
  /fact-store-metadata/
  ├── by-name/{name} → StoredFactStoreMetadata (id, name, createdAt)
  └── by-id/{id} → name (reverse index)
  ```

### Name Validation

Valid fact store names consist only of:
- Alphanumeric characters (a-z, A-Z, 0-9)
- Hyphens (-)
- Underscores (_)

Constraints:
- Non-empty
- Maximum 255 characters
- Validated on all operations

### Concurrency & Atomicity

All operations leverage FoundationDB's transactional guarantees:

1. **Create**: Atomically checks name uniqueness and creates metadata + directory
2. **Delete**: Atomically removes metadata and cascades directory deletion
3. **Exists/GetMetadata**: Read-only operations using read transactions

The implementation uses `database.runAsync()` with futures for non-blocking concurrent handling.

### Data Persistence

Metadata is stored using FoundationDB's serialization:

```kotlin
@Serializable
data class StoredFactStoreMetadata(
    val id: String,         // UUID as string
    val name: String,
    val createdAt: Long,    // Unix timestamp in milliseconds
)
```

### Usage Examples

```kotlin
// Initialize
val db = FDB.instance().open(clusterFilePath)
val factory = FdbFactStoreFactory.create(db)

// Create
val metadata = factory.create("my-store")
println("Created: ${metadata.id}, ${metadata.name}")

// Retrieve
val store = factory.get("my-store")
store.append(fact)

// Check existence
if (factory.exists("my-store")) {
    val meta = factory.getMetadata("my-store")
    println("Created at: ${meta.createdAt}")
}

// List all
val allStores = factory.listAll()

// Delete (cascade)
factory.delete("my-store")
```

### Testing

Comprehensive tests in `FdbFactStoreFactoryTest.kt` cover:

- **Basic Operations**:
  - Creating stores with valid names
  - Retrieving stores by name
  - Listing all stores
  - Deleting stores

- **Validation**:
  - Rejecting empty names
  - Rejecting invalid characters
  - Rejecting names exceeding 255 characters
  - Accepting alphanumeric, hyphens, underscores

- **Uniqueness**:
  - Rejecting duplicate names
  - Making names available after deletion

- **Cascade Deletion**:
  - Verifying all facts are deleted with the store
  - Allowing store recreation with same name

- **Concurrency**:
  - Concurrent create operations with name uniqueness
  - Mixed concurrent operations (create, read metadata, check existence)
  - Unique UUID assignment
  - Monotonic timestamp ordering

### Future Enhancements

1. **Dogfooding with Facts**: Store factory metadata as facts in a special system fact store
2. **Soft Deletion**: Support for audit trails while preventing recreation
3. **Strict Deletion**: Option to require empty stores before deletion
4. **Configuration**: Support for per-store configuration (retention policies, access control)
5. **Lifecycle Events**: Hooks for pre/post creation, deletion
6. **Discovery**: Service discovery integration for distributed scenarios

### Design Rationale

- **Metadata in Separate Keyspace**: Keeps metadata independent from individual fact store data, enabling efficient factory operations
- **UUID + Name Combination**: User-friendly names with internal unique identifiers for resilience
- **Async/CompletableFuture**: Non-blocking operations compatible with coroutine suspension
- **Atomic Operations**: Prevents race conditions and ensures consistency
- **Cascade Deletion**: Simplifies cleanup and prevents orphaned data

