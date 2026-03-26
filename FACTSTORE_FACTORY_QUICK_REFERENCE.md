# FactStoreFactory Quick Reference

## Creating the Factory

```kotlin
// Initialize FoundationDB
val db = FDB.instance().open("/etc/foundationdb/fdb.cluster")

// Create the factory (initializes metadata directory)
val factory = FdbFactStoreFactory.create(db)
```

## Core Operations

### Create a Fact Store
```kotlin
val metadata = factory.create("my-store")
// metadata.id: UUID
// metadata.name: String
// metadata.createdAt: Long (timestamp in ms)
```

### Get a Fact Store
```kotlin
val store: FactStore = factory.get("my-store")

// Now you can append and query facts
store.append(fact)
val results = store.findBySubject(subjectRef)
```

### Get Metadata Only
```kotlin
val metadata = factory.getMetadata("my-store")
println("Store ID: ${metadata.id}")
println("Created: ${metadata.createdAt}")
```

### Check Existence
```kotlin
if (factory.exists("my-store")) {
    println("Store exists!")
}
```

### Delete a Fact Store
```kotlin
factory.delete("my-store")
// All facts in "my-store" are deleted
// The name "my-store" is available for reuse
```

### List All Stores
```kotlin
val allStores: List<FactStoreMetadata> = factory.listAll()
allStores.forEach { meta ->
    println("${meta.name} (ID: ${meta.id})")
}
```

## Name Validation Rules

✅ **Valid** names contain only:
- Letters (a-z, A-Z)
- Numbers (0-9)
- Hyphens (-)
- Underscores (_)

❌ **Invalid** names contain:
- Spaces
- Special characters (. / @ : $ etc.)
- Non-ASCII characters
- Are empty
- Exceed 255 characters

## Example: Complete Workflow

```kotlin
suspend fun demonstrateFactory() {
    val db = FDB.instance().open(clusterFilePath)
    val factory = FdbFactStoreFactory.create(db)
    
    // Create two fact stores
    val store1Meta = factory.create("users-events")
    val store2Meta = factory.create("orders-events")
    
    // Get and use first store
    val usersStore = factory.get("users-events")
    usersStore.append(
        Fact(
            id = FactId.generate(),
            type = "user_created".toFactType(),
            payload = "{...}".toFactPayload(),
            subjectRef = SubjectRef("user", "user123"),
            appendedAt = Instant.now()
        )
    )
    
    // List all stores
    val stores = factory.listAll()
    println("Total stores: ${stores.size}")
    
    // Delete first store
    factory.delete("users-events")
    
    // Verify deletion
    println("Exists: ${factory.exists("users-events")}")  // false
    
    // Create again with same name
    val newMeta = factory.create("users-events")
    println("New ID: ${newMeta.id}")  // Different UUID from before
}
```

## Exception Handling

```kotlin
try {
    factory.create("my-store")
} catch (e: FactStoreAlreadyExistsException) {
    println("Store already exists: ${e.name}")
} catch (e: InvalidFactStoreNameException) {
    println("Invalid name: ${e.name} - ${e.reason}")
} catch (e: FactStoreNotFoundException) {
    println("Store not found: ${e.name}")
}
```

## Thread-Safety & Concurrency

All operations are **thread-safe** and **atomic**:

```kotlin
// Safe concurrent creates with same name
val jobs = (1..10).map {
    launch { factory.create("concurrent-test") }
}
// Only 1 succeeds, 9 throw FactStoreAlreadyExistsException

// Safe concurrent mixed operations
launch { factory.create("store1") }
launch { factory.exists("store1") }
launch { factory.getMetadata("store1") }
```

## Integration with FactStore

After creating/retrieving a fact store, you can use all FactStore APIs:

```kotlin
val store = factory.get("my-store")

// Append operations
store.append(fact)

// Query operations
store.findById(factId)
store.existsById(factId)
store.findInTimeRange(start, end)
store.findBySubject(subjectRef)
store.findByTags(tags)
store.findByTagQuery(query)

// Stream operations (via FactStreamer)
store.stream(streamRequest).collect { fact ->
    // Process fact
}
```

## Best Practices

1. **Reuse Factory Instance**: Create once and reuse across your application
2. **Validate Names**: Use `FdbFactStoreFactory.validateName()` before creating
3. **Handle Exceptions**: Always catch factory-specific exceptions
4. **Async Context**: Factory methods are suspend functions - call from coroutines
5. **Metadata Caching**: Cache metadata if accessed frequently
6. **Database Lifecycle**: Ensure FoundationDB connection stays open while using factory

## Performance Notes

- **Create**: Single transaction, O(1) for metadata
- **Get**: Directory lookup (fast), fact store context initialization
- **Delete**: Cascades all data removal (depends on store size)
- **List**: Scans metadata directory (scales with number of stores)
- **Exists**: Single key lookup, O(1)

---

For detailed design information, see `FACTSTSTORE_FACTORY_DESIGN.md`

