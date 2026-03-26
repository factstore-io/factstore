# FactStoreFactory Architecture Diagram

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Application Code                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  val factory = FdbFactStoreFactory.create(db)                   │
│  factory.create("my-store")                                     │
│  val store = factory.get("my-store")                            │
│  store.append(fact)                                             │
│                                                                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
        ┌────────────────┴────────────────┐
        │                                 │
        v                                 v
   ┌──────────────┐          ┌───────────────────┐
   │  Specification│          │  Implementation   │
   │   (Interface) │          │   (FoundationDB)  │
   ├──────────────┤          ├───────────────────┤
   │              │          │                   │
   │ FactStore    │          │ FdbFactStore      │
   │ Factory      │          │ Factory           │
   │              │          │                   │
   │ +create()    │          │ +append()         │
   │ +get()       │          │ +findById()       │
   │ +exists()    │          │ +findBySubject()  │
   │ +delete()    │          │ +stream()         │
   │ +listAll()   │          │                   │
   │              │          │                   │
   └──────────────┘          └────────┬──────────┘
        ^                             │
        │                             │
        └─────────────────────────────┘
              Interface Implementation
```

## FactStoreFactory Component Diagram

```
┌────────────────────────────────────────────────────────────┐
│                FdbFactStoreFactory                         │
├────────────────────────────────────────────────────────────┤
│                                                             │
│  Public Methods (FactStoreFactory Interface):             │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ suspend fun create(name): FactStoreMetadata          │ │
│  │ suspend fun get(name): FactStore                    │ │
│  │ suspend fun getMetadata(name): FactStoreMetadata    │ │
│  │ suspend fun exists(name): Boolean                   │ │
│  │ suspend fun delete(name)                            │ │
│  │ suspend fun listAll(): List<FactStoreMetadata>      │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                             │
│  Companion Object:                                         │
│  ┌──────────────────────────────────────────────────────┐ │
│  │ suspend fun create(database): FdbFactStoreFactory   │ │
│  │ fun validateName(name)                              │ │
│  └──────────────────────────────────────────────────────┘ │
│                                                             │
└────────────────────────────────────────────────────────────┘
                         │
                         │ Uses
                         v
        ┌────────────────────────────────┐
        │     FoundationDB Database      │
        │                                │
        │ Metadata Keyspace:             │
        │ /fact-store-metadata/          │
        │  ├── /by-name/{name}           │
        │  └── /by-id/{id}               │
        │                                │
        │ Fact Store Keyspaces:          │
        │ /fact-store/{name}/            │
        │  ├── /facts                    │
        │  ├── /indexes                  │
        │  └── /idempotency-keys         │
        │                                │
        └────────────────────────────────┘
```

## Data Flow: Creating a Fact Store

```
User Code
   │
   │ factory.create("my-store")
   │
   v
┌─────────────────────────────────────────────────────────────┐
│ FdbFactStoreFactory.create()                               │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│ 1. Validate name                                            │
│    ✓ Not empty                                              │
│    ✓ Alphanumeric + hyphens + underscores only             │
│    ✓ Max 255 characters                                     │
│                                                              │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   v
┌─────────────────────────────────────────────────────────────┐
│ 2. Generate Metadata                                        │
│                                                              │
│    id = UUID.randomUUID()                                   │
│    createdAt = System.currentTimeMillis()                   │
│                                                              │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   v
┌─────────────────────────────────────────────────────────────┐
│ 3. FoundationDB Transaction                                 │
│                                                              │
│    a) Check name uniqueness                                 │
│       - Query /fact-store-metadata/by-name/{name}           │
│       - If exists → throw FactStoreAlreadyExistsException   │
│                                                              │
│    b) Store metadata                                        │
│       - Set /fact-store-metadata/by-name/{name}             │
│       - Set /fact-store-metadata/by-id/{id}                 │
│                                                              │
│    c) Create fact store directory                           │
│       - createOrOpen /fact-store/{name}                     │
│       - Initialize subspaces (facts, indexes, etc.)         │
│                                                              │
│    d) Commit (atomic)                                       │
│                                                              │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   v
                Return FactStoreMetadata
                   │
                   v
                User Code

         FactStoreMetadata(
           id = UUID(...),
           name = "my-store",
           createdAt = 1645000000000
         )
```

## Data Flow: Concurrent Creates with Same Name

```
┌─────────────────────────────────────────────────────────────┐
│                  Two Concurrent Threads                      │
├──────────────────────┬──────────────────────────────────────┤
│                      │                                       │
│  Thread 1            │  Thread 2                            │
│  ─────────────────   │  ────────────────                    │
│  create("same-name") │  create("same-name")                │
│         │            │         │                            │
│         v            │         v                            │
│  Tx 1: Begin         │  Tx 2: Begin                         │
│         │            │         │                            │
│         v            │         v                            │
│  Query name          │  Query name                          │
│  Not found ✓         │  Not found ✓                         │
│         │            │         │                            │
│         v            │         v                            │
│  Write metadata ─────┼────→ Conflict!                       │
│  Tx 1: Commit ✓      │  Tx 2: Retry or Fail                │
│         │            │         │                            │
│         v            │         v                            │
│  Return Success      │  Query name                          │
│                      │  Now exists ✗                        │
│                      │  Throw exception                     │
│                      │  Return Failure                      │
│                      │                                      │
└──────────────────────┴──────────────────────────────────────┘

Result: Exactly one succeeds, others get FactStoreAlreadyExistsException
```

## Metadata Storage Structure

```
FoundationDB Keyspace Layout:

┌────────────────────────────────────────────────────────────┐
│ /fact-store-metadata/                                      │
│                                                             │
│  /by-name/                                                 │
│  ├── "my-store" → StoredFactStoreMetadata {                │
│  │               id: "550e8400-e29b-41d4...",              │
│  │               name: "my-store",                         │
│  │               createdAt: 1645000000000                  │
│  │            }                                            │
│  │                                                         │
│  ├── "orders-events" → StoredFactStoreMetadata {...}       │
│  │                                                         │
│  └── "user-events" → StoredFactStoreMetadata {...}         │
│                                                             │
│  /by-id/                                                   │
│  ├── "550e8400-e29b-41d4..." → "my-store"                  │
│  ├── "6ba7b810-9dad-11d1..." → "orders-events"            │
│  └── "6ba7b811-9dad-11d1..." → "user-events"              │
│                                                             │
└────────────────────────────────────────────────────────────┘

/fact-store/
├── my-store/
│   ├── /facts/                    (fact data)               
│   ├── /head-index/               (latest timestamp)        
│   ├── /fact-positions/           (fact ID → position)      
│   ├── /type-index/               (type → facts)            
│   ├── /created-at-index/         (timestamp → facts)       
│   ├── /subject-index/            (subject → facts)         
│   ├── /metadata-index/           (metadata → facts)        
│   ├── /tags-index/               (tags → facts)            
│   └── /idempotency-keys/         (idempotency keys)        
│                                                             
├── orders-events/                                            
│   ├── /facts/                                               
│   ├── ... (same structure)                                 
│                                                             
└── user-events/                                              
    ├── /facts/                                               
    └── ... (same structure)                                  
```

## Operation Latency

```
Operation                  | Latency | Scalability
─────────────────────────────────────────────────────
create(name)               | Low     | O(log n) metadata size
get(name)                  | Medium  | O(1) metadata + init time
getMetadata(name)          | Low     | O(log n) metadata size
exists(name)               | Low     | O(log n) metadata size
delete(name)               | High    | O(store size) - cascade delete
listAll()                  | Medium  | O(m) where m = number of stores
```

## Exception Hierarchy

```
Exception
    │
    ├── FactStoreException
    │       │
    │       └── FactStoreFactoryException
    │               │
    │               ├── FactStoreAlreadyExistsException
    │               │   (name already exists)
    │               │
    │               ├── FactStoreNotFoundException
    │               │   (name not found)
    │               │
    │               ├── InvalidFactStoreNameException
    │               │   (name validation failed)
    │               │
    │               └── FactStoreNotEmptyException
    │                   (store not empty, for future use)
    │
    ├── DuplicateFactIdException
    │   (fact operation, not factory-specific)
    │
    └── ... (other FactStore exceptions)
```

## Integration Points

```
┌──────────────────────────────────────────────┐
│          Application Layer                   │
└────────┬─────────────────────────┬───────────┘
         │                         │
         v                         v
    ┌────────────────┐    ┌────────────────┐
    │  FactStore     │    │  FactStore     │
    │  Factory       │    │  Operations    │
    │  (create/get)  │    │  (append/find) │
    └────────┬───────┘    └────────┬───────┘
             │                     │
             └─────────────┬───────┘
                           │
                           v
                ┌──────────────────────┐
                │   FactStore          │
                │   Interface          │
                └──────────────────────┘
                           │
                           v
                ┌──────────────────────┐
                │   FactAppender       │
                │   FactFinder         │
                │   FactStreamer       │
                └──────────────────────┘
                           │
                           v
                ┌──────────────────────┐
                │   FoundationDB       │
                │   Implementation     │
                └──────────────────────┘
```

---

This architecture ensures:
- ✅ Clean separation of concerns
- ✅ Atomic operations with strong consistency
- ✅ Concurrent request handling
- ✅ Metadata independence from fact storage
- ✅ Extensibility for future enhancements

