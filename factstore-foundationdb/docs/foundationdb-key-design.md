# FoundationDB Key Design for FactStore

This document outlines the key design principles for using FoundationDB as the underlying storage engine for FactStore.
The key design is crucial for ensuring efficient data retrieval, scalability, and maintainability.

## Key Structure

In FoundationDB, keys are byte strings that can be structured to optimize for specific access patterns. 
For FactStore, we will use a hierarchical key structure that allows for efficient querying and data organization.

Overview of the key structure:

```
/factstore-root
    /stores/
        /<store_id>
            /metadata
            /facts
                /global/{versionstamp} = serialized fact data
                /head = {versionstamp
                /fact-position-index/{fact_id} = (versionstamp)
                /fact-type-index/{fact_type}/{versionstamp} = (fact_id)
                /created-at-index/{created_at}/{versionstamp} = (fact_id)
                /subject-index/{subject_id}/{versionstamp} = (fact_id)
                /tags-index/{tag_key}/{tag_value}/{versionstamp} = (fact_id)
                /fact-type-tag-index/{fact_type}/{tag_key}/{tag_value}/{versionstamp} = (fact_id)
    /store-index/
        /store-name/{store_name} = (store_id)
```

Alternative design: 

```
/factstore-root
    /store
        /<store-id>
    /store-facts/
        /<store_id>
            /global/{versionstamp} = serialized fact data
            /head = {versionstamp
            /fact-position-index/{fact_id} = (versionstamp)
            /fact-type-index/{fact_type}/{versionstamp} = (fact_id)
            /created-at-index/{created_at}/{versionstamp} = (fact_id)
            /subject-index/{subject_id}/{versionstamp} = (fact_id)
            /tags-index/{tag_key}/{tag_value}/{versionstamp} = (fact_id)
            /fact-type-tag-index/{fact_type}/{tag_key}/{tag_value}/{versionstamp} = (fact_id)
    /store-fact-types
        /<store_id>
            /fact-type/{fact_type} = ???
    /store-index/
        /store-name/{store_name} = (store_id)
```

```
/factstore-root
    /stores/<store-id> = (metadata)
    /store-index/name-to-id/{store_name} = (store_id)
    /store-facts/<store_id>/{vs} = serialized fact data
    /store-head/<store_id> = (versionstamp)
    /store-fact-position-index/<store_id>/{fact_id} = (versionstamp)
    /store-fact-type-index/<store_id>/{fact_type}/{versionstamp} = (fact_id)
    /store-created-at-index/<store_id>/{created_at}/{versionstamp} = (fact_id)
    /store-subject-index/<store_id>/{subject_id}/{versionstamp} = (fact_id)
    /store-tags-index/<store_id>/{tag_key}/{tag_value}/{versionstamp} = (fact_id)
    /store-fact-type-tag-index/<store_id>/{fact_type}/{tag_key}/{tag_value}/{versionstamp} = (fact_id)    
    /idempotency-keys/<store_id>/{idempotency_key} = (versionstamp)
    
```

With this approach we could use something like a system fact store to store facts about fact stores. 
So the solution is dogfooding itself. 
The system fact store has a built-in projection that is used build a read model can be used to query for store metadata and configuration.
It is immediate consistent with the fact store data, and can be used to drive the UI and other components that need to query for store metadata and configuration.
Ideally, this projection is only updated from the facts in the system fact store, and not from any other source, to ensure that it is always consistent with the fact store data.