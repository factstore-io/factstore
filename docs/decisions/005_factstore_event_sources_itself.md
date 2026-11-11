# 005 — FactStore event-sources itself

**Date:** 2026-11-08
**Status:** Proposed, explored on the branch `factstore-event-sourced`

## Summary

- **FactStore event-sources itself.** Its own business rules, such as "a store name is unique" or
  "facts can only be appended to a live store", are decisions over FactStore's own facts, made
  with the same concepts FactStore offers its users: commands, facts, queries and conditional
  appends. FactStore is built the way it asks its users to build.
- The **system store** is FactStore's catalog, as system tables are a relational database's: an
  ordinary store that only FactStore's decisions append to.
- Storage engines hold **no business rules**. They implement one contract: read facts by query,
  and append facts on condition.

## Why

Every storage engine implemented the same rules again: checking that a store exists before an
append, rejecting a second store with the same name. Each new engine, and each new rule such as
archiving or fact types, multiplied that work, and what the rules leave out is left to each engine: a
subscription to a removed store waits forever on FoundationDB, and a stream over it ends early
without saying why. The rules are
FactStore's domain, not storage. With dynamic consistency boundaries, each decision reads exactly
the facts it needs, so these rules need no aggregates and no sagas.

## Principles

| What | Decision | Why |
|---|---|---|
| Intent | One command per intent, one decider per command: a vertical slice | A slice can be built, tested and changed on its own |
| Outcome | Every outcome is a fact, business rejections included. Invalid input and missing authentication are stopped at the edge. A command about a store without any facts is answered, not recorded | One shape for every outcome, and nothing recorded for requests that never reached a decision |
| Content | A fact records what its own decision decided, nothing more | No fact carries data only so that a later step can use it |
| Envelope | Idempotency keys, versions, correlation and the actor travel next to the command, not in it | Technical concerns stay out of the decisions |
| Consistency | A decision reads its query and appends on condition that nothing matching that query was appended since. The query belongs to the slice, next to its decider | The condition always covers what the decision read |
| Scope | Queries never scan across stores; every query item names one store. A decision may read and append across stores in one transaction | Isolation holds by construction, and cross-store rules stay single decisions |
| Identity | A store's id is its identity; its name is an alias | Renaming later leaves past decisions untouched |
| Reads | Reads are queries and record nothing. Facts are recorded only when FactStore takes on a responsibility, such as a durable subscription | Commands decide, queries do not |
| End of a store | No fact of a store can follow its `StoreRemoved`, so a read ends with `StoreRemoved` as its last element | Removal reaches readers as a fact, the same on every engine |
| Removal | Purge is the only removal of facts. After a grace period, it erases the store and the system store's facts about it. FactStore keeps no audit log of its own | Deleted data is gone, as in other databases |

## The system store

- An ordinary store with a fixed id. Only FactStore's decisions append to it; users read it only
  with admin rights, since it holds the names of every tenant's stores.
- FactStore's facts about a store live in the system store, tagged with the store's id, as
  catalog rows describe a table without living in it. Producers' facts live in their store.
- The fact types carry the reserved prefix `factstore.`, which producers may not use.

## Architecture

| Module | Holds | Depends on |
|---|---|---|
| `factstore-domain` | Commands, facts, deciders and views: every business rule, as pure functions | `factstore-specification`, fmodel |
| `factstore-application` | One slice per command (its query, decider and fact mapping), the handler that runs them, and the facade the edge calls. No business rules | domain, storage API |
| `factstore-storage-api` | The `FactLog` contract, and `FactLogContractTest`, which every engine must pass | `factstore-specification` |
| `factstore-storage-memory` | The in-memory engine | storage API |

The edge (HTTP, gRPC, MCP) calls the facade and only transports commands and facts. A storage
engine guarantees atomic transactions across stores, one order over all facts, and reads that
name their stores.

## Adding a feature

1. Model it first: commands, facts, views and automations in the event model, with Given/When/Then.
2. Check information completeness: every field of a fact comes from the command, the edge,
   FactStore, or the facts its decision read.
3. Decide which store each fact lives in, and which tags its decisions query.
4. In the domain: the command, the facts, and the decider with its `<Command>DecisionModel`,
   tested with Given/When/Then.
5. In the application: the slice's query and the fact mapping.
6. In storage: nothing, unless the feature needs a new capability. Then the contract and
   `FactLogContractTest` change first, and every engine follows.

A feature that can be expressed as a query over tags needs no new concept.

## Open

- To decide before users depend on them: the versioning of system facts, whether users see system
  facts in their reads, and the storage guarantees if stores ever spread across clusters.
- Following a query in the contract (`follow`), the length of the grace period, and the slices
  Remove Store, Purge Store and Append Facts.
