# ADR 1: Initial Query API

Author: Domenic Cassisi
Date: 2026-07-21
Status: Accepted

This document explains the reasoning about the initial version of FactStore's Query API and why it looks like the way
it does. 

## Context

FactStore provides multiple individual "find-by" functions to filter facts by specific criteria. 
These specialized query slices work well for their cases, but are not very flexible. Composing more complex queries
is currently not supported. This motivated the exploration of a unified query API.

## Decision

It was decided to start with a very small feature set that can be reasoned about easily, while still being flexible
for more complex queries. During prototyping, a lot of different query models were explored, however, not for all could
a scaling and predictable solution be found. Some features like first N and last N were omitted for nested queries,
as they showed issues in terms of scalability. For some use cases, a lot of facts needed to be buffered in memory 
before returning the result stream, which was not scaling for larger stores. One of the main requirements was to 
ensure scalability across store sizes. 

That's why we came up with the following query model that allows to filter facts by
- subject
- type
- tag(s)
- time range

A query contains a filter, which is a list of query items, each composing one or more criteria from the list above.
Complex queries / streams can be constructed by using multiple query items, which are OR-ed together. 

There is a global read direction that can be either forward or backward. The resulting stream ensures ordering across
substreams. It is not possible to set a stream direction per query item.

The result stream can be limited by setting a global limit. Setting a limit returns up to limit items before the 
stream terminates. 

## Consequences

Focusing on a small feature set consciously omits other "advanced" features like first N, last N, or other more 
complex nested predicates. We might explore their feasability in more detail in future design sessions.

