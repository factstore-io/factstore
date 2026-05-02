# Design Document: Zero-Exception Business Logic Policy

## 1. Abstract

Historically, Java and early Kotlin applications relied on `Exceptions` to handle non-happy-path outcomes. 
This document proposes a shift toward Functional Error Handling, where anticipated business outcomes—including failures—are treated as first-class data using Kotlin's `sealed` interfaces.

## 2. The Problem with Exceptions

Exceptions are often referred to as "hidden side effects." Using them for business logic creates several architectural hurdles:

- Invisible Control Flow: Functions that throw exceptions lie about their signature. fun append(...) suggests it returns a result, but it may actually terminate the caller's execution unexpectedly.
- Cognitive Load: Developers must "just know" which exceptions might be thrown, leading to defensive try-catch blocks or, worse, unhandled crashes.
- Performance Overhead: JVM exceptions are expensive to instantiate because they must capture the entire thread stack trace, even if that trace is never read.
- Lack of Exhaustiveness: The compiler cannot verify that a developer has handled a specific Exception type, leading to gaps in business logic.

## 3. The Proposed Policy: Results as Sum Types

We define "Exceptional" vs. "Anticipated" outcomes:

- Exceptional (Use Exceptions): Infrastructure failures (network down, disk full), fatal programmer errors (IndexOutOfBounds), or non-recoverable system states.
- Anticipated (Use Sealed Results): Business logic decisions (StoreNotFound, ConditionViolated, ...). These are valid branches of the program's execution.

### Implementation Pattern

Every public-facing service method must return a Result sealed interface.

```kotlin
sealed interface AppendResult {
    data object Appended : AppendResult
    data object StoreNotFound : AppendResult
    data object AlreadyApplied : AppendResult
    data object AppendConditionViolated : AppendResult
    data class DuplicateFactIds(val ids: List<FactId>) : AppendResult
}
```

## 4. Architectural Benefits

### 4.1. Exhaustive Switching

Kotlin’s compiler ensures that every branch of a sealed interface is handled in a when expression. This eliminates the "I forgot to handle that error" bug category.

### 4.2. Self-Documenting APIs

The API becomes "Honest." A developer looking at the signature:
```kotlin
suspend fun append(request: AppendRequest): AppendResult`
```
instantly understands every possible outcome of the call without reading the implementation.

### 4.3. Railway Oriented Programming

This policy allows us to chain operations safely. We can model the flow of data as a "track" that can switch to a "failure track" without the violence of an exception throw.

## 5. Conclusion

By adopting a Zero-Exception policy for business logic, FactStore becomes more predictable, performant, and developer-friendly. 
We trade implicit "magic" for explicit "data," ensuring that our error-handling is as robust as our happy-path logic.
