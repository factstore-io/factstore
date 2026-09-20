package io.factstore.server

import io.factstore.core.*

/*
 * Executes a request against the store.
 */

internal suspend fun AppendRequest.publishTo(factStore: FactStore): AppendResult =
    factStore.append(this)

internal suspend fun FindByIdRequest.publishTo(factStore: FactStore): FindByIdResult =
    factStore.findById(this)

internal suspend fun ExistsByIdRequest.publishTo(factStore: FactStore): ExistsByIdResult =
    factStore.existsById(this)

internal suspend fun StreamFactsRequest.publishTo(factStore: FactStore): StreamFactsResult =
    factStore.streamFacts(this)

internal suspend fun StreamFactsBySubjectRequest.publishTo(factStore: FactStore): StreamFactsBySubjectResult =
    factStore.streamFactsBySubject(this)

internal suspend fun StreamFactsByTypeRequest.publishTo(factStore: FactStore): StreamFactsByTypeResult =
    factStore.streamFactsByType(this)

internal suspend fun FindByTagsRequest.publishTo(factStore: FactStore): FindByTagsResult =
    factStore.findByTags(this)

internal suspend fun FindByTagQueryRequest.publishTo(factStore: FactStore): FindByTagQueryResult =
    factStore.findByTagQuery(this)

internal suspend fun FindInTimeRangeRequest.publishTo(factStore: FactStore): FindInTimeRangeResult =
    factStore.findInTimeRange(this)

internal suspend fun SubscribeRequest.publishTo(factStore: FactStore): SubscribeResult =
    factStore.subscribe(this)

internal suspend fun ReplayRequest.publishTo(factStore: FactStore): ReplayResult =
    factStore.replay(this)

internal suspend fun CreateStoreRequest.publishTo(factStore: FactStore): CreateStoreResult =
    factStore.create(this)

internal suspend fun FindStoreByNameRequest.publishTo(factStore: FactStore): FindStoreByNameResult =
    factStore.findByName(this)

internal suspend fun ExistsStoreByNameRequest.publishTo(factStore: FactStore): ExistsStoreByNameResult =
    factStore.existsByName(this)

internal suspend fun RemoveStoreRequest.publishTo(factStore: FactStore): RemoveStoreResult =
    factStore.remove(this)
