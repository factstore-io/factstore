package io.factstore.application.slices

import com.fraktalio.fmodel.domain.IDecider
import io.factstore.application.FactRepository
import io.factstore.application.Slice
import io.factstore.application.SystemConsistencyBoundary
import io.factstore.application.SystemFactTags
import io.factstore.application.SystemFactTypes
import io.factstore.application.SystemStore
import io.factstore.core.FactId
import io.factstore.core.FactQueryItem
import io.factstore.domain.CreateStore
import io.factstore.domain.CreateStoreDecisionModel
import io.factstore.domain.SystemFact
import io.factstore.storage.LogCondition
import io.factstore.storage.LogQuery
import io.factstore.storage.LogQueryItem

typealias CreateStoreSlice = Slice<CreateStore, CreateStoreDecisionModel, SystemFact, LogQuery, LogCondition, FactId>

fun CreateStoreSlice(
    decider: IDecider<CreateStore, CreateStoreDecisionModel, SystemFact>,
    repository: FactRepository<SystemFact, LogQuery, LogCondition, FactId>,
): CreateStoreSlice = Slice(decider, CreateStoreConsistencyBoundary, repository)

private object CreateStoreConsistencyBoundary : SystemConsistencyBoundary<CreateStore> {

    override fun query(command: CreateStore): LogQuery = LogQuery(
        LogQueryItem(
            storeId = SystemStore.ID,
            item = FactQueryItem(
                types = setOf(SystemFactTypes.STORE_CREATED),
                tags = mapOf(SystemFactTags.storeName(command.storeName)),
            ),
        )
    )
}
